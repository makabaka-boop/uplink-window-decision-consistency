// Package store 封装 PostgreSQL 持久化与租约裁决逻辑。
package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// 指令生命周期状态。迁移约束、存储裁决、HTTP 响应与查询视图共用这一份契约。
const (
	StatusPending   = "pending"
	StatusDelivered = "delivered"
	StatusFailed    = "failed"
	// StatusBlocked 是只读终态：前驱确认失败或到期后，尚未送达的后继链原子转入，
	// 不产生租约、不产生结算。
	StatusBlocked = "blocked"
	// StatusExpired 是到期终态：数据库时钟已过执行截止点且指令尚未送达时，
	// 由针对该指令的到期结算原子转入。它在 settlements 留有正式结算行（根因
	// result='expired'），但不伪造任何租约回执（无代次、无令牌）。
	StatusExpired = "expired"
)

// 终态结果集合，供 HTTP 层做入参校验。
var ValidResults = map[string]bool{StatusDelivered: true, StatusFailed: true}

var (
	// ErrNoAvailableCommand 没有可领取（未结算且租约未生效）的指令。
	ErrNoAvailableCommand = errors.New("no available command")
	// ErrCommandNotFound 未知指令编号。
	ErrCommandNotFound = errors.New("command not found")
	// ErrInvalidLeaseToken 令牌无法识别（格式错误或从不属于该指令）。
	ErrInvalidLeaseToken = errors.New("invalid lease token")
	// ErrLeaseStale 令牌属于过期/被取代的旧代次，或当前代次已到期。
	ErrLeaseStale = errors.New("lease token is expired or superseded")
	// ErrAlreadySettled 指令已在终态（delivered/failed/blocked/expired），禁止结算或覆盖。
	ErrAlreadySettled = errors.New("command already settled")
	// ErrPredecessorNotFound 创建指令时引用的前驱编号不存在（或引用了更晚的编号）。
	ErrPredecessorNotFound = errors.New("predecessor command not found")
	// ErrDeadlineNotReached 到期结算被提前调用：该指令未填写截止时刻，或数据库时钟
	// 尚未到达截止点。裁决只看数据库时钟，重复的提前调用不会改变任何状态。
	ErrDeadlineNotReached = errors.New("execution deadline not reached")
	// ErrDeadlineReached 回执到达时数据库时钟已过截止点：即使持有代次正确、租期未到
	// 的旧租约，也不能再送达。与到期结算在同一行锁下互斥裁决。
	ErrDeadlineReached = errors.New("execution deadline reached")
)

// Store 基于 pgxpool 的存储实现。
type Store struct {
	pool *pgxpool.Pool
	// now 是实例本地时钟的测试注入点（见 NewWithClock）。
	// 注意：租约有效性等一切裁决都以数据库时钟为准（到期时间写入、领取侧
	// 到期判定、回执侧有效性判定共用同一时钟域），且裁决时刻是在拿到指令
	// 行锁之后读取的数据库真实时钟（clock_timestamp()）——锁等待期间跨过
	// 到期点/截止点的请求按裁决时刻判定，不按入队时刻判定；实例本地时钟
	// 不参与裁决。该注入点存在的唯一目的是让回归测试能验证“实例时钟偏移
	// 不改变裁决结论”，若有人重新把实例时钟引入裁决，带偏移的验收测试会
	// 立即失败。
	now func() time.Time
}

func New(pool *pgxpool.Pool) *Store {
	return NewWithClock(pool, time.Now)
}

// NewWithClock 允许测试注入实例本地时钟，用于回归验证多实例时钟偏移下
// 租约裁决的一致性（裁决本身只依赖数据库时钟）。
func NewWithClock(pool *pgxpool.Pool, now func() time.Time) *Store {
	return &Store{pool: pool, now: now}
}

// Ping 用于健康检查。
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Command 是指令聚合根的内存表示。
type Command struct {
	ID              int64
	Payload         []byte
	Status          string
	LeaseGeneration int64
	LeaseExpiresAt  *time.Time
	// PredecessorID 为唯一前驱编号；nil 表示无前驱（旧请求保持原样）。
	PredecessorID *int64
	// BlockedBy 记录阻断来源（确认失败或到期的那条根指令）；仅 status=blocked 时非空。
	BlockedBy *int64
	// DeadlineAt 为可选 UTC 执行截止时刻；nil 表示创建时未填写，行为与旧请求完全一致。
	DeadlineAt *time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// LeaseView 描述某一代租约（观察用，不含令牌原文）。
type LeaseView struct {
	Generation int64
	ExpiresAt  time.Time
	ClaimedAt  time.Time
}

// Settlement 终态结算记录。
// Generation 在 delivered/failed 时指向签收回执的租约代次；到期结算（result=expired）
// 不伪造任何回执，Generation 为 nil。
type Settlement struct {
	Generation *int64
	Result     string
	SettledAt  time.Time
}

// CommandDetail 观察视图：指令 + 各代租约 + 终态。
type CommandDetail struct {
	Command
	Leases     []LeaseView
	Settlement *Settlement
}

// Claim 是领取成功后的返回值。
type Claim struct {
	CommandID      int64
	Generation     int64
	LeaseToken     string
	LeaseExpiresAt time.Time
}

// newToken 生成 256bit 不透明随机令牌（十六进制编码）。
// 每次领取都重新生成，配合 leases.lease_token 全局唯一约束，
// 新令牌在数学上必然不同于任何旧令牌。
func newToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

const commandColumns = `id, payload, status, lease_generation, lease_expires_at,
	predecessor_id, blocked_by, deadline_at, created_at, updated_at`

func scanCommand(row pgx.Row, c *Command) error {
	return row.Scan(&c.ID, &c.Payload, &c.Status, &c.LeaseGeneration, &c.LeaseExpiresAt,
		&c.PredecessorID, &c.BlockedBy, &c.DeadlineAt, &c.CreatedAt, &c.UpdatedAt)
}

// CreateCommand 按调用顺序（由 IDENTITY 列保证）写入一条指令。
//
// predecessorID 为 nil 时是无前驱的旧请求，行为与过去完全一致。
// deadline 为 nil 时未填写执行截止时刻，领取/回执/到期行为与旧请求完全一致。
// 否则只能引用已存在的较早编号；整个判定在单事务内完成：
//   - 沿前驱链自根向叶逐行加 FOR UPDATE 锁，与任何在途的确认/到期事务串行化，
//     由数据库裁决“创建与前驱结算交错”，绝不留下前驱已失败/到期却仍可领取的后继；
//   - 前驱（直接前驱状态即代表整条链——链上不变量由裁决维护）已 delivered
//     或仍 pending 时，新指令正常 pending 等待；
//   - 前驱已 failed/blocked/expired 时，新指令出生即为 blocked 终态并记录阻断来源，
//     不写入任何租约或结算。
func (s *Store) CreateCommand(ctx context.Context, payload []byte, predecessorID *int64, deadline *time.Time) (*Command, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("create begin: %w", err)
	}
	defer tx.Rollback(ctx)

	status := StatusPending
	var blockedBy *int64

	if predecessorID != nil {
		if *predecessorID <= 0 {
			return nil, ErrPredecessorNotFound
		}
		// 递归收集整条前驱链并按 id 升序加锁：
		// 与确认事务（首步即 FOR UPDATE 锁定被确认指令）形成统一锁序，
		// 既避免死锁，也保证锁释放后本事务看到的是结算后的最终状态。
		rows, err := tx.Query(ctx, `
			WITH RECURSIVE chain(id, predecessor_id) AS (
				SELECT id, predecessor_id FROM commands WHERE id = $1
				UNION ALL
				SELECT p.id, p.predecessor_id
				FROM chain ch JOIN commands p ON p.id = ch.predecessor_id
			)
			SELECT c.id, c.status, c.blocked_by
			FROM chain ch JOIN commands c ON c.id = ch.id
			ORDER BY c.id
			FOR UPDATE OF c`, *predecessorID)
		if err != nil {
			return nil, fmt.Errorf("lock predecessor chain: %w", err)
		}
		found := false
		for rows.Next() {
			var id int64
			var st string
			var bBy *int64
			if err := rows.Scan(&id, &st, &bBy); err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan predecessor chain: %w", err)
			}
			if id == *predecessorID {
				found = true
				switch st {
				case StatusFailed, StatusExpired:
					// 直接前驱本身就是失败/到期根。
					src := id
					blockedBy = &src
					status = StatusBlocked
				case StatusBlocked:
					// 继承阻断来源，链上所有 blocked 指向同一失败/到期根。
					blockedBy = bBy
					status = StatusBlocked
				}
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate predecessor chain: %w", err)
		}
		if !found {
			return nil, ErrPredecessorNotFound
		}
	}

	c := &Command{}
	err = tx.QueryRow(ctx, `
		INSERT INTO commands (payload, predecessor_id, status, blocked_by, deadline_at)
		VALUES ($1::jsonb, $2, $3, $4, $5)
		RETURNING `+commandColumns,
		string(payload), predecessorID, status, blockedBy, deadline,
	).Scan(scanArgs(c)...)
	if err != nil {
		return nil, fmt.Errorf("create command: %w", err)
	}

	// 双保险：自增 ID 单调，存在的前驱编号必然更早；若异常违反则整体回滚。
	if predecessorID != nil && *predecessorID >= c.ID {
		return nil, fmt.Errorf("predecessor %d is not an earlier command than %d", *predecessorID, c.ID)
	}

	if predecessorID != nil {
		// 维护传递闭包：新指令到直接前驱一条边，外加直接前驱的全部祖先。
		// 与插入同一事务，任何崩溃/回滚都不会留下“有前驱边却无闭包”的半成品。
		if _, err := tx.Exec(ctx, `
			INSERT INTO command_closure (command_id, ancestor_id)
			SELECT $1::bigint, $2::bigint
			UNION ALL
			SELECT $1::bigint, ancestor_id FROM command_closure WHERE command_id = $2`,
			c.ID, *predecessorID); err != nil {
			return nil, fmt.Errorf("create closure: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("create commit: %w", err)
	}
	return c, nil
}

// Claim 原子领取最早可用的一条指令：
//   - 资格（整条前驱链全部 delivered）由传递闭包反连接在 commands 的行锁扫描中
//     逐行求值：存在任一状态不为 delivered 的祖先即跳过。
//     该形态与“直接扫 commands 再 FOR UPDATE SKIP LOCKED”等价安全——锁在扫描行走时
//     施加，而非先在递归 CTE 中物化候选再加锁（后者在高并发下会放过同一行）；
//   - FOR UPDATE SKIP LOCKED 保证并发领取者各自拿到不同的行（唯一领取）；
//   - 前驱尚未送达的指令只能等待，绝不预发租约；
//   - 已过执行截止点（deadline_at <= 裁决时刻，数据库时钟）的指令被跳过：过窗的
//     准备动作不能再被领取，其状态由针对该指令的到期结算另行原子裁决；
//   - 未填写截止时刻的指令不受该谓词影响，领取顺序与旧行为完全一致；
//   - 所有时间判定与租约到期时间的写入都使用语句执行时刻的数据库真实时钟
//     （clock_timestamp()，而非事务开始时钟 now()）：即使本事务在连接/锁上
//     有过等待，发放的租约与扫描资格仍锚定真正发放租约的那一刻；
//   - 在同一事务内推进 lease_generation 并写入新一代租约。
func (s *Store) Claim(ctx context.Context, leaseFor time.Duration) (*Claim, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("claim begin: %w", err)
	}
	defer tx.Rollback(ctx)

	var id int64
	err = tx.QueryRow(ctx, `
		SELECT c.id
		FROM commands c
		WHERE c.status = 'pending'
		  AND (c.lease_expires_at IS NULL OR c.lease_expires_at <= clock_timestamp())
		  AND (c.deadline_at IS NULL OR c.deadline_at > clock_timestamp())
		  AND NOT EXISTS (
				SELECT 1
				FROM command_closure k
				JOIN commands a ON a.id = k.ancestor_id
				WHERE k.command_id = c.id AND a.status <> 'delivered'
		  )
		ORDER BY c.id ASC
		FOR UPDATE OF c SKIP LOCKED
		LIMIT 1`).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNoAvailableCommand
	}
	if err != nil {
		return nil, fmt.Errorf("claim select: %w", err)
	}

	token, err := newToken()
	if err != nil {
		return nil, fmt.Errorf("generate token: %w", err)
	}

	var gen int64
	var expiresAt time.Time
	err = tx.QueryRow(ctx, `
		UPDATE commands
		SET lease_generation = lease_generation + 1,
		    lease_token      = $2,
		    lease_expires_at = clock_timestamp() + ($3::bigint * interval '1 microsecond'),
		    updated_at       = clock_timestamp()
		WHERE id = $1
		RETURNING lease_generation, lease_expires_at`,
		id, token, leaseFor.Microseconds(),
	).Scan(&gen, &expiresAt)
	if err != nil {
		return nil, fmt.Errorf("claim update: %w", err)
	}

	if _, err = tx.Exec(ctx, `
		INSERT INTO leases (command_id, generation, lease_token, expires_at, claimed_at)
		VALUES ($1, $2, $3, $4, clock_timestamp())`,
		id, gen, token, expiresAt); err != nil {
		return nil, fmt.Errorf("claim lease insert: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("claim commit: %w", err)
	}
	return &Claim{CommandID: id, Generation: gen, LeaseToken: token, LeaseExpiresAt: expiresAt}, nil
}

// Ack 凭未过期且属于当前代次的令牌，将指令置为终态。
// 旧代次/已到期/已结算/已过截止点等冲突通过哨兵错误返回，状态绝不改变。
//
// result=failed 时在同一事务内把尚未送达（pending）的整条后继链原子转为
// blocked 终态并记录阻断来源；blocked 指令不写租约、不写结算。
// result=delivered 仅结算自身，随后继之而来的后继由领取查询自动解锁。
//
// 截止点裁决：若指令填写了 deadline_at，回执只在数据库时钟仍早于截止点时
// 有效；过窗后即使令牌属于当前代次、租期未到，也只能由到期结算接管
// （ErrDeadlineReached），截止前的成功送达不可被追改。
//
// 裁决时刻：回执事务可能先在指令行锁上排队（另一裁决/创建事务持锁），
// 等待期间数据库时钟仍在走——入队时尚未过窗，拿到锁时租约或截止点可能
// 已经跨过。因此一切时间判定都使用拿到行锁之后读取的数据库真实时钟
// （clock_timestamp()），且租约有效性判定与截止点判定共用这同一个裁决
// 时刻：一次有效裁决，不因请求在锁队列中的等待顺序而改变结论。
func (s *Store) Ack(ctx context.Context, id int64, token, result string) error {
	if !ValidResults[result] {
		// 双保险：HTTP 层已拦截，存储层不接受任何越界值。
		return fmt.Errorf("invalid result %q", result)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("ack begin: %w", err)
	}
	defer tx.Rollback(ctx)

	// 首步即取指令行锁：与任何在途的领取/回执/到期/创建事务串行化。
	var status string
	var currentGen int64
	var deadlineAt *time.Time
	err = tx.QueryRow(ctx, `
		SELECT status, lease_generation, deadline_at
		FROM commands
		WHERE id = $1
		FOR UPDATE`, id).Scan(&status, &currentGen, &deadlineAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrCommandNotFound
	}
	if err != nil {
		return fmt.Errorf("ack load command: %w", err)
	}

	// 裁决时刻：拿到行锁之后的数据库真实时钟。不能用事务开始时钟 now()——
	// 它冻结在 BEGIN 时刻，会把锁等待期间已经跨过租约到期点/执行截止点的
	// 请求按入队时刻误判（已失效租约被送达、过窗指令被确认）。
	var adjudgedAt time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&adjudgedAt); err != nil {
		return fmt.Errorf("ack adjudication clock: %w", err)
	}

	// 裁决顺序（行锁内，无并发可改写状态）：
	// 1) 已在终态（delivered/failed/expired，以及失败/到期传播产生的只读 blocked）：
	//    一律拒绝重复结算/覆盖——先于令牌裁决，保证 blocked 指令不可能因任何令牌被改写；
	// 2) 令牌从未对该指令签发；
	// 3) 令牌非当前代次：旧持有者迟到；
	// 4) 令牌已过期：租约到期（以裁决时刻的数据库时钟判定，与实例本地时钟无关）；
	// 5) 数据库时钟已过执行截止点：旧租约即使尚未达到自身租期也不能送达。
	if status != StatusPending {
		return ErrAlreadySettled
	}

	// 令牌必须是系统签发过、且属于本指令的（其他指令的令牌视为从未签发）。
	// 有效期裁决使用拿到行锁后读取的裁决时刻（expires_at > 裁决时刻）：
	// 到期时间的写入、领取侧的到期判定与这里的回执侧判定同属数据库时钟域，
	// 多实例部署时各实例本地时钟可能有偏差，只有数据库时钟能在所有实例上
	// 给出相同结论。
	var leaseGen int64
	var leaseLive bool
	err = tx.QueryRow(ctx, `
		SELECT generation, (expires_at > $2)
		FROM leases
		WHERE lease_token = $1 AND command_id = $3`, token, adjudgedAt, id).Scan(&leaseGen, &leaseLive)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrInvalidLeaseToken
	}
	if err != nil {
		return fmt.Errorf("ack load lease: %w", err)
	}

	if leaseGen != currentGen {
		return ErrLeaseStale
	}
	if !leaseLive {
		return ErrLeaseStale
	}
	// 截止点判定与租约有效期判定共用同一裁决时刻、同一行锁：过窗后
	// 回执与到期结算互斥，谁先在行锁内落地，另一个只能看到终态。
	if deadlineAt != nil && !deadlineAt.After(adjudgedAt) {
		return ErrDeadlineReached
	}

	// settlements.command_id 主键兜底：即使上层有漏洞也无法二次结算。
	// settled_at 记录本次裁决时刻：回执、阻断传播与详情视图呈现同一次
	// 有效裁决，现场时间线可与详情查询对账。
	if _, err = tx.Exec(ctx, `
		INSERT INTO settlements (command_id, generation, lease_token, result, settled_at)
		VALUES ($1, $2, $3, $4, $5)`,
		id, currentGen, token, result, adjudgedAt); err != nil {
		return fmt.Errorf("ack settlement insert: %w", err)
	}

	if _, err = tx.Exec(ctx, `
		UPDATE commands
		SET status = $2, updated_at = $3
		WHERE id = $1 AND status = 'pending'`, id, result, adjudgedAt); err != nil {
		return fmt.Errorf("ack status update: %w", err)
	}

	if result == StatusFailed {
		// 失败传播：沿 predecessor_id 边递归展开全部后继，仅把仍 pending
		// （即从未送达、也无有效租约——它们在根失败前根本不可领取）的后继
		// 原子转为 blocked，阻断来源统一记为失败根 $1。
		// 已在终态的行保持原样；传播因此天然幂等。
		if _, err = tx.Exec(ctx, `
			WITH RECURSIVE descendants(did) AS (
				SELECT id FROM commands WHERE predecessor_id = $1
				UNION ALL
				SELECT c.id FROM commands c JOIN descendants d ON c.predecessor_id = d.did
			)
			UPDATE commands
			SET status = 'blocked', blocked_by = $1, updated_at = $2
			WHERE id IN (SELECT did FROM descendants)
			  AND status = 'pending'`, id, adjudgedAt); err != nil {
			return fmt.Errorf("ack block propagation: %w", err)
		}
	}

	return tx.Commit(ctx)
}

// Expire 对指定指令执行到期结算：仅当数据库时钟已到达截止点且指令尚未送达
// （仍为 pending）时，在同一行锁内原子地把它置为 expired 终态，并按现有
// 前驱闭包规则把尚未送达的整条后继链原子转为 blocked（blocked_by 记录
// 到期根编号，即到期根因）。
//
// 裁决时刻与 Ack 同一规则：到期事务可能先在指令行锁上排队，等待期间
// 数据库时钟仍在走——入队时未到点，拿到锁时截止点可能已经到达。因此
// 到点判定使用拿到行锁之后读取的数据库真实时钟（clock_timestamp()），
// 绝不把已经过窗的指令误判为“尚未到期”而留下无法对账的 pending。
//
// 裁决顺序与 Ack 共用同一把行锁，互斥成立：
//   - 指令不存在 → ErrCommandNotFound；
//   - 已在终态（delivered/failed/blocked/expired）→ ErrAlreadySettled：
//     截止前的成功送达不可被追改；重复到期结算也在这里被吸收，
//     绝不会制造第二次结算（settlements.command_id 主键物理兜底）；
//   - 未填写截止时刻，或裁决时刻尚未到达截止点 → ErrDeadlineNotReached，
//     状态不变；
//   - 否则写入 result='expired' 的结算行（无代次、无令牌——expired 不伪造
//     任何回执），翻转状态并传播阻断，全部在单事务内提交。
func (s *Store) Expire(ctx context.Context, id int64) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("expire begin: %w", err)
	}
	defer tx.Rollback(ctx)

	// 首步即取指令行锁：与任何在途的领取/回执/到期/创建事务串行化。
	var status string
	var deadlineAt *time.Time
	err = tx.QueryRow(ctx, `
		SELECT status, deadline_at
		FROM commands
		WHERE id = $1
		FOR UPDATE`, id).Scan(&status, &deadlineAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrCommandNotFound
	}
	if err != nil {
		return fmt.Errorf("expire load command: %w", err)
	}

	// 裁决时刻：拿到行锁之后的数据库真实时钟。不能用事务开始时钟 now()——
	// 它冻结在 BEGIN 时刻，会把锁等待期间已经跨过截止点的请求按入队时刻
	// 误判为“尚未到期”。
	var adjudgedAt time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&adjudgedAt); err != nil {
		return fmt.Errorf("expire adjudication clock: %w", err)
	}

	if status != StatusPending {
		return ErrAlreadySettled
	}
	if deadlineAt == nil || deadlineAt.After(adjudgedAt) {
		return ErrDeadlineNotReached
	}

	// 到期结算是正式结算（settlements 恰一行），但不来自任何租约令牌：
	// generation/lease_token 置 NULL，由迁移中的 CHECK 约束强制该契约。
	// settled_at 记录本次裁决时刻，与详情视图呈现同一次有效裁决。
	if _, err = tx.Exec(ctx, `
		INSERT INTO settlements (command_id, generation, lease_token, result, settled_at)
		VALUES ($1, NULL, NULL, 'expired', $2)`, id, adjudgedAt); err != nil {
		return fmt.Errorf("expire settlement insert: %w", err)
	}

	if _, err = tx.Exec(ctx, `
		UPDATE commands
		SET status = 'expired', updated_at = $2
		WHERE id = $1 AND status = 'pending'`, id, adjudgedAt); err != nil {
		return fmt.Errorf("expire status update: %w", err)
	}

	// 到期传播：与失败传播同一规则——沿 predecessor_id 边递归展开全部后继，
	// 仅把仍 pending 的后继原子转为 blocked，阻断来源统一记为到期根 $1。
	// 已在终态的行保持原样；传播因此天然幂等。
	if _, err = tx.Exec(ctx, `
		WITH RECURSIVE descendants(did) AS (
			SELECT id FROM commands WHERE predecessor_id = $1
			UNION ALL
			SELECT c.id FROM commands c JOIN descendants d ON c.predecessor_id = d.did
		)
		UPDATE commands
		SET status = 'blocked', blocked_by = $1, updated_at = $2
		WHERE id IN (SELECT did FROM descendants)
		  AND status = 'pending'`, id, adjudgedAt); err != nil {
		return fmt.Errorf("expire block propagation: %w", err)
	}

	return tx.Commit(ctx)
}

// scanArgs 适配 Command 字段顺序的扫描助手。
func scanArgs(c *Command) []any {
	return []any{
		&c.ID, &c.Payload, &c.Status, &c.LeaseGeneration, &c.LeaseExpiresAt,
		&c.PredecessorID, &c.BlockedBy, &c.DeadlineAt, &c.CreatedAt, &c.UpdatedAt,
	}
}

// GetCommand 返回指令详情（含领取代次历史与终态）。未知编号返回 ErrCommandNotFound。
//
// 指令本体、各代租约、结算记录在同一个 REPEATABLE READ 只读事务的快照中读取：
// 三者必然对应同一可解释的状态。查询恰好跨过并发领取或成功回执时，
// 也绝不会拼出“旧代次状态 + 新代次租约”或“pending + 已结算”的矛盾视图。
func (s *Store) GetCommand(ctx context.Context, id int64) (*CommandDetail, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("get begin: %w", err)
	}
	defer tx.Rollback(ctx)

	d := &CommandDetail{}
	err = scanCommand(tx.QueryRow(ctx, `
		SELECT `+commandColumns+`
		FROM commands WHERE id = $1`, id), &d.Command)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrCommandNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get command: %w", err)
	}

	rows, err := tx.Query(ctx, `
		SELECT generation, expires_at, claimed_at
		FROM leases WHERE command_id = $1
		ORDER BY generation`, id)
	if err != nil {
		return nil, fmt.Errorf("get leases: %w", err)
	}
	for rows.Next() {
		var lv LeaseView
		if err := rows.Scan(&lv.Generation, &lv.ExpiresAt, &lv.ClaimedAt); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan lease: %w", err)
		}
		d.Leases = append(d.Leases, lv)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var st Settlement
	err = tx.QueryRow(ctx, `
		SELECT generation, result, settled_at
		FROM settlements WHERE command_id = $1`, id).Scan(&st.Generation, &st.Result, &st.SettledAt)
	switch {
	case errors.Is(err, nil):
		d.Settlement = &st
	case errors.Is(err, pgx.ErrNoRows):
	default:
		return nil, fmt.Errorf("get settlement: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("get commit: %w", err)
	}
	return d, nil
}

// ListCommands 按创建顺序返回指令简要列表。
func (s *Store) ListCommands(ctx context.Context, limit int) ([]Command, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+commandColumns+`
		FROM commands
		ORDER BY id
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list commands: %w", err)
	}
	defer rows.Close()
	var out []Command
	for rows.Next() {
		var c Command
		if err := scanCommand(rows, &c); err != nil {
			return nil, fmt.Errorf("scan command: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
