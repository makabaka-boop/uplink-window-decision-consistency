package store_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"deepspace/internal/store"
)

// dbNow 以数据库时钟取当前时刻：截止边界测试的时间轴锚定在裁决唯一时钟上，
// 避免用实例本地时钟构造“数据库看来的过去/未来”。
func dbNow(t *testing.T, pool *pgxpool.Pool) time.Time {
	t.Helper()
	var now time.Time
	if err := pool.QueryRow(context.Background(), `SELECT now()`).Scan(&now); err != nil {
		t.Fatalf("db now(): %v", err)
	}
	return now
}

// createWithDeadline 创建带截止时刻的指令（deadline 直接透传）。
func createWithDeadline(t *testing.T, st *store.Store, payload string, deadline time.Time) *store.Command {
	t.Helper()
	c, err := st.CreateCommand(context.Background(), []byte(payload), nil, &deadline)
	if err != nil {
		t.Fatalf("create with deadline: %v", err)
	}
	return c
}

// TestDeadlineFieldRoundTrips 截止时刻随创建写入、随查询读出；未填写时为 nil，
// 旧指令的视图形态不变。
func TestDeadlineFieldRoundTrips(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	old, err := st.CreateCommand(ctx, []byte(`{"legacy":true}`), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if old.DeadlineAt != nil {
		t.Fatalf("command without deadline must read nil, got %v", old.DeadlineAt)
	}

	deadline := dbNow(t, pool).Add(2 * time.Hour).UTC()
	c := createWithDeadline(t, st, `{"win":"tight"}`, deadline)
	if c.DeadlineAt == nil {
		t.Fatal("created command must carry deadline_at")
	}
	if !c.DeadlineAt.Equal(deadline) {
		t.Fatalf("deadline mismatch: got %v want %v", c.DeadlineAt, deadline)
	}

	d, err := st.GetCommand(ctx, c.ID)
	if err != nil {
		t.Fatal(err)
	}
	if d.DeadlineAt == nil || !d.DeadlineAt.Equal(deadline) {
		t.Fatalf("deadline not persisted in detail view: %+v", d.DeadlineAt)
	}
}

// TestClaimSkipsPastDeadline 过窗指令即使从未被领取（没有任何租约）也不能再被领取：
// 领取扫描跳过它，转而去领编号更大的可用指令；无截止的领取顺序（ID 升序）不变。
func TestClaimSkipsPastDeadline(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	a := createWithDeadline(t, st, `{"a":1}`, dbNow(t, pool).Add(250*time.Millisecond))
	if _, err := st.CreateCommand(ctx, []byte(`{"b":1}`), nil, nil); err != nil {
		t.Fatal(err)
	}

	// 截止前：a 是编号最小的可用指令，正常领取（5s 长租约，保证后续过期是“截止
	// 先于租期”，而不是租期到期）。
	cl, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("pre-deadline claim: %v", err)
	}
	if cl.CommandID != a.ID {
		t.Fatalf("pre-deadline claim got %d want %d", cl.CommandID, a.ID)
	}
	time.Sleep(400 * time.Millisecond)

	// 过窗后：a 被跳过（其长租约本身仍未到期，证明跳过是截止谓词在起作用），
	// 下一条无截止指令被领取，顺序仍是编号升序。
	got, err := st.Claim(ctx, time.Second)
	if err != nil {
		t.Fatalf("claim must skip past-deadline and move on: %v", err)
	}
	if got.CommandID != a.ID+1 {
		t.Fatalf("want next command %d, got %d", a.ID+1, got.CommandID)
	}
	if _, err := st.Claim(ctx, time.Second); !errors.Is(err, store.ErrNoAvailableCommand) {
		t.Fatalf("queue exhausted, got %v", err)
	}
}

// TestClaimSkipsPastDeadlineWithoutLease 没有任何租约、纯粹过窗的指令同样被跳过。
func TestClaimSkipsPastDeadlineWithoutLease(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	a := createWithDeadline(t, st, `{"a":1}`, dbNow(t, pool).Add(200*time.Millisecond))
	b, err := st.CreateCommand(ctx, []byte(`{"b":1}`), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)

	got, err := st.Claim(ctx, time.Second)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if got.CommandID != b.ID {
		t.Fatalf("unleased past-deadline command must be skipped, got %d want %d", got.CommandID, a.ID)
	}
}

// TestExpireRejectsEarlyAndNoDeadline 未到截止点、或未填写截止时刻：到期结算被拒，
// 状态绝不改变。
func TestExpireRejectsEarlyAndNoDeadline(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	future := createWithDeadline(t, st, `{"future":1}`, dbNow(t, pool).Add(time.Hour))
	if err := st.Expire(ctx, future.ID); !errors.Is(err, store.ErrDeadlineNotReached) {
		t.Fatalf("early expire: want ErrDeadlineNotReached, got %v", err)
	}
	d, _ := st.GetCommand(ctx, future.ID)
	if d.Status != store.StatusPending || d.Settlement != nil {
		t.Fatalf("early expire changed state: %+v", d)
	}

	plain, err := st.CreateCommand(ctx, []byte(`{"plain":1}`), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Expire(ctx, plain.ID); !errors.Is(err, store.ErrDeadlineNotReached) {
		t.Fatalf("expire without deadline: want ErrDeadlineNotReached, got %v", err)
	}

	if err := st.Expire(ctx, 987654321); !errors.Is(err, store.ErrCommandNotFound) {
		t.Fatalf("expire unknown: want ErrCommandNotFound, got %v", err)
	}
}

// TestExpireAtDeadlineSettlesOnce 截止边界（deadline == 数据库时钟的已发生时刻）
// 的到期结算：原子置 expired、写恰好一行无回执结算（generation/lease_token 为空），
// 重复到期结算被吸收为 AlreadySettled，绝不制造第二次结算。
func TestExpireAtDeadlineSettlesOnce(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	c := createWithDeadline(t, st, `{"d":1}`, dbNow(t, pool).Add(200*time.Millisecond))
	time.Sleep(350 * time.Millisecond)

	if err := st.Expire(ctx, c.ID); err != nil {
		t.Fatalf("expire at deadline: %v", err)
	}
	d, err := st.GetCommand(ctx, c.ID)
	if err != nil {
		t.Fatal(err)
	}
	if d.Status != store.StatusExpired {
		t.Fatalf("status=%s want expired", d.Status)
	}
	if d.Settlement == nil || d.Settlement.Result != store.StatusExpired {
		t.Fatalf("expired settlement root cause missing: %+v", d.Settlement)
	}
	if d.Settlement.Generation != nil {
		t.Fatalf("expired settlement must not fabricate a generation: %+v", d.Settlement)
	}

	// 带外核对：settlements 恰一行，且 generation/lease_token 物理为 NULL。
	var rows int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM settlements WHERE command_id=$1
		 AND result='expired' AND generation IS NULL AND lease_token IS NULL`, c.ID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("want exactly 1 expired settlement row (no receipt), got %d", rows)
	}

	// 重复到期结算：不再产生第二行，错误为 AlreadySettled。
	if err := st.Expire(ctx, c.ID); !errors.Is(err, store.ErrAlreadySettled) {
		t.Fatalf("duplicate expire: want ErrAlreadySettled, got %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM settlements WHERE command_id=$1`, c.ID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("second settlement fabricated: %d rows", rows)
	}

	// expired 不可领取、不可回执。
	if _, err := st.Claim(ctx, time.Second); !errors.Is(err, store.ErrNoAvailableCommand) {
		t.Fatalf("expired command claimable: %v", err)
	}
	if err := st.Ack(ctx, c.ID, "00", store.StatusDelivered); !errors.Is(err, store.ErrAlreadySettled) {
		t.Fatalf("ack expired: want ErrAlreadySettled, got %v", err)
	}
}

// TestExpireExactBoundaryPredicates 精确边界语义：
// 当 deadline_at 取“过去事务的 now()”时，后续事务的数据库时钟必已到达截止点
// （事务时间戳单调），到期谓词 deadline_at <= now() 成立；回执谓词 deadline_at > now()
// 不成立。无需睡眠即可确定性地覆盖 == 边界。
func TestExpireExactBoundaryPredicates(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	expireEdge := createWithDeadline(t, st, `{"edge":"expire"}`, dbNow(t, pool).Add(time.Hour))
	// 先领取以制造一条尚未到期的长租约，再把截止点钉到“数据库已发生的某一刻”。
	if _, err := st.Claim(ctx, 5*time.Second); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE commands SET deadline_at = now() WHERE id=$1`, expireEdge.ID); err != nil {
		t.Fatal(err)
	}
	if err := st.Expire(ctx, expireEdge.ID); err != nil {
		t.Fatalf("expire at exact deadline boundary: %v", err)
	}
	if d, _ := st.GetCommand(ctx, expireEdge.ID); d.Status != store.StatusExpired {
		t.Fatalf("status=%s want expired at exact boundary", d.Status)
	}

	ackEdge := createWithDeadline(t, st, `{"edge":"ack"}`, dbNow(t, pool).Add(time.Hour))
	cl, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := pool.Exec(ctx,
		`UPDATE commands SET deadline_at = now() WHERE id=$1`, ackEdge.ID); err != nil {
		t.Fatal(err)
	}
	// 数据库时钟已到截止点的同一刻：租期再长（5s）也不能送达。
	if err := st.Ack(ctx, ackEdge.ID, cl.LeaseToken, store.StatusDelivered); !errors.Is(err, store.ErrDeadlineReached) {
		t.Fatalf("ack at exact deadline boundary: want ErrDeadlineReached, got %v", err)
	}
	if d, _ := st.GetCommand(ctx, ackEdge.ID); d.Status != store.StatusPending || d.Settlement != nil {
		t.Fatalf("rejected ack must leave state pending: %+v", d)
	}
}

// TestAckAfterDeadlineRejectedEvenWithLiveLease 截止后，旧租约即使尚未达到自身租期，
// 也不能送达；状态保持 pending，等待到期结算接管。
func TestAckAfterDeadlineRejectedEvenWithLiveLease(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	c := createWithDeadline(t, st, `{"d":1}`, dbNow(t, pool).Add(200*time.Millisecond))
	cl, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if cl.CommandID != c.ID {
		t.Fatalf("claim got %d want %d", cl.CommandID, c.ID)
	}
	time.Sleep(350 * time.Millisecond)

	if err := st.Ack(ctx, c.ID, cl.LeaseToken, store.StatusDelivered); !errors.Is(err, store.ErrDeadlineReached) {
		t.Fatalf("post-deadline ack: want ErrDeadlineReached, got %v", err)
	}
	d, _ := st.GetCommand(ctx, c.ID)
	if d.Status != store.StatusPending || d.Settlement != nil {
		t.Fatalf("rejected ack changed state: %+v", d)
	}

	// 到期结算随后接管。
	if err := st.Expire(ctx, c.ID); err != nil {
		t.Fatalf("expire after rejected ack: %v", err)
	}
}

// TestDeliveredBeforeDeadlineCannotBeOverturned 截止前成功送达不可被追改：
// 之后（即使数据库时钟已过截止点）到期结算只能看到终态。
func TestDeliveredBeforeDeadlineCannotBeOverturned(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	c := createWithDeadline(t, st, `{"d":1}`, dbNow(t, pool).Add(200*time.Millisecond))
	cl, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Ack(ctx, c.ID, cl.LeaseToken, store.StatusDelivered); err != nil {
		t.Fatalf("pre-deadline ack: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	if err := st.Expire(ctx, c.ID); !errors.Is(err, store.ErrAlreadySettled) {
		t.Fatalf("expire after delivery: want ErrAlreadySettled, got %v", err)
	}
	d, _ := st.GetCommand(ctx, c.ID)
	if d.Status != store.StatusDelivered {
		t.Fatalf("delivered state overturned to %s", d.Status)
	}
	if d.Settlement == nil || d.Settlement.Result != store.StatusDelivered {
		t.Fatalf("settlement altered: %+v", d.Settlement)
	}
}

// TestExpireBlocksSuccessors 到期根因按现有前驱闭包规则传播：尚未送达的整条后继链
// 原子转 blocked，blocked_by 记到期根，不写租约、不写结算；之后新建后继出生即 blocked。
func TestExpireBlocksSuccessors(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	root := createWithDeadline(t, st, `{"root":1}`, dbNow(t, pool).Add(200*time.Millisecond))
	c1, err := st.CreateCommand(ctx, []byte(`{"n":1}`), pid(root.ID), nil)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := st.CreateCommand(ctx, []byte(`{"n":2}`), pid(c1.ID), nil)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(350 * time.Millisecond)

	if err := st.Expire(ctx, root.ID); err != nil {
		t.Fatalf("expire root: %v", err)
	}
	for _, id := range []int64{c1.ID, c2.ID} {
		d, err := st.GetCommand(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if d.Status != store.StatusBlocked || d.BlockedBy == nil || *d.BlockedBy != root.ID {
			t.Fatalf("node %d: %+v want blocked by expired root %d", id, d.Command, root.ID)
		}
		if len(d.Leases) != 0 || d.Settlement != nil {
			t.Fatalf("blocked node %d fabricated leases=%d settlement=%+v", id, len(d.Leases), d.Settlement)
		}
	}

	// 到期之后才挂的后继：出生即 blocked，继承到期根因。
	late, err := st.CreateCommand(ctx, []byte(`{"late":true}`), pid(root.ID), nil)
	if err != nil {
		t.Fatal(err)
	}
	if late.Status != store.StatusBlocked || late.BlockedBy == nil || *late.BlockedBy != root.ID {
		t.Fatalf("late successor born %+v, want blocked by %d", late, root.ID)
	}

	// 链终态，没有任何可领取指令。
	if _, err := st.Claim(ctx, time.Second); !errors.Is(err, store.ErrNoAvailableCommand) {
		t.Fatalf("expired/blocked chain claimable: %v", err)
	}
}

// TestBornBlockedWhenPredecessorAlreadyExpired 与失败根对称：直接前驱已 expired 时
// 新建后继出生即 blocked，阻断来源指向到期根。
func TestBornBlockedWhenPredecessorAlreadyExpired(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	root := createWithDeadline(t, st, `{"root":1}`, dbNow(t, pool).Add(250*time.Millisecond))
	time.Sleep(400 * time.Millisecond)
	if err := st.Expire(ctx, root.ID); err != nil {
		t.Fatal(err)
	}
	child, err := st.CreateCommand(ctx, []byte(`{"c":1}`), pid(root.ID), nil)
	if err != nil {
		t.Fatal(err)
	}
	if child.Status != store.StatusBlocked || child.BlockedBy == nil || *child.BlockedBy != root.ID {
		t.Fatalf("child of expired root: %+v", child)
	}
}

// TestAckExpireRowLockArbitration 回执与到期结算在同一行锁下互斥：
// 用一把“旁观者”行锁制造排队，裁决顺序完全确定——
//   - 过窗场景：旁观者持锁时回执先到并排队；锁释放后回执在行锁内看到截止谓词不成立，
//     必须 ErrDeadlineReached 且状态不变，随后到期结算落 expired；
//   - 未过窗场景：截止前回执落 delivered，之后到期结算只能看到终态（不可追改）。
func TestAckExpireRowLockArbitration(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	// —— 场景一：截止后旧租约不能送达 ——
	past := createWithDeadline(t, st, `{"past":1}`, dbNow(t, pool).Add(250*time.Millisecond))
	pastClaim, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)

	holder, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Exec(ctx, `SELECT 1 FROM commands WHERE id=$1 FOR UPDATE`, past.ID); err != nil {
		t.Fatal(err)
	}
	ackErr := make(chan error, 1)
	go func() {
		ackErr <- st.Ack(ctx, past.ID, pastClaim.LeaseToken, store.StatusDelivered)
	}()
	time.Sleep(150 * time.Millisecond) // 确保回执已在锁队列中等待
	if err := holder.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-ackErr; !errors.Is(err, store.ErrDeadlineReached) {
		t.Fatalf("queued ack past deadline: want ErrDeadlineReached, got %v", err)
	}
	if d, _ := st.GetCommand(ctx, past.ID); d.Status != store.StatusPending {
		t.Fatalf("state changed by rejected ack: %s", d.Status)
	}
	if err := st.Expire(ctx, past.ID); err != nil {
		t.Fatalf("expire after queued ack: %v", err)
	}
	if d, _ := st.GetCommand(ctx, past.ID); d.Status != store.StatusExpired {
		t.Fatalf("status=%s want expired", d.Status)
	}

	// —— 场景二：截止前送达不可追改 ——
	soon := createWithDeadline(t, st, `{"soon":1}`, dbNow(t, pool).Add(200*time.Millisecond))
	soonClaim, err := st.Claim(ctx, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Ack(ctx, soon.ID, soonClaim.LeaseToken, store.StatusDelivered); err != nil {
		t.Fatalf("pre-deadline ack: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if err := st.Expire(ctx, soon.ID); !errors.Is(err, store.ErrAlreadySettled) {
		t.Fatalf("expire overturning delivery: want ErrAlreadySettled, got %v", err)
	}
}

// TestExpireAckConcurrentExactlyOneSettlement 过窗指令面对并发的到期结算与回执：
// 行锁串行化后恰有一个终态——要么 expired（settlements 恰一行 result=expired），
// 不允许出现“既过期又送达”或两行结算。
func TestExpireAckConcurrentExactlyOneSettlement(t *testing.T) {
	pool, _ := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	for round := 0; round < 8; round++ {
		c := createWithDeadline(t, st, fmt.Sprintf(`{"round":%d}`, round),
			dbNow(t, pool).Add(200*time.Millisecond))
		claim, err := st.Claim(ctx, 5*time.Second)
		if err != nil || claim.CommandID != c.ID {
			t.Fatalf("round %d claim: %+v %v", round, claim, err)
		}
		time.Sleep(320 * time.Millisecond)

		const writers = 10
		start := make(chan struct{})
		var wg sync.WaitGroup
		var mu sync.Mutex
		results := map[error]int{}
		wg.Add(writers)
		for i := 0; i < writers; i++ {
			go func(i int) {
				defer wg.Done()
				<-start
				var e error
				if i%2 == 0 {
					e = st.Expire(ctx, c.ID)
				} else {
					e = st.Ack(ctx, c.ID, claim.LeaseToken, store.StatusDelivered)
				}
				mu.Lock()
				results[e]++
				mu.Unlock()
			}(i)
		}
		close(start)
		wg.Wait()

		d, err := st.GetCommand(ctx, c.ID)
		if err != nil {
			t.Fatal(err)
		}
		if d.Status != store.StatusExpired {
			t.Fatalf("round %d: past-deadline verdict must be expired, got %s", round, d.Status)
		}
		var n int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM settlements WHERE command_id=$1`, c.ID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("round %d: settlements=%d want exactly 1; errors=%v", round, n, results)
		}
		// 非过期的写入都必须以冲突哨兵失败，不允许“成功送达”。
		if results[nil] != 1 {
			t.Fatalf("round %d: want exactly 1 successful settle, got errors=%v", round, results)
		}
	}
}

// TestExpiredPersistsAcrossRestart 重启等价物：新池重新迁移后，expired 终态、
// 无回执结算与后继阻断全部保持；重复结算仍被拒绝。
func TestExpiredPersistsAcrossRestart(t *testing.T) {
	pool, dbURL := openTestDB(t)
	st := store.New(pool)
	ctx := context.Background()

	root := createWithDeadline(t, st, `{"persist":1}`, dbNow(t, pool).Add(250*time.Millisecond))
	child, err := st.CreateCommand(ctx, []byte(`{"c":1}`), pid(root.ID), nil)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)
	if err := st.Expire(ctx, root.ID); err != nil {
		t.Fatal(err)
	}
	pool.Close()

	pool2, err := openPool(dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool2.Close()
	if err := store.Migrate(ctx, pool2); err != nil {
		t.Fatalf("re-migrate: %v", err)
	}
	st2 := store.New(pool2)

	d, err := st2.GetCommand(ctx, root.ID)
	if err != nil {
		t.Fatal(err)
	}
	if d.Status != store.StatusExpired || d.Settlement == nil ||
		d.Settlement.Result != store.StatusExpired || d.Settlement.Generation != nil {
		t.Fatalf("expired state lost after restart: %+v", d)
	}
	cd, _ := st2.GetCommand(ctx, child.ID)
	if cd.Status != store.StatusBlocked || cd.BlockedBy == nil || *cd.BlockedBy != root.ID {
		t.Fatalf("blocked successor lost after restart: %+v", cd.Command)
	}
	if err := st2.Expire(ctx, root.ID); !errors.Is(err, store.ErrAlreadySettled) {
		t.Fatalf("re-expire after restart: want ErrAlreadySettled, got %v", err)
	}
	if _, err := st2.Claim(ctx, time.Second); !errors.Is(err, store.ErrNoAvailableCommand) {
		t.Fatalf("expired chain claimable after restart: %v", err)
	}
}
