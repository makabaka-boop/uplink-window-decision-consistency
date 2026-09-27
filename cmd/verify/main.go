// Command verify 是一次性验收服务（compose: verify）。
//
// 它完整执行需求中的验收交错：
//  1. 并发抢领唯一指令，断言恰有一方成功，其余得到 204；
//  2. 用 100ms 租期等待到期后重新领取，断言代次推进且新令牌不同；
//  3. 逆序确认（旧令牌先、新令牌后），断言旧令牌 409 且不改变状态，
//     新令牌置终态；重复确认得到 409；
//  4. 重启 API 进程（同库重连），重启前后终态唯一且一致；
//  5. 直接查询 PostgreSQL 作为带外证据：settlements 仅一行、状态唯一；
//  6. 读一致性：领取/回执交错期间高频并发 GET，断言任何响应都不存在跨代租约
//     或“待处理且已结算”的组合；失败传播期间下游不呈现虚假可执行状态；
//  7. 实例时钟偏移：两台时钟分别偏移 ±1 小时的实例对同一凭证给出相同裁决；
//  8. 执行截止时刻：过窗跳过领取、截止后回执/到期结算互斥、expired 无回执结算、
//     后继阻断、重复结算不产生第二行、重启保持；回执/到期结算在指令行锁队列
//     等待期间跨过截止点时，裁决以拿到锁之后的数据库时钟为准。
//
// 退出码 0 表示全部通过；任何断言失败都会打印详细错误并以非零退出。
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type failure struct{ msg string }

func (f *failure) Error() string { return f.msg }
func fail(format string, a ...any) error {
	return &failure{msg: fmt.Sprintf(format, a...)}
}

type verifier struct {
	baseURL string
	dbURL   string
	client  *http.Client
}

func main() {
	v := &verifier{
		baseURL: envOr("BASE_URL", "http://api:8080"),
		dbURL:   envOr("DATABASE_URL", "postgres://postgres:postgres@db:5432/deepspace?sslmode=disable"),
		client:  &http.Client{Timeout: 10 * time.Second},
	}
	ctx := context.Background()
	if err := v.waitForAPI(ctx); err != nil {
		fatal(err)
	}
	if err := v.run(ctx); err != nil {
		fatal(err)
	}
	fmt.Println("VERIFY: all acceptance checks passed")
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "VERIFY FAILED: %v\n", err)
	os.Exit(1)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type apiError struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type claimOut struct {
	CommandID      int64     `json:"command_id"`
	Generation     int64     `json:"generation"`
	LeaseToken     string    `json:"lease_token"`
	LeaseExpiresAt time.Time `json:"lease_expires_at"`
}

type commandOut struct {
	ID              int64            `json:"id"`
	Status          string           `json:"status"`
	PredecessorID   *int64           `json:"predecessor_id"`
	BlockedBy       *int64           `json:"blocked_by"`
	Deadline        *time.Time       `json:"deadline_at"`
	LeaseGeneration int64            `json:"lease_generation"`
	Settlement      map[string]any   `json:"settlement"`
	Leases          []map[string]any `json:"leases"`
}

// createCommand 创建一条指令（predecessor 传 nil 表示无前驱），断言 201 并返回。
func (v *verifier) createCommand(ctx context.Context, payload map[string]any, predecessor *int64) commandOut {
	body := map[string]any{"payload": payload}
	if predecessor != nil {
		body["predecessor_id"] = *predecessor
	}
	status, raw := v.doJSON(ctx, http.MethodPost, "/commands", body)
	if status != http.StatusCreated {
		fatal(fail("create command: status=%d body=%s", status, raw))
	}
	return decode[commandOut](raw)
}

// createCommandWithDeadline 创建带 UTC 执行截止时刻的指令，断言 201 并返回。
func (v *verifier) createCommandWithDeadline(ctx context.Context, payload map[string]any, deadline time.Time) commandOut {
	body := map[string]any{"payload": payload, "deadline_at": deadline.UTC().Format(time.RFC3339Nano)}
	status, raw := v.doJSON(ctx, http.MethodPost, "/commands", body)
	if status != http.StatusCreated {
		fatal(fail("create command with deadline: status=%d body=%s", status, raw))
	}
	out := decode[commandOut](raw)
	if out.Deadline == nil {
		fatal(fail("created command missing deadline echo: %s", raw))
	}
	return out
}

// doJSON 发送 JSON 请求并返回状态码与原始体。
func (v *verifier) doJSON(ctx context.Context, method, path string, body any) (int, []byte) {
	code, raw, err := v.doJSONErr(ctx, method, path, body)
	if err != nil {
		fatal(fmt.Errorf("%s %s: %w", method, path, err))
	}
	return code, raw
}

// doJSONErr 同 doJSON，但把传输层错误返回给调用方（用于健康检查重试）。
func (v *verifier) doJSONErr(ctx context.Context, method, path string, body any) (int, []byte, error) {
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, v.baseURL+path, rdr)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := v.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, raw, nil
}

func decode[T any](raw []byte) T {
	var out T
	if err := json.Unmarshal(raw, &out); err != nil {
		fatal(fmt.Errorf("decode response %s: %w", string(raw), err))
	}
	return out
}

func (v *verifier) waitForAPI(ctx context.Context) error {
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		status, _, err := v.doJSONErr(ctx, http.MethodGet, "/healthz", nil)
		if err == nil && status == http.StatusOK {
			return nil
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("healthz status=%d", status)
		}
		time.Sleep(250 * time.Millisecond)
	}
	return fail("api did not become healthy within 60s: %v", lastErr)
}

func (v *verifier) run(ctx context.Context) error {
	// 场景零：输入校验契约。
	if err := v.checkValidation(ctx); err != nil {
		return err
	}

	// 前驱链：链式解锁、失败传播、阻断来源与旧指令兼容。
	if err := v.checkPredecessorChains(ctx); err != nil {
		return err
	}

	// 读一致性：领取/回执交错期间的并发查询不得出现跨代租约或“待处理且已结算”。
	if err := v.checkReadConsistency(ctx); err != nil {
		return err
	}

	// 实例时钟偏移：两台时钟分别偏移 ±1h 的实例对同一凭证必须给出相同裁决。
	if err := v.checkClockSkewAcrossInstances(ctx); err != nil {
		return err
	}

	// 执行截止时刻：到期结算、领取跳过、回执竞态、后继阻断与重启保持。
	if err := v.checkDeadlines(ctx); err != nil {
		return err
	}

	// 锁等待跨窗裁决：回执/到期结算在指令行锁队列中等待期间跨过截止点，
	// 裁决以拿到锁之后的数据库时钟为准，终态不因请求等待顺序而改变。
	if err := v.checkDeadlineLockQueue(ctx); err != nil {
		return err
	}

	// 场景一：创建唯一指令，并发抢领。
	status, raw := v.doJSON(ctx, http.MethodPost, "/commands", map[string]any{
		"payload": map[string]any{
			"type":      "uplink",
			"target":    "mars-relay-7",
			"sequence":  1,
			"issued_at": "2026-09-18T00:00:00Z",
		},
	})
	if status != http.StatusCreated {
		return fail("create command: status=%d body=%s", status, raw)
	}
	created := decode[commandOut](raw)
	if created.ID <= 0 || created.Status != "pending" {
		return fail("unexpected created command: %s", raw)
	}
	cmdPath := fmt.Sprintf("/commands/%d", created.ID)
	fmt.Printf("created command id=%d\n", created.ID)

	const n = 16
	start := make(chan struct{})
	var wg sync.WaitGroup
	statuses := make([]int, n)
	bodies := make([][]byte, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			statuses[i], bodies[i] = v.doJSON(ctx, http.MethodPost, "/claims",
				map[string]any{"lease_duration_ms": 2000})
		}(i)
	}
	close(start)
	wg.Wait()

	winners := 0
	var first claimOut
	for i := 0; i < n; i++ {
		switch statuses[i] {
		case http.StatusOK:
			winners++
			first = decode[claimOut](bodies[i])
		case http.StatusNoContent:
		default:
			return fail("claim goroutine %d got unexpected status %d: %s", i, statuses[i], bodies[i])
		}
	}
	if winners != 1 {
		return fail("expected exactly 1 winning claim, got %d", winners)
	}
	if first.CommandID != created.ID || first.Generation != 1 || len(first.LeaseToken) < 32 {
		return fail("unexpected winning claim: %+v", first)
	}
	oldToken := first.LeaseToken
	fmt.Printf("concurrent claim: exactly 1 winner, gen=%d token=%s…\n", first.Generation, oldToken[:8])

	// 租约有效期内，再领取应无可用（204）。
	var st int
	if st, _ = v.doJSON(ctx, http.MethodPost, "/claims",
		map[string]any{"lease_duration_ms": 500}); st != http.StatusNoContent {
		return fail("claim during active lease should be 204, got %d", st)
	}

	// 场景二：等待第一代租期到期后重新领取（断言代次推进、令牌换新）。
	fmt.Println("waiting for first lease (2000ms) to expire…")
	time.Sleep(2150 * time.Millisecond)

	st, raw = v.doJSON(ctx, http.MethodPost, "/claims", map[string]any{"lease_duration_ms": 100})
	if st != http.StatusOK {
		return fail("reclaim after expiry: status=%d body=%s", st, raw)
	}
	second := decode[claimOut](raw)
	if second.CommandID != created.ID {
		return fail("reclaimed wrong command id: %+v", second)
	}
	if second.Generation != 2 {
		return fail("expected generation 2 after reclaim, got %d", second.Generation)
	}
	if second.LeaseToken == oldToken {
		return fail("new lease token must differ from the old one")
	}
	newToken := second.LeaseToken
	fmt.Printf("reclaim after expiry: gen=%d new token=%s…\n", second.Generation, newToken[:8])

	// 新代次生效期间：旧令牌迟到确认 -> 409，状态仍 pending。
	st, raw = v.doJSON(ctx, http.MethodPost, cmdPath+"/ack",
		map[string]any{"lease_token": oldToken, "result": "delivered"})
	if st != http.StatusConflict {
		return fail("stale old token ack: expected 409, got %d body=%s", st, raw)
	}
	ae := decode[apiError](raw)
	if ae.Error.Code != "lease_expired" {
		return fail("stale ack error code: expected lease_expired, got %q", ae.Error.Code)
	}
	got := v.getCommand(ctx, created.ID)
	if got.Status != "pending" || got.Settlement != nil {
		return fail("stale ack must not change state, got status=%s settlement=%v",
			got.Status, got.Settlement)
	}
	fmt.Println("old token late ack -> 409 lease_expired, state unchanged (pending)")

	// 逆序场景的另一半：新令牌若先到期，会产生第三代；构造“先等到第二代到期，
	// 再领第三代，然后用第二代旧令牌确认”，覆盖真正的逆序确认交错。
	fmt.Println("waiting for second lease (100ms) to expire…")
	time.Sleep(350 * time.Millisecond)

	st, raw = v.doJSON(ctx, http.MethodPost, "/claims", map[string]any{"lease_duration_ms": 3000})
	if st != http.StatusOK {
		return fail("third claim: expected 200, got %d body=%s", st, raw)
	}
	third := decode[claimOut](raw)
	if third.Generation != 3 || third.LeaseToken == newToken || third.LeaseToken == oldToken {
		return fail("third claim must advance generation and issue a fresh token: %+v", third)
	}
	holderToken := third.LeaseToken
	fmt.Printf("third claim: gen=%d token=%s…\n", third.Generation, holderToken[:8])

	// 第二代令牌（旧持有者）迟到确认 -> 409。
	st, raw = v.doJSON(ctx, http.MethodPost, cmdPath+"/ack",
		map[string]any{"lease_token": newToken, "result": "failed"})
	if st != http.StatusConflict {
		return fail("gen-2 stale ack: expected 409, got %d body=%s", st, raw)
	}
	got = v.getCommand(ctx, created.ID)
	if got.Status != "pending" {
		return fail("state must remain pending after gen-2 stale ack, got %s", got.Status)
	}
	fmt.Println("gen-2 token late ack -> 409, state still pending")

	// 当前持有者以 failed 结算终态。
	st, raw = v.doJSON(ctx, http.MethodPost, cmdPath+"/ack",
		map[string]any{"lease_token": holderToken, "result": "failed"})
	if st != http.StatusOK {
		return fail("current holder ack: expected 200, got %d body=%s", st, raw)
	}
	settled := decode[commandOut](raw)
	if settled.Status != "failed" || settled.Settlement["generation"].(float64) != 3 {
		return fail("unexpected settled body: %s", raw)
	}
	fmt.Println("current holder settled command -> failed (gen=3)")

	// 旧令牌、当前令牌重复确认 -> 均 409，终态不得被覆盖。
	for i, tok := range []string{oldToken, newToken, holderToken} {
		st, raw = v.doJSON(ctx, http.MethodPost, cmdPath+"/ack",
			map[string]any{"lease_token": tok, "result": "delivered"})
		if st != http.StatusConflict {
			return fail("duplicate ack #%d: expected 409, got %d body=%s", i, st, raw)
		}
	}
	got = v.getCommand(ctx, created.ID)
	if got.Status != "failed" {
		return fail("terminal state must remain failed, got %s", got.Status)
	}
	if got.Settlement["result"] != "failed" {
		return fail("settlement result overwritten: %v", got.Settlement)
	}
	fmt.Println("all duplicate/overriding acks -> 409, terminal state stays failed")

	// 终态指令不会再被领取：204。
	if st, _ := v.doJSON(ctx, http.MethodPost, "/claims",
		map[string]any{"lease_duration_ms": 100}); st != http.StatusNoContent {
		return fail("settled command must not be claimable, got %d", st)
	}

	// 观察性：代次历史应为 3 代。
	if len(got.Leases) != 3 {
		return fail("expected 3 observable lease generations, got %d", len(got.Leases))
	}

	// 带外：直接查库，settlements 恰一行。
	if err := v.assertDatabase(ctx, created.ID); err != nil {
		return err
	}

	// 场景三：重启 API 进程，重启后终态唯一且一致。
	if err := v.restartAPIAndRecheck(ctx, cmdPath); err != nil {
		return err
	}
	return nil
}

// checkPredecessorChains 验证前驱链完整契约：
//   - 创建可带可选 predecessor_id，只能引用已存在的较早编号；非法类型 422、
//     未知编号 422 predecessor_not_found；
//   - 前驱未全部送达的后继不可领取（不预发租约），根送达后逐节解锁；
//   - 链上某节点 failed，其尚未领取的后继原子转只读 blocked、记录同一阻断来源，
//     不写租约或结算，且 blocked 不可领取/不可确认；
//   - 无 predecessor_id 的旧指令行为完全不变。
func (v *verifier) checkPredecessorChains(ctx context.Context) error {
	// 非法 predecessor_id 类型/取值 -> 422 invalid_predecessor_id。
	for _, body := range []string{
		`{"payload":{},"predecessor_id":"1"}`,
		`{"payload":{},"predecessor_id":true}`,
		`{"payload":{},"predecessor_id":0}`,
		`{"payload":{},"predecessor_id":-7}`,
	} {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, v.baseURL+"/commands",
			bytes.NewReader([]byte(body)))
		req.Header.Set("Content-Type", "application/json")
		resp, err := v.client.Do(req)
		if err != nil {
			return fmt.Errorf("bad predecessor request: %w", err)
		}
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnprocessableEntity {
			return fail("invalid predecessor %s: status=%d body=%s", body, resp.StatusCode, raw)
		}
		if decode[apiError](raw).Error.Code != "invalid_predecessor_id" {
			return fail("invalid predecessor %s: wrong code %s", body, raw)
		}
	}
	// 未知（更晚/不存在）编号 -> 422 predecessor_not_found。
	st, raw := v.doJSON(ctx, http.MethodPost, "/commands",
		map[string]any{"payload": map[string]any{}, "predecessor_id": 987654321})
	if st != http.StatusUnprocessableEntity || decode[apiError](raw).Error.Code != "predecessor_not_found" {
		return fail("unknown predecessor: status=%d body=%s", st, raw)
	}

	// 链 r0 <- r1 <- r2 <- r3。无前驱旧指令的兼容性在链裁决结束后单独验证，
	// 以免它作为独立根提前进入可领取集合干扰链式解锁断言。
	r0 := v.createCommand(ctx, map[string]any{"node": 0}, nil)
	r1 := v.createCommand(ctx, map[string]any{"node": 1}, &r0.ID)
	r2 := v.createCommand(ctx, map[string]any{"node": 2}, &r1.ID)
	r3 := v.createCommand(ctx, map[string]any{"node": 3}, &r2.ID)
	for _, c := range []commandOut{r1, r2, r3} {
		if c.Status != "pending" {
			return fail("chain node %d created as %s, want pending", c.ID, c.Status)
		}
	}
	fmt.Printf("predecessor chain created: r0=%d r1=%d r2=%d r3=%d\n", r0.ID, r1.ID, r2.ID, r3.ID)

	// 最小可领取是 r0（编号最小且无前驱）；持有期间其余全部等待。
	claim := func() (int, claimOut) {
		s, b := v.doJSON(ctx, http.MethodPost, "/claims", map[string]any{"lease_duration_ms": 2000})
		var cl claimOut
		if s == http.StatusOK {
			cl = decode[claimOut](b)
		}
		return s, cl
	}
	s, cl := claim()
	if s != http.StatusOK || cl.CommandID != r0.ID {
		return fail("first chain claim: status=%d claim=%+v want r0=%d", s, cl, r0.ID)
	}
	if s, _ := claim(); s != http.StatusNoContent {
		return fail("while r0 leased, successors must not be pre-leased: status=%d", s)
	}
	if s, b := v.doJSON(ctx, http.MethodPost, fmt.Sprintf("/commands/%d/ack", r0.ID),
		map[string]any{"lease_token": cl.LeaseToken, "result": "delivered"}); s != http.StatusOK {
		return fail("deliver r0: status=%d body=%s", s, b)
	}
	// r0 delivered -> r1 解锁为最小可领取。
	if s, cl := claim(); s != http.StatusOK || cl.CommandID != r1.ID {
		return fail("after r0 delivered, want r1=%d claimable: status=%d %+v", r1.ID, s, cl)
	} else {
		// r1 失败 -> r1 failed；r2、r3（均未领取）原子 blocked，阻断来源=r1。
		if s, b := v.doJSON(ctx, http.MethodPost, fmt.Sprintf("/commands/%d/ack", r1.ID),
			map[string]any{"lease_token": cl.LeaseToken, "result": "failed"}); s != http.StatusOK {
			return fail("fail r1: status=%d body=%s", s, b)
		}
	}
	for _, id := range []int64{r2.ID, r3.ID} {
		got := v.getCommand(ctx, id)
		if got.Status != "blocked" || got.BlockedBy == nil || *got.BlockedBy != r1.ID {
			return fail("node %d: status=%s blocked_by=%v want blocked by r1=%d",
				id, got.Status, got.BlockedBy, r1.ID)
		}
		if len(got.Leases) != 0 || got.Settlement != nil {
			return fail("blocked node %d must have no leases/settlement: leases=%d settlement=%v",
				id, len(got.Leases), got.Settlement)
		}
	}
	// blocked 链不可领取；对 blocked 后继的确认被 409 already_settled 拒绝。
	if s, _ := claim(); s != http.StatusNoContent {
		return fail("blocked successors must not be claimable: status=%d", s)
	}
	if s, b := v.doJSON(ctx, http.MethodPost, fmt.Sprintf("/commands/%d/ack", r2.ID),
		map[string]any{"lease_token": "00", "result": "delivered"}); s != http.StatusConflict ||
		decode[apiError](b).Error.Code != "already_settled" {
		return fail("ack blocked r2: status=%d body=%s want 409 already_settled", s, b)
	}
	// 失败根 r1 之后再挂后继：出生即 blocked，继承阻断来源 r1。
	born := v.createCommand(ctx, map[string]any{"late": true}, &r1.ID)
	if born.Status != "blocked" || born.BlockedBy == nil || *born.BlockedBy != r1.ID {
		return fail("late successor of failed root must be born blocked: %+v", born)
	}

	// 旧指令不受影响：无 predecessor_id 的指令在 blocked 链旁仍正常创建与领取。
	// 它此刻是全库唯一可领取指令（r0 delivered、r1 failed、r2/r3 blocked）。
	legacy := v.createCommand(ctx, map[string]any{"legacy": true}, nil)
	if legacy.PredecessorID != nil || legacy.BlockedBy != nil {
		return fail("legacy command must carry no predecessor/blocked_by: %+v", legacy)
	}
	s, cl = claim()
	if s != http.StatusOK || cl.CommandID != legacy.ID {
		return fail("legacy command must be the claimable one: status=%d claim=%+v want %d",
			s, cl, legacy.ID)
	}
	if s, b := v.doJSON(ctx, http.MethodPost, fmt.Sprintf("/commands/%d/ack", legacy.ID),
		map[string]any{"lease_token": cl.LeaseToken, "result": "delivered"}); s != http.StatusOK {
		return fail("deliver legacy: status=%d body=%s", s, b)
	}
	if s, _ := claim(); s != http.StatusNoContent {
		return fail("after legacy delivered nothing should be claimable: status=%d", s)
	}
	fmt.Println("predecessor chain checks passed (sequential unlock + failure propagation + legacy)")
	return nil
}

func (v *verifier) checkValidation(ctx context.Context) error {
	// 非法租期：过小、过大、缺失。
	for _, body := range []map[string]any{
		{"lease_duration_ms": 99},
		{"lease_duration_ms": 5001},
		{"lease_duration_ms": -1},
		{},
	} {
		st, raw := v.doJSON(ctx, http.MethodPost, "/claims", body)
		if st != http.StatusUnprocessableEntity {
			return fail("invalid lease %v: expected 422, got %d body=%s", body, st, raw)
		}
		ae := decode[apiError](raw)
		if ae.Error.Code == "" {
			return fail("422 must carry stable error code, body=%s", raw)
		}
	}
	// 无可用指令 -> 204，且无响应体。
	st, raw := v.doJSON(ctx, http.MethodPost, "/claims", map[string]any{"lease_duration_ms": 100})
	if st != http.StatusNoContent || len(bytes.TrimSpace(raw)) != 0 {
		return fail("empty claim: expected 204 no body, got %d body=%q", st, raw)
	}
	// 未知编号 -> 404（查询与确认均如此）。
	if st, raw := v.doJSON(ctx, http.MethodGet, "/commands/99999999", nil); st != http.StatusNotFound {
		return fail("unknown GET: expected 404, got %d body=%s", st, raw)
	}
	if st, raw := v.doJSON(ctx, http.MethodPost, "/commands/99999999/ack",
		map[string]any{"lease_token": "x", "result": "delivered"}); st != http.StatusNotFound {
		return fail("unknown ack: expected 404, got %d body=%s", st, raw)
	}
	// 非法结果 / 缺失令牌 -> 422。
	if st, raw := v.doJSON(ctx, http.MethodPost, "/commands/1/ack",
		map[string]any{"lease_token": "x", "result": "lost_in_space"}); st != http.StatusUnprocessableEntity {
		return fail("invalid result: expected 422, got %d body=%s", st, raw)
	}
	if st, raw := v.doJSON(ctx, http.MethodPost, "/commands/1/ack",
		map[string]any{"result": "delivered"}); st != http.StatusUnprocessableEntity {
		return fail("missing token: expected 422, got %d body=%s", st, raw)
	}
	// 载荷缺失 -> 422；非法 JSON -> 400。
	if st, raw := v.doJSON(ctx, http.MethodPost, "/commands",
		map[string]any{"payload": nil}); st != http.StatusUnprocessableEntity {
		return fail("null payload: expected 422, got %d body=%s", st, raw)
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, v.baseURL+"/commands",
		bytes.NewReader([]byte("{not json")))
	req.Header.Set("Content-Type", "application/json")
	resp, err := v.client.Do(req)
	if err != nil {
		return fmt.Errorf("malformed json request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		return fail("malformed json: expected 400, got %d", resp.StatusCode)
	}
	fmt.Println("validation contract checks passed")
	return nil
}

func (v *verifier) getCommand(ctx context.Context, id int64) commandOut {
	st, raw := v.doJSON(ctx, http.MethodGet, fmt.Sprintf("/commands/%d", id), nil)
	if st != http.StatusOK {
		fatal(fmt.Errorf("get command: status=%d body=%s", st, raw))
	}
	return decode[commandOut](raw)
}

func (v *verifier) assertDatabase(ctx context.Context, id int64) error {
	pool, err := pgxpool.New(ctx, v.dbURL)
	if err != nil {
		return fmt.Errorf("db connect: %w", err)
	}
	defer pool.Close()

	var status string
	var gen int64
	if err := pool.QueryRow(ctx,
		`SELECT status, lease_generation FROM commands WHERE id=$1`, id).Scan(&status, &gen); err != nil {
		return fmt.Errorf("db query command: %w", err)
	}
	var settlementCount int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM settlements WHERE command_id=$1`, id).Scan(&settlementCount); err != nil {
		return fmt.Errorf("db query settlements: %w", err)
	}
	var leaseCount int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM leases WHERE command_id=$1`, id).Scan(&leaseCount); err != nil {
		return fmt.Errorf("db query leases: %w", err)
	}
	if status != "failed" || settlementCount != 1 || leaseCount != 3 || gen != 3 {
		return fail("db invariants violated: status=%s settlements=%d leases=%d gen=%d",
			status, settlementCount, leaseCount, gen)
	}
	fmt.Println("database invariants: 1 settlement row, 3 lease generations, status=failed")
	return nil
}

// spawnAPI 在验收容器内启动一个 API 进程（同一数据库，可注入额外环境变量），
// 返回指向该实例的客户端与停止函数。用于模拟重启与多实例（含时钟偏移）。
func (v *verifier) spawnAPI(ctx context.Context, port string, extraEnv ...string) (*verifier, func(), error) {
	bin := os.Getenv("API_BIN")
	if bin == "" {
		bin = "/usr/local/bin/api"
	}
	if _, err := os.Stat(bin); err != nil {
		return nil, nil, fail("API binary not found at %s (set API_BIN): %v", bin, err)
	}
	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(), "API_PORT="+port)
	cmd.Env = append(cmd.Env, extraEnv...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, nil, fmt.Errorf("start api on %s: %w", port, err)
	}
	stop := func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		_ = cmd.Wait()
	}
	inst := &verifier{
		baseURL: "http://127.0.0.1:" + port,
		dbURL:   v.dbURL,
		client:  &http.Client{Timeout: 10 * time.Second},
	}
	if err := inst.waitForAPI(ctx); err != nil {
		stop()
		return nil, nil, fmt.Errorf("api on %s unhealthy: %w", port, err)
	}
	return inst, stop, nil
}

// restartAPIAndRecheck 在验证容器内重启 API 进程（重新执行迁移+连接同一数据库），
// 模拟服务重启后继续裁决。需要 API_BIN 指向同构二进制（Dockerfile 已内置）。
func (v *verifier) restartAPIAndRecheck(ctx context.Context, cmdPath string) error {
	port := os.Getenv("RESTART_API_PORT")
	if port == "" {
		port = "18080"
	}
	restarted, stop, err := v.spawnAPI(ctx, port)
	if err != nil {
		return err
	}
	defer stop()

	st, raw := restarted.doJSON(ctx, http.MethodGet, cmdPath, nil)
	if st != http.StatusOK {
		return fail("after restart GET: status=%d body=%s", st, raw)
	}
	after := decode[commandOut](raw)
	if after.Status != "failed" || after.LeaseGeneration != 3 {
		return fail("after restart state changed: status=%s gen=%d", after.Status, after.LeaseGeneration)
	}
	if after.Settlement["result"] != "failed" ||
		int(after.Settlement["generation"].(float64)) != 3 {
		return fail("after restart settlement mismatch: %v", after.Settlement)
	}

	// 重启后仍要正确拒绝重复结算与覆盖。
	for _, tok := range []string{"garbage", "00"} {
		st, raw = restarted.doJSON(ctx, http.MethodPost, cmdPath+"/ack",
			map[string]any{"lease_token": tok, "result": "delivered"})
		if st != http.StatusConflict {
			return fail("after restart stale ack (%s): expected 409, got %d body=%s", tok, st, raw)
		}
	}
	st, _ = restarted.doJSON(ctx, http.MethodPost, "/claims",
		map[string]any{"lease_duration_ms": 100})
	if st != http.StatusNoContent {
		return fail("after restart settled command still claimable: %d", st)
	}
	fmt.Printf("restart recheck on %s: unique terminal state failed/gen3 preserved\n",
		restarted.baseURL)
	return nil
}

// consistencyView 是 GET 响应中用于自洽性校验的字段子集。
type consistencyView struct {
	Status          string `json:"status"`
	LeaseGeneration int64  `json:"lease_generation"`
	Leases          []struct {
		Generation int64 `json:"generation"`
	} `json:"leases"`
	Settlement *struct {
		Generation int64  `json:"generation"`
		Result     string `json:"result"`
	} `json:"settlement"`
}

// assertConsistentView 校验单次 GET 响应内部自洽：
//   - 租约历史与 lease_generation 同源（第 i 条租约代次为 i+1，条数等于当前代次），
//     不允许“旧代次状态 + 新代次租约”的跨代视图；
//   - pending 不得带结算；delivered/failed 必须带与当前代次一致的结算
//     （不允许“待处理且已结算”及其反面）；
//   - blocked 不得有租约或结算（失败阻断不伴随虚假的可执行状态）。
func assertConsistentView(raw []byte) error {
	var cv consistencyView
	if err := json.Unmarshal(raw, &cv); err != nil {
		return fmt.Errorf("decode view: %w", err)
	}
	if int64(len(cv.Leases)) != cv.LeaseGeneration {
		return fail("cross-generation view: lease_generation=%d but %d lease rows: %s",
			cv.LeaseGeneration, len(cv.Leases), raw)
	}
	for i, l := range cv.Leases {
		if l.Generation != int64(i+1) {
			return fail("non-contiguous lease history: leases[%d].generation=%d: %s",
				i, l.Generation, raw)
		}
	}
	switch cv.Status {
	case "pending":
		if cv.Settlement != nil {
			return fail("pending command carries a settlement: %s", raw)
		}
	case "delivered", "failed":
		if cv.Settlement == nil {
			return fail("terminal %s without settlement: %s", cv.Status, raw)
		}
		if cv.Settlement.Generation != cv.LeaseGeneration || cv.Settlement.Result != cv.Status {
			return fail("settlement/status mismatch: %s", raw)
		}
	case "expired":
		// 到期结算是正式结算（result=expired），但不伪造任何租约回执：generation=null。
		// 历史租约行数仍须与 lease_generation 一致（到期前可能已被领取过）。
		if cv.Settlement == nil {
			return fail("expired without settlement: %s", raw)
		}
		if cv.Settlement.Result != "expired" || cv.Settlement.Generation != 0 {
			return fail("expired settlement must be {result:expired, generation:null}: %s", raw)
		}
	case "blocked":
		if cv.Settlement != nil || len(cv.Leases) != 0 {
			return fail("blocked command carries settlement/leases: %s", raw)
		}
	default:
		return fail("unknown status in view: %s", raw)
	}
	return nil
}

// checkReadConsistency 可控的领取/回执交错 + 高频并发查询：
// 阶段一让一条指令经历 10 代短租约更替后 delivered 结算；
// 阶段二在根 failed 传播期间查询整条前驱链。
// 任何一次 GET 都必须返回内部自洽的视图——不存在跨代租约或
// “待处理且已结算”的组合；传播结束后下游 blocked 状态与结算记录相符。
func (v *verifier) checkReadConsistency(ctx context.Context) error {
	// 阶段一：租约代次更替 + 成功回执交错。
	created := v.createCommand(ctx, map[string]any{"consistency": "churn"}, nil)
	churnPath := fmt.Sprintf("/commands/%d", created.ID)

	stop := make(chan struct{})
	violations := make(chan error, 1)
	var readers sync.WaitGroup
	reader := func(path string) {
		defer readers.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			st, raw, err := v.doJSONErr(ctx, http.MethodGet, path, nil)
			if err != nil {
				select {
				case violations <- fmt.Errorf("get %s: %w", path, err):
				default:
				}
				return
			}
			if st != http.StatusOK {
				select {
				case violations <- fail("get %s during churn: status=%d body=%s", path, st, raw):
				default:
				}
				return
			}
			if err := assertConsistentView(raw); err != nil {
				select {
				case violations <- err:
				default:
				}
				return
			}
			// 微睡眠降压：仍保持高频交错，但避免读侧把写侧的领取/回执
			// 饿死在连接池上（验收环境可能是低规格容器）。
			time.Sleep(2 * time.Millisecond)
		}
	}
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go reader(churnPath)
	}

	for i := 0; i < 10; i++ {
		st, raw := v.doJSON(ctx, http.MethodPost, "/claims",
			map[string]any{"lease_duration_ms": 100})
		if st != http.StatusOK {
			return fail("churn claim %d: status=%d body=%s", i, st, raw)
		}
		time.Sleep(150 * time.Millisecond)
	}
	// 最后一代用满 5000ms 租期：读侧压力下写路径可能排队，
	// 留足余量避免“租约在回执前真实到期”的测试自身时序问题。
	st, raw := v.doJSON(ctx, http.MethodPost, "/claims", map[string]any{"lease_duration_ms": 5000})
	if st != http.StatusOK {
		return fail("final churn claim: status=%d body=%s", st, raw)
	}
	finalClaim := decode[claimOut](raw)
	st, raw = v.doJSON(ctx, http.MethodPost, churnPath+"/ack",
		map[string]any{"lease_token": finalClaim.LeaseToken, "result": "delivered"})
	if st != http.StatusOK {
		return fail("final churn ack: status=%d body=%s", st, raw)
	}

	// 阶段二：失败回执的阻断传播交错。
	root := v.createCommand(ctx, map[string]any{"consistency": "chain-root"}, nil)
	child := v.createCommand(ctx, map[string]any{"n": 1}, &root.ID)
	grand := v.createCommand(ctx, map[string]any{"n": 2}, &child.ID)
	chainPaths := []string{
		fmt.Sprintf("/commands/%d", root.ID),
		fmt.Sprintf("/commands/%d", child.ID),
		fmt.Sprintf("/commands/%d", grand.ID),
	}
	for _, p := range chainPaths {
		for i := 0; i < 2; i++ {
			readers.Add(1)
			go reader(p)
		}
	}
	st, raw = v.doJSON(ctx, http.MethodPost, "/claims", map[string]any{"lease_duration_ms": 5000})
	if st != http.StatusOK {
		return fail("chain root claim: status=%d body=%s", st, raw)
	}
	rootClaim := decode[claimOut](raw)
	if rootClaim.CommandID != root.ID {
		return fail("chain root claim got %d want %d", rootClaim.CommandID, root.ID)
	}
	st, raw = v.doJSON(ctx, http.MethodPost, chainPaths[0]+"/ack",
		map[string]any{"lease_token": rootClaim.LeaseToken, "result": "failed"})
	if st != http.StatusOK {
		return fail("fail chain root: status=%d body=%s", st, raw)
	}

	close(stop)
	readers.Wait()
	select {
	case err := <-violations:
		return err
	default:
	}

	// 阶段一终态：10 代更替 + 最后一代 = 11 代，delivered 结算与当前代次一致。
	got := v.getCommand(ctx, created.ID)
	if got.Status != "delivered" || got.LeaseGeneration != 11 || len(got.Leases) != 11 {
		return fail("churn final state: status=%s gen=%d leases=%d",
			got.Status, got.LeaseGeneration, len(got.Leases))
	}
	if got.Settlement == nil || got.Settlement["result"] != "delivered" ||
		int(got.Settlement["generation"].(float64)) != 11 {
		return fail("churn settlement inconsistent: %v", got.Settlement)
	}

	// 阶段二终态：失败后下游状态与结算记录相符。
	gotRoot := v.getCommand(ctx, root.ID)
	if gotRoot.Status != "failed" || gotRoot.Settlement == nil ||
		gotRoot.Settlement["result"] != "failed" ||
		int(gotRoot.Settlement["generation"].(float64)) != int(gotRoot.LeaseGeneration) {
		return fail("root after propagation inconsistent: %+v", gotRoot)
	}
	for _, id := range []int64{child.ID, grand.ID} {
		d := v.getCommand(ctx, id)
		if d.Status != "blocked" || d.BlockedBy == nil || *d.BlockedBy != root.ID {
			return fail("node %d: status=%s blocked_by=%v want blocked by %d",
				id, d.Status, d.BlockedBy, root.ID)
		}
		if len(d.Leases) != 0 || d.Settlement != nil {
			return fail("blocked node %d must have no leases/settlement: leases=%d settlement=%v",
				id, len(d.Leases), d.Settlement)
		}
	}
	if st, _ := v.doJSON(ctx, http.MethodPost, "/claims",
		map[string]any{"lease_duration_ms": 100}); st != http.StatusNoContent {
		return fail("after failure propagation nothing should be claimable, got %d", st)
	}
	fmt.Println("read consistency checks passed (no cross-generation or pending+settled views)")
	return nil
}

// checkClockSkewAcrossInstances 启动两台 API 实例，实例本地时钟分别落后/超前 1 小时
// （DEEPSPACE_CLOCK_OFFSET_MS 注入；裁决只依赖数据库时钟）：
//   - 尚在有效期的凭证，在时钟超前的实例上也必须被接受；
//   - 已过期的凭证，在时钟落后的实例（以及超前实例）上都必须被拒绝。
func (v *verifier) checkClockSkewAcrossInstances(ctx context.Context) error {
	lag, stopLag, err := v.spawnAPI(ctx, "18101", "DEEPSPACE_CLOCK_OFFSET_MS=-3600000")
	if err != nil {
		return err
	}
	defer stopLag()
	lead, stopLead, err := v.spawnAPI(ctx, "18102", "DEEPSPACE_CLOCK_OFFSET_MS=3600000")
	if err != nil {
		return err
	}
	defer stopLead()

	// 尚在有效期的凭证：立刻在时钟超前 1 小时的实例上确认，必须被接受。
	c1 := lag.createCommand(ctx, map[string]any{"skew": "valid"}, nil)
	st, raw := lag.doJSON(ctx, http.MethodPost, "/claims",
		map[string]any{"lease_duration_ms": 5000})
	if st != http.StatusOK {
		return fail("skew valid claim: status=%d body=%s", st, raw)
	}
	validToken := decode[claimOut](raw).LeaseToken
	st, raw = lead.doJSON(ctx, http.MethodPost, fmt.Sprintf("/commands/%d/ack", c1.ID),
		map[string]any{"lease_token": validToken, "result": "delivered"})
	if st != http.StatusOK {
		return fail("leading-clock instance rejected a valid lease: status=%d body=%s", st, raw)
	}

	// 已过期的凭证：等到真实到期后，在时钟落后 1 小时的实例上确认，必须被拒绝。
	c2 := lead.createCommand(ctx, map[string]any{"skew": "expired"}, nil)
	st, raw = lead.doJSON(ctx, http.MethodPost, "/claims",
		map[string]any{"lease_duration_ms": 100})
	if st != http.StatusOK {
		return fail("skew expired claim: status=%d body=%s", st, raw)
	}
	expiredToken := decode[claimOut](raw).LeaseToken
	time.Sleep(300 * time.Millisecond)
	st, raw = lag.doJSON(ctx, http.MethodPost, fmt.Sprintf("/commands/%d/ack", c2.ID),
		map[string]any{"lease_token": expiredToken, "result": "delivered"})
	if st != http.StatusConflict || decode[apiError](raw).Error.Code != "lease_expired" {
		return fail("lagging-clock instance accepted an expired lease: status=%d body=%s", st, raw)
	}
	// 同一张过期凭证在超前实例上也必须被拒绝：两台实例结论一致。
	st, raw = lead.doJSON(ctx, http.MethodPost, fmt.Sprintf("/commands/%d/ack", c2.ID),
		map[string]any{"lease_token": expiredToken, "result": "delivered"})
	if st != http.StatusConflict || decode[apiError](raw).Error.Code != "lease_expired" {
		return fail("leading-clock instance accepted an expired lease: status=%d body=%s", st, raw)
	}

	// 收尾：c2 仍 pending（过期令牌被拒），领取并送达，避免遗留可领取指令干扰后续场景。
	st, raw = lag.doJSON(ctx, http.MethodPost, "/claims",
		map[string]any{"lease_duration_ms": 5000})
	if st != http.StatusOK {
		return fail("cleanup claim: status=%d body=%s", st, raw)
	}
	cleanup := decode[claimOut](raw)
	if cleanup.CommandID != c2.ID {
		return fail("cleanup claim got command %d, want %d", cleanup.CommandID, c2.ID)
	}
	st, raw = lag.doJSON(ctx, http.MethodPost, fmt.Sprintf("/commands/%d/ack", c2.ID),
		map[string]any{"lease_token": cleanup.LeaseToken, "result": "delivered"})
	if st != http.StatusOK {
		return fail("cleanup ack: status=%d body=%s", st, raw)
	}
	fmt.Println("clock skew checks passed (±1h instance clocks, identical lease verdicts)")
	return nil
}

// checkDeadlines 验收可选 UTC 执行截止时刻与到期结算的完整语义：
//   - 未填写/显式 null 沿用旧行为；非法时间类型 422 invalid_deadline；
//   - 提前到期结算 409 deadline_not_reached，状态不变；
//   - 领取跳过过窗指令（无论是否持有未到期租约），无截止指令的领取顺序仍是 ID 升序；
//   - 截止后持未到期租约的回执 409 deadline_reached；截止前送达不可被到期结算追改；
//   - 到点到期结算原子置 expired、无回执结算（generation=null）、尚未送达后继
//     原子 blocked（根因=到期根）；重复到期不制造第二次结算；
//   - 重启（新进程同库）后 expired/blocked 保持；带外核对结算恰一行；
//   - 领取与到期结算的并发互斥（可选，行锁裁决只允许一个终态）。
func (v *verifier) checkDeadlines(ctx context.Context) error {
	post := func(id int64, action string) (int, []byte) {
		return v.doJSON(ctx, http.MethodPost, fmt.Sprintf("/commands/%d/%s", id, action), nil)
	}

	// 1) 输入契约：非法 deadline 一律 422 invalid_deadline；null 是旧行为。
	for _, body := range []string{
		`{"payload":{},"deadline_at":1758240000}`,
		`{"payload":{},"deadline_at":true}`,
		`{"payload":{},"deadline_at":"not-a-time"}`,
		`{"payload":{},"deadline_at":"2026-09-20 00:00:00"}`,
	} {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, v.baseURL+"/commands",
			bytes.NewReader([]byte(body)))
		req.Header.Set("Content-Type", "application/json")
		resp, err := v.client.Do(req)
		if err != nil {
			return fmt.Errorf("bad deadline request: %w", err)
		}
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnprocessableEntity ||
			decode[apiError](raw).Error.Code != "invalid_deadline" {
			return fail("invalid deadline %s: status=%d body=%s", body, resp.StatusCode, raw)
		}
	}
	st, raw := v.doJSON(ctx, http.MethodPost, "/commands",
		map[string]any{"payload": map[string]any{"deadline": "null"}, "deadline_at": nil})
	if st != http.StatusCreated {
		return fail("null deadline: status=%d body=%s", st, raw)
	}
	nullCmd := decode[commandOut](raw)
	if nullCmd.Deadline != nil {
		return fail("null deadline must not echo deadline: %s", raw)
	}

	// 2) 未填写截止时刻的旧指令：提前到期结算被拒。
	plain := v.createCommand(ctx, map[string]any{"deadline": "none"}, nil)
	if st, raw = post(plain.ID, "expire"); st != http.StatusConflict ||
		decode[apiError](raw).Error.Code != "deadline_not_reached" {
		return fail("expire no-deadline: status=%d body=%s", st, raw)
	}
	if got := v.getCommand(ctx, plain.ID); got.Status != "pending" {
		return fail("premature expire changed state: %s", got.Status)
	}
	if st, _ = post(987654321, "expire"); st != http.StatusNotFound {
		return fail("expire unknown id: status=%d want 404", st)
	}

	// 收掉这两条无截止的旧指令（按 ID 升序领取送达），避免干扰后续“跳过过窗”断言。
	for _, id := range []int64{nullCmd.ID, plain.ID} {
		st, raw = v.doJSON(ctx, http.MethodPost, "/claims", map[string]any{"lease_duration_ms": 5000})
		if st != http.StatusOK || decode[claimOut](raw).CommandID != id {
			return fail("cleanup claim %d: status=%d body=%s", id, st, raw)
		}
		tok := decode[claimOut](raw).LeaseToken
		if st, raw = v.doJSON(ctx, http.MethodPost, fmt.Sprintf("/commands/%d/ack", id),
			map[string]any{"lease_token": tok, "result": "delivered"}); st != http.StatusOK {
			return fail("cleanup ack %d: status=%d body=%s", id, st, raw)
		}
	}

	// 3) 过窗但未被领取的指令被领取跳过：编号更大的无截止指令照常领取，顺序不变。
	past0 := v.createCommandWithDeadline(ctx, map[string]any{"d": "skip"},
		time.Now().Add(300*time.Millisecond))
	plainOK := v.createCommand(ctx, map[string]any{"d": "ok"}, nil)
	time.Sleep(500 * time.Millisecond)
	st, raw = v.doJSON(ctx, http.MethodPost, "/claims", map[string]any{"lease_duration_ms": 5000})
	if st != http.StatusOK {
		return fail("claim when oldest is past deadline: status=%d body=%s", st, raw)
	}
	skipClaim := decode[claimOut](raw)
	if skipClaim.CommandID != plainOK.ID {
		return fail("claim must skip unleased past-deadline %d and take %d, got %d",
			past0.ID, plainOK.ID, skipClaim.CommandID)
	}
	if st, raw = v.doJSON(ctx, http.MethodPost, fmt.Sprintf("/commands/%d/ack", plainOK.ID),
		map[string]any{"lease_token": skipClaim.LeaseToken, "result": "delivered"}); st != http.StatusOK {
		return fail("deliver plainOK: status=%d body=%s", st, raw)
	}
	// 被跳过的过窗指令由到期结算接管。
	if st, raw = post(past0.ID, "expire"); st != http.StatusOK ||
		decode[commandOut](raw).Status != "expired" {
		return fail("expire skipped past0: status=%d body=%s", st, raw)
	}

	// 4) 截止后：旧租约即使未达自身租期也不能送达；到期结算接管。
	root := v.createCommandWithDeadline(ctx, map[string]any{"d": "root"},
		time.Now().Add(300*time.Millisecond))
	child := v.createCommand(ctx, map[string]any{"d": "child"}, &root.ID)
	grand := v.createCommand(ctx, map[string]any{"d": "grand"}, &child.ID)

	st, raw = v.doJSON(ctx, http.MethodPost, "/claims", map[string]any{"lease_duration_ms": 5000})
	if st != http.StatusOK {
		return fail("claim root: status=%d body=%s", st, raw)
	}
	rootClaim := decode[claimOut](raw)
	if rootClaim.CommandID != root.ID {
		return fail("claim got %d want root %d", rootClaim.CommandID, root.ID)
	}
	time.Sleep(500 * time.Millisecond)
	// 5s 租约在截止后仍“未到期”，但回执必须被拒（409 deadline_reached）。
	st, raw = v.doJSON(ctx, http.MethodPost, fmt.Sprintf("/commands/%d/ack", root.ID),
		map[string]any{"lease_token": rootClaim.LeaseToken, "result": "delivered"})
	if st != http.StatusConflict || decode[apiError](raw).Error.Code != "deadline_reached" {
		return fail("post-deadline ack: status=%d body=%s", st, raw)
	}
	if got := v.getCommand(ctx, root.ID); got.Status != "pending" || got.Settlement != nil {
		return fail("rejected post-deadline ack changed state: %+v", got)
	}
	// 过窗指令也不再可领取（其租约本身未到期，跳过完全来自截止谓词）。
	if st, _ = v.doJSON(ctx, http.MethodPost, "/claims",
		map[string]any{"lease_duration_ms": 100}); st != http.StatusNoContent {
		return fail("past-deadline command must not be claimable, got %d", st)
	}

	// 5) 到期结算：expired + 无回执结算，后继原子 blocked（根因=到期根）。
	st, raw = post(root.ID, "expire")
	if st != http.StatusOK {
		return fail("expire root: status=%d body=%s", st, raw)
	}
	settled := decode[commandOut](raw)
	if settled.Status != "expired" {
		return fail("expire root body status=%s", settled.Status)
	}
	if settled.Settlement == nil || settled.Settlement["result"] != "expired" ||
		settled.Settlement["generation"] != nil {
		return fail("expired settlement must be {result:expired, generation:null}: %v",
			settled.Settlement)
	}
	if len(settled.Leases) != 1 || settled.LeaseGeneration != 1 {
		return fail("expired keeps pre-deadline lease history: gen=%d leases=%d",
			settled.LeaseGeneration, len(settled.Leases))
	}
	for _, id := range []int64{child.ID, grand.ID} {
		got := v.getCommand(ctx, id)
		if got.Status != "blocked" || got.BlockedBy == nil || *got.BlockedBy != root.ID {
			return fail("node %d: status=%s blocked_by=%v want blocked by expired root %d",
				id, got.Status, got.BlockedBy, root.ID)
		}
		if len(got.Leases) != 0 || got.Settlement != nil {
			return fail("blocked node %d fabricated leases=%d settlement=%v",
				id, len(got.Leases), got.Settlement)
		}
	}
	// 到期之后新建后继：出生即 blocked，根因相同。
	late := v.createCommand(ctx, map[string]any{"late": true}, &root.ID)
	if late.Status != "blocked" || late.BlockedBy == nil || *late.BlockedBy != root.ID {
		return fail("late successor must be born blocked by expired root: %+v", late)
	}

	// 6) 重复到期 / 回执覆盖：都不得制造第二次结算。
	if st, raw = post(root.ID, "expire"); st != http.StatusConflict ||
		decode[apiError](raw).Error.Code != "already_settled" {
		return fail("duplicate expire: status=%d body=%s", st, raw)
	}
	st, raw = v.doJSON(ctx, http.MethodPost, fmt.Sprintf("/commands/%d/ack", root.ID),
		map[string]any{"lease_token": rootClaim.LeaseToken, "result": "failed"})
	if st != http.StatusConflict || decode[apiError](raw).Error.Code != "already_settled" {
		return fail("ack expired root: status=%d body=%s", st, raw)
	}
	if err := v.assertExpiredDatabase(ctx, root.ID, 1); err != nil {
		return err
	}
	if st, _ = v.doJSON(ctx, http.MethodPost, "/claims",
		map[string]any{"lease_duration_ms": 100}); st != http.StatusNoContent {
		return fail("expired chain claimable after settlement: %d", st)
	}

	// 7) 截止前送达不可被追改。
	fast := v.createCommandWithDeadline(ctx, map[string]any{"d": "fast"},
		time.Now().Add(600*time.Millisecond))
	st, raw = v.doJSON(ctx, http.MethodPost, "/claims", map[string]any{"lease_duration_ms": 5000})
	if st != http.StatusOK {
		return fail("claim fast: status=%d body=%s", st, raw)
	}
	fastClaim := decode[claimOut](raw)
	if st, raw = v.doJSON(ctx, http.MethodPost, fmt.Sprintf("/commands/%d/ack", fast.ID),
		map[string]any{"lease_token": fastClaim.LeaseToken, "result": "delivered"}); st != http.StatusOK {
		return fail("pre-deadline ack: status=%d body=%s", st, raw)
	}
	time.Sleep(750 * time.Millisecond)
	if st, raw = post(fast.ID, "expire"); st != http.StatusConflict ||
		decode[apiError](raw).Error.Code != "already_settled" {
		return fail("expire must not overturn pre-deadline delivery: status=%d body=%s", st, raw)
	}
	if got := v.getCommand(ctx, fast.ID); got.Status != "delivered" {
		return fail("pre-deadline delivery overturned: %s", got.Status)
	}

	// 8) 重启后终态保持，重复到期仍 409，不可领取。
	restarted, stop, err := v.spawnAPI(ctx, "18081")
	if err != nil {
		return err
	}
	defer stop()
	sr, rr := restarted.doJSON(ctx, http.MethodGet, fmt.Sprintf("/commands/%d", root.ID), nil)
	if sr != http.StatusOK || decode[commandOut](rr).Status != "expired" {
		return fail("after restart root: status=%d body=%s", sr, rr)
	}
	if sr, rr = restarted.doJSON(ctx, http.MethodPost,
		fmt.Sprintf("/commands/%d/expire", root.ID), nil); sr != http.StatusConflict {
		return fail("after restart re-expire: status=%d body=%s", sr, rr)
	}
	if sr, _ = restarted.doJSON(ctx, http.MethodPost, "/claims",
		map[string]any{"lease_duration_ms": 100}); sr != http.StatusNoContent {
		return fail("after restart expired chain claimable: %d", sr)
	}
	for _, id := range []int64{child.ID, grand.ID, late.ID} {
		sr, rr = restarted.doJSON(ctx, http.MethodGet, fmt.Sprintf("/commands/%d", id), nil)
		got := decode[commandOut](rr)
		if sr != http.StatusOK || got.Status != "blocked" ||
			got.BlockedBy == nil || *got.BlockedBy != root.ID {
			return fail("after restart node %d not blocked by %d: %s", id, root.ID, rr)
		}
	}
	fmt.Println("deadline checks passed (expiry settlement, ack/expire arbitration, successor blocking, restart)")
	return nil
}

// checkDeadlineLockQueue 验收“锁等待跨窗裁决”：回执/到期结算恰好跨过时间边界——
// 请求入队（事务开始）时尚未过窗，但在指令行锁队列中等待期间数据库时钟跨过
// 执行截止点。裁决必须以拿到行锁之后的数据库时钟为准：
//   - 过窗回执 409 deadline_reached，状态保持 pending，随后到期结算接管为 expired；
//   - 过窗到期结算照常 200 expired（不得 409 deadline_not_reached）；
//
// 回执、到期结算与详情查询呈现同一次有效裁决，过窗指令的终态不因请求在锁
// 队列中的等待顺序而改变。
func (v *verifier) checkDeadlineLockQueue(ctx context.Context) error {
	pool, err := pgxpool.New(ctx, v.dbURL)
	if err != nil {
		return fmt.Errorf("db connect: %w", err)
	}
	defer pool.Close()

	// 等待对方事务进入行锁等待（证明其已在截止点之前入队）。
	waitQueued := func() error {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			var waiting int
			if err := pool.QueryRow(ctx, `
				SELECT count(*) FROM pg_locks
				WHERE NOT granted AND locktype IN ('tuple','transactionid')`).Scan(&waiting); err == nil && waiting > 0 {
				return nil
			}
			time.Sleep(5 * time.Millisecond)
		}
		return fail("queued request never reached the lock-wait state")
	}
	// 旁观者事务持锁，制造一次短暂的指令锁等待。
	holdRowLock := func(id int64) (func(), error) {
		holder, err := pool.Begin(ctx)
		if err != nil {
			return nil, err
		}
		if _, err := holder.Exec(ctx, `SELECT 1 FROM commands WHERE id=$1 FOR UPDATE`, id); err != nil {
			holder.Rollback(ctx)
			return nil, err
		}
		return func() { holder.Commit(ctx) }, nil
	}
	type httpResult struct {
		status int
		raw    []byte
	}

	// a) 回执在锁等待期间跨过截止点：必须 409 deadline_reached，状态不变。
	queued := v.createCommandWithDeadline(ctx, map[string]any{"d": "queued-ack"},
		time.Now().Add(400*time.Millisecond))
	st, raw := v.doJSON(ctx, http.MethodPost, "/claims", map[string]any{"lease_duration_ms": 5000})
	if st != http.StatusOK {
		return fail("claim queued-ack: status=%d body=%s", st, raw)
	}
	queuedClaim := decode[claimOut](raw)
	if queuedClaim.CommandID != queued.ID {
		return fail("claim got %d want queued-ack %d", queuedClaim.CommandID, queued.ID)
	}

	release, err := holdRowLock(queued.ID)
	if err != nil {
		return fmt.Errorf("holder lock: %w", err)
	}
	ackCh := make(chan httpResult, 1)
	go func() {
		st, raw := v.doJSON(ctx, http.MethodPost, fmt.Sprintf("/commands/%d/ack", queued.ID),
			map[string]any{"lease_token": queuedClaim.LeaseToken, "result": "delivered"})
		ackCh <- httpResult{st, raw}
	}()
	if err := waitQueued(); err != nil {
		return err
	}
	time.Sleep(600 * time.Millisecond) // 数据库时钟明确跨过截止点（400ms）
	release()
	ackRes := <-ackCh
	if ackRes.status != http.StatusConflict ||
		decode[apiError](ackRes.raw).Error.Code != "deadline_reached" {
		return fail("ack queued across deadline: status=%d body=%s, want 409 deadline_reached",
			ackRes.status, ackRes.raw)
	}
	if got := v.getCommand(ctx, queued.ID); got.Status != "pending" || got.Settlement != nil {
		return fail("queued ack past deadline changed state: %+v", got)
	}
	// 同一交接下，到期结算随后接管为 expired。
	st, raw = v.doJSON(ctx, http.MethodPost, fmt.Sprintf("/commands/%d/expire", queued.ID), nil)
	if st != http.StatusOK || decode[commandOut](raw).Status != "expired" {
		return fail("expire after queued ack: status=%d body=%s", st, raw)
	}

	// b) 到期结算在锁等待期间跨过截止点：必须照常 200 expired，
	//    不得把已经过窗的指令误判为“尚未到期”。
	queuedExp := v.createCommandWithDeadline(ctx, map[string]any{"d": "queued-expire"},
		time.Now().Add(400*time.Millisecond))
	release2, err := holdRowLock(queuedExp.ID)
	if err != nil {
		return fmt.Errorf("holder2 lock: %w", err)
	}
	expCh := make(chan httpResult, 1)
	go func() {
		st, raw := v.doJSON(ctx, http.MethodPost, fmt.Sprintf("/commands/%d/expire", queuedExp.ID), nil)
		expCh <- httpResult{st, raw}
	}()
	if err := waitQueued(); err != nil {
		return err
	}
	time.Sleep(600 * time.Millisecond)
	release2()
	expRes := <-expCh
	if expRes.status != http.StatusOK || decode[commandOut](expRes.raw).Status != "expired" {
		return fail("expire queued across deadline must settle expired: status=%d body=%s",
			expRes.status, expRes.raw)
	}
	fmt.Println("deadline lock-queue checks passed (verdicts use the post-lock database clock)")
	return nil
}

// assertExpiredDatabase 带外核对：到期指令在 settlements 恰一行（result=expired、
// generation/lease_token 均为 NULL），状态唯一为 expired。
func (v *verifier) assertExpiredDatabase(ctx context.Context, id int64, leaseGen int64) error {
	pool, err := pgxpool.New(ctx, v.dbURL)
	if err != nil {
		return fmt.Errorf("db connect: %w", err)
	}
	defer pool.Close()

	var status string
	var gen int64
	if err := pool.QueryRow(ctx,
		`SELECT status, lease_generation FROM commands WHERE id=$1`, id).Scan(&status, &gen); err != nil {
		return fmt.Errorf("db query command: %w", err)
	}
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM settlements
		 WHERE command_id=$1 AND result='expired'
		   AND generation IS NULL AND lease_token IS NULL`, id).Scan(&n); err != nil {
		return fmt.Errorf("db query settlements: %w", err)
	}
	var total int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM settlements WHERE command_id=$1`, id).Scan(&total); err != nil {
		return fmt.Errorf("db query all settlements: %w", err)
	}
	if status != "expired" || n != 1 || total != 1 || gen != leaseGen {
		return fail("expired db invariants violated: status=%s expired_rows=%d total=%d gen=%d want gen=%d",
			status, n, total, gen, leaseGen)
	}
	return nil
}
