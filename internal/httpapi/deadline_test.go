package httpapi_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"deepspace/internal/testdb"
)

// rawRequest 发送原始 JSON 字符串，返回状态码与原始体。
func rawRequest(t *testing.T, tsURL, method, path, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, tsURL+path, bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

// TestCreateDeadlineValidation deadline 字段的输入契约：
// 缺省/null 是旧行为；非字符串/非 RFC3339 一律 422 invalid_deadline；
// 合法 RFC3339（含非 UTC 偏移）被接受并按 UTC 回显。
func TestCreateDeadlineValidation(t *testing.T) {
	ts := newTestServer(t)

	// 缺省：旧行为，不回显 deadline。
	status, m := doJSON(t, http.MethodPost, ts.URL+"/commands", map[string]any{"payload": map[string]any{"x": 1}})
	if status != 201 {
		t.Fatalf("legacy create status=%d %v", status, m)
	}
	if _, present := m["deadline_at"]; present {
		t.Fatalf("command without deadline must not expose field, got %v", m["deadline_at"])
	}

	// 显式 null：同样是旧行为。
	status, m = doJSON(t, http.MethodPost, ts.URL+"/commands",
		map[string]any{"payload": map[string]any{"x": 2}, "deadline_at": nil})
	if status != 201 {
		t.Fatalf("null deadline status=%d %v", status, m)
	}
	if _, present := m["deadline_at"]; present {
		t.Fatalf("null deadline must not expose field, got %v", m["deadline_at"])
	}

	bad := []string{
		`{"payload":{},"deadline_at":1758240000}`,
		`{"payload":{},"deadline_at":true}`,
		`{"payload":{},"deadline_at":["2026-09-20T00:00:00Z"]}`,
		`{"payload":{},"deadline_at":"not-a-time"}`,
		`{"payload":{},"deadline_at":"2026-09-20 00:00:00"}`,
	}
	for _, body := range bad {
		st, raw := rawRequest(t, ts.URL, http.MethodPost, "/commands", body)
		if st != 422 {
			t.Fatalf("body %s: status=%d want 422 raw=%s", body, st, raw)
		}
		if got := decodeErrCode(raw); got != "invalid_deadline" {
			t.Fatalf("body %s: code=%s want invalid_deadline raw=%s", body, got, raw)
		}
	}

	// 合法 UTC：回显原样。
	utc := "2030-01-02T03:04:05Z"
	status, m = doJSON(t, http.MethodPost, ts.URL+"/commands",
		map[string]any{"payload": map[string]any{"x": 3}, "deadline_at": utc})
	if status != 201 {
		t.Fatalf("valid deadline status=%d %v", status, m)
	}
	if m["deadline_at"] != utc {
		t.Fatalf("deadline echo=%v want %s", m["deadline_at"], utc)
	}

	// 合法带偏移：服务端归一化为 UTC（+02:00 -> Z 减 2 小时）。
	offset := "2030-01-02T05:04:05+02:00"
	status, m = doJSON(t, http.MethodPost, ts.URL+"/commands",
		map[string]any{"payload": map[string]any{"x": 4}, "deadline_at": offset})
	if status != 201 {
		t.Fatalf("offset deadline status=%d %v", status, m)
	}
	if m["deadline_at"] != "2030-01-02T03:04:05Z" {
		t.Fatalf("deadline not normalized to UTC: %v", m["deadline_at"])
	}
}

// TestExpireHTTPLifecycle 通过 HTTP 覆盖到期结算完整契约：
// 提前调用 409 deadline_not_reached；到点后 200 expired（无回执结算、后继 blocked）；
// 重复到期 409 already_settled；终态不可领取。
func TestExpireHTTPLifecycle(t *testing.T) {
	ts := newTestServer(t)

	// 无截止时刻 -> deadline_not_reached。
	status, m := doJSON(t, http.MethodPost, ts.URL+"/commands", map[string]any{"payload": map[string]any{"x": 1}})
	if status != 201 {
		t.Fatalf("create: %d %v", status, m)
	}
	id := int64(m["id"].(float64))
	status, m = doJSON(t, http.MethodPost, ts.URL+"/commands/"+strconv.FormatInt(id, 10)+"/expire", nil)
	if status != 409 || m["error"].(map[string]any)["code"] != "deadline_not_reached" {
		t.Fatalf("early expire: status=%d body=%v", status, m)
	}
	status, m = doJSON(t, http.MethodGet, ts.URL+"/commands/"+strconv.FormatInt(id, 10), nil)
	if m["status"] != "pending" {
		t.Fatalf("state changed by early expire: %v", m["status"])
	}

	// 收掉这条无截止的旧指令（领取并送达），避免它作为可领取指令干扰后续 204 断言。
	status, m = doJSON(t, http.MethodPost, ts.URL+"/claims", map[string]any{"lease_duration_ms": 5000})
	if status != 200 {
		t.Fatalf("cleanup claim: %d %v", status, m)
	}
	if got := int64(m["command_id"].(float64)); got != id {
		t.Fatalf("cleanup claimed %d want %d", got, id)
	}
	cleanupToken := m["lease_token"].(string)
	if st, _ := doJSON(t, http.MethodPost, ts.URL+"/commands/"+strconv.FormatInt(id, 10)+"/ack",
		map[string]any{"lease_token": cleanupToken, "result": "delivered"}); st != 200 {
		t.Fatalf("cleanup ack: status=%d", st)
	}

	// 未知编号 -> 404 unknown_command。
	status, m = doJSON(t, http.MethodPost, ts.URL+"/commands/99999999/expire", nil)
	if status != 404 || m["error"].(map[string]any)["code"] != "unknown_command" {
		t.Fatalf("unknown expire: status=%d body=%v", status, m)
	}

	// 到点的到期结算。
	deadline := time.Now().Add(300 * time.Millisecond).UTC()
	status, m = doJSON(t, http.MethodPost, ts.URL+"/commands",
		map[string]any{"payload": map[string]any{"x": 2}, "deadline_at": deadline.Format(time.RFC3339Nano)})
	if status != 201 {
		t.Fatalf("create with deadline: %d %v", status, m)
	}
	rootID := int64(m["id"].(float64))

	status, m = doJSON(t, http.MethodPost, ts.URL+"/commands",
		map[string]any{"payload": map[string]any{"x": 3}, "predecessor_id": rootID})
	if status != 201 {
		t.Fatalf("create successor: %d %v", status, m)
	}
	succID := int64(m["id"].(float64))

	time.Sleep(500 * time.Millisecond)
	status, m = doJSON(t, http.MethodPost, ts.URL+"/commands/"+strconv.FormatInt(rootID, 10)+"/expire", nil)
	if status != 200 {
		t.Fatalf("expire at deadline: status=%d body=%v", status, m)
	}
	if m["status"] != "expired" {
		t.Fatalf("expire body status=%v", m["status"])
	}
	settlement, ok := m["settlement"].(map[string]any)
	if !ok || settlement["result"] != "expired" || settlement["generation"] != nil {
		t.Fatalf("expired settlement must be {result:expired, generation:null}: %v", m["settlement"])
	}
	if len(m["leases"].([]any)) != 0 {
		t.Fatalf("expired without any claim must keep leases empty: %v", m["leases"])
	}

	// 后继按前驱闭包规则 blocked，根因=到期根。
	status, m = doJSON(t, http.MethodGet, ts.URL+"/commands/"+strconv.FormatInt(succID, 10), nil)
	if status != 200 || m["status"] != "blocked" {
		t.Fatalf("successor status=%d body=%v", status, m)
	}
	if b := m["blocked_by"].(float64); int64(b) != rootID {
		t.Fatalf("blocked_by=%v want %d", m["blocked_by"], rootID)
	}

	// 重复到期结算 -> already_settled。
	status, m = doJSON(t, http.MethodPost, ts.URL+"/commands/"+strconv.FormatInt(rootID, 10)+"/expire", nil)
	if status != 409 || m["error"].(map[string]any)["code"] != "already_settled" {
		t.Fatalf("duplicate expire: status=%d body=%v", status, m)
	}

	// 终态不可领取。
	if st, _ := doJSON(t, http.MethodPost, ts.URL+"/claims", map[string]any{"lease_duration_ms": 100}); st != 204 {
		t.Fatalf("expired chain claimable: status=%d", st)
	}
}

// TestHTTPAckAfterDeadlineConflict 截止后持未到期租约的回执被拒：
// 409 deadline_reached，状态保持 pending；随后到期结算成功。
func TestHTTPAckAfterDeadlineConflict(t *testing.T) {
	ts := newTestServer(t)

	deadline := time.Now().Add(300 * time.Millisecond).UTC()
	status, m := doJSON(t, http.MethodPost, ts.URL+"/commands",
		map[string]any{"payload": map[string]any{"x": 1}, "deadline_at": deadline.Format(time.RFC3339Nano)})
	if status != 201 {
		t.Fatalf("create: %d %v", status, m)
	}
	id := int64(m["id"].(float64))

	status, m = doJSON(t, http.MethodPost, ts.URL+"/claims", map[string]any{"lease_duration_ms": 5000})
	if status != 200 {
		t.Fatalf("claim: %d %v", status, m)
	}
	token := m["lease_token"].(string)

	time.Sleep(500 * time.Millisecond)

	status, m = doJSON(t, http.MethodPost, ts.URL+"/commands/"+strconv.FormatInt(id, 10)+"/ack",
		map[string]any{"lease_token": token, "result": "delivered"})
	if status != 409 {
		t.Fatalf("post-deadline ack status=%d want 409 body=%v", status, m)
	}
	if m["error"].(map[string]any)["code"] != "deadline_reached" {
		t.Fatalf("post-deadline ack code=%v want deadline_reached", m["error"])
	}

	status, m = doJSON(t, http.MethodGet, ts.URL+"/commands/"+strconv.FormatInt(id, 10), nil)
	if m["status"] != "pending" || m["settlement"] != nil {
		t.Fatalf("rejected ack changed state: status=%v settlement=%v", m["status"], m["settlement"])
	}

	status, m = doJSON(t, http.MethodPost, ts.URL+"/commands/"+strconv.FormatInt(id, 10)+"/expire", nil)
	if status != 200 || m["status"] != "expired" {
		t.Fatalf("expire: status=%d body=%v", status, m)
	}
}

// TestHTTPExpirePersistsAcrossRestart 重启等价验证：直接开第二个连接池+重新迁移
// （同一物理库），expired 状态保持、重复到期仍 409。
func TestHTTPExpirePersistsAcrossRestart(t *testing.T) {
	pool := testdb.New(t)
	if err := migratePool(context.Background(), pool); err != nil {
		t.Fatal(err)
	}

	// 直连数据库构造一条“截止点已是过去”的指令：INSERT deadline_at=now()。
	var id int64
	if err := pool.QueryRow(context.Background(),
		`INSERT INTO commands (payload, status, deadline_at)
		 VALUES ($1::jsonb, 'pending', now()) RETURNING id`,
		`{"restart":1}`).Scan(&id); err != nil {
		t.Fatalf("insert past-deadline command: %v", err)
	}

	// 用新池模拟重启，执行到期结算。
	pool2, err := pgxpool.New(context.Background(), pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer pool2.Close()
	if err := migratePool(context.Background(), pool2); err != nil {
		t.Fatalf("re-migrate: %v", err)
	}

	ts := newTestServerFromPool(t, pool2)
	status, m := doJSON(t, http.MethodPost,
		ts.URL+"/commands/"+strconv.FormatInt(id, 10)+"/expire", nil)
	if status != 200 || m["status"] != "expired" {
		t.Fatalf("expire after restart-equivalent: status=%d body=%v", status, m)
	}
	pool2.Close()

	pool3, err := pgxpool.New(context.Background(), pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	defer pool3.Close()
	if err := migratePool(context.Background(), pool3); err != nil {
		t.Fatalf("re-migrate 2: %v", err)
	}
	ts2 := newTestServerFromPool(t, pool3)
	status, m = doJSON(t, http.MethodPost,
		ts2.URL+"/commands/"+strconv.FormatInt(id, 10)+"/expire", nil)
	if status != 409 || m["error"].(map[string]any)["code"] != "already_settled" {
		t.Fatalf("re-expire after restart: status=%d body=%v", status, m)
	}
	status, m = doJSON(t, http.MethodGet, ts2.URL+"/commands/"+strconv.FormatInt(id, 10), nil)
	if m["status"] != "expired" {
		t.Fatalf("expired state lost across restart: %v", m["status"])
	}
}
