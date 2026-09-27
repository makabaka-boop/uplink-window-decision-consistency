# deepspace — 深空测控指令裁决服务

纯后端 API（Go 1.23 + 标准库 `net/http` + PostgreSQL）。在中继并发领取上行指令、
失联者带着旧确认在租约过期后迟到的场景下，保证：

- **不重复结算**：一条指令至多一个终态；终态永不被覆盖。
- **前驱链有序放行**：后继只在整条前驱链确认送达后才可领取，绝不预发租约；前驱确认
  失败（或到期）时，尚未送达的后继链原子转为只读 `blocked`（记录阻断来源，无租约、
  无伪造结算）。
- **过窗指令不发放、不送达**：创建时可选填 UTC 执行截止时刻（`deadline_at`）。
  过窗（数据库时钟已到截止点）的指令不会再被领取；持旧租约的回执即使租约本身尚未
  到期也不能送达。针对指定指令的到期结算只在“已到截止点且尚未送达”时原子置
  `expired`（有正式结算行、无伪造回执），并按前驱闭包规则阻断尚未送达的后继
  （根因=到期根）；截止前的成功送达不可被追改，重复到期不会制造第二次结算。
- **不抢占新持有者结果**：旧代次令牌迟到时返回 `409`，状态不变。
- **查询视图自洽**：`GET /commands/{id}` 在同一个 `REPEATABLE READ` 只读事务快照中
  读取指令状态、租约历史与结算记录；查询恰好跨过并发领取/回执/到期时，也绝不出现
  “旧代次状态 + 新代次租约”或“待处理 + 已结算”的拼接视图。
- **裁决时钟唯一**：租约到期时间的写入、领取侧到期判定、回执侧有效性判定、
  截止点判定与到期结算全部使用**数据库时钟**（`now()`）。多实例部署时实例本地时钟
  有偏差也不会改变裁决结论——有效凭证不会在一台被判过期，已过期凭证（含过窗租约）
  不会在另一台被接受。
- **重启后继续裁决**：指令、前驱闭包、租约代次、终态全部持久化在 PostgreSQL。

## 运行

需要 Docker Compose（内置 PostgreSQL）。

```bash
# 宿主机发布端口由 API_PORT 控制（默认 8080）
API_PORT=8080 docker compose up --build
```

`up` 会依次启动 `db`、`api` 与 **`verify` 一次性验收服务**。`verify` 跑完即退出，
退出码为 0 表示验收通过。单独重跑：

```bash
docker compose run --rm verify
```

### 本地开发

```bash
# 需要一个可达的 PostgreSQL
export DATABASE_URL="postgres://user:pass@localhost:5432/deepspace?sslmode=disable"
go run ./cmd/api            # 端口取 API_PORT，默认 8080

# 集成测试（自动创建隔离的临时数据库）
export DEEPSPACE_TEST_ADMIN_URL="postgres://postgres@localhost:5432/postgres?sslmode=disable"
go test -race ./...
```

服务首次连接数据库时会自动执行幂等迁移（带咨询锁，多实例同时启动也安全）。

## API 契约

所有请求/响应均为 `application/json`。错误响应使用**稳定信封**，禁止客户端依赖
自然语言文案：

```json
{ "error": { "code": "lease_expired", "message": "lease token is expired or superseded by a newer generation" } }
```

### `POST /commands` — 创建指令

按创建顺序写入（自增 ID 即顺序）。`payload` 必填且不能为 `null`，可以是任意 JSON 值。

请求：`{"payload": {"seq": 1, "target": "mars-relay-7"}}`

可选 `predecessor_id`（正整数）声明**唯一前驱**，用于“等上一条确认送达后才允许中继
领取后续动作”：

- 只能引用**已存在的较早编号**；缺省（或显式 `null`）即为无前驱的旧请求，行为不变。
- 前驱链尚未全部 `delivered` 时，该指令不可领取，也**不会预发租约**；前驱逐节送达
  后才自动开放领取。
- 前驱（直接前驱或任一祖先）确认 `failed` 或到期 `expired` 时，尚未送达的整条后继链
  在同一事务内原子转为只读终态 `blocked` 并记录 `blocked_by`（阻断来源）；在失败/
  到期之后才创建的后继出生即为 `blocked`。`blocked` 不产生租约、不产生结算。

可选 `deadline_at`（RFC3339 时间字符串，建议带 `Z` 的 UTC 时刻）声明 **UTC 执行截止
时刻**：

- 缺省（或显式 `null`）时沿用现有行为：领取、回执、查询全部不变。
- 数据库时钟到达截止点（`deadline_at <= now()`）后：该指令不再可领取（领取扫描跳过，
  即使其租约本身尚未到期）；旧租约的回执一律 `409 deadline_reached`，即使该租约
  尚未达到自身租期。
- 截止点之前的成功送达永久成立，到期结算不能追改。

请求示例：`{"payload": {"seq": 2}, "predecessor_id": 1, "deadline_at": "2026-09-26T12:00:00Z"}`

- `201` 返回指令对象（有前驱时含 `predecessor_id`，被阻断时含 `blocked_by`，
  填写截止时刻时含 `deadline_at`）
- `400 invalid_json`：请求体不是合法 JSON（或含多个 JSON 值）
- `422 missing_payload`：缺少 `payload` 或为 `null`
- `422 invalid_predecessor_id`：`predecessor_id` 不是正整数（字符串/浮点/布尔/0/负数）
- `422 predecessor_not_found`：引用了不存在或晚于本条的编号
- `422 invalid_deadline`：`deadline_at` 存在但不是合法 RFC3339 时间字符串
  （数字/布尔/数组/无法解析的字符串）

### `POST /commands/{id}/expire` — 针对指定指令的到期结算

无请求体。只有**数据库时钟已到截止点且指令尚未送达**（仍 `pending`）时才结算：

- 状态原子置为只读终态 `expired`；写入恰好一行 `settlements`（`result="expired"`，
  **不伪造回执**：`generation`/`lease_token` 均为 `null`），历史租约只保留观察记录。
- 在同一事务/同一行锁裁决下，按现有前驱闭包规则把尚未送达（仍 `pending`）的整条
  后继链原子转为 `blocked`，每条后继的 `blocked_by` 记录**到期根编号**（到期根因）；
  已在终态的节点保持原样。
- 与回执互斥：截止前送达已落地的指令，到期结算只看到终态 → `409 already_settled`，
  送达不被追改；截止后才落地的回执看到截止谓词不成立 → `409 deadline_reached`，
  到期结算随后接管。
- 重复到期结算返回 `409 already_settled`，**绝不产生第二行结算**
  （`settlements.command_id` 主键物理兜底）。

裁决顺序：未知编号 `404 unknown_command` → 已在终态（含 `expired`/`blocked`）
`409 already_settled` → 未填写截止时刻或数据库时钟未到点 `409 deadline_not_reached`。
成功 `200` 返回指令详情（形态与 `GET /commands/{id}` 一致）。

### `POST /claims` — 原子领取最早可用指令

请求：`{"lease_duration_ms": 1000}`

- `lease_duration_ms` 必须是 **100–5000** 的整数，否则 `422 invalid_lease_duration`；
  字段缺失为 `422 missing_lease_duration`。
- 领取规则：状态为 `pending`、未持有有效（未过期）租约、**未过执行截止点**
  （`deadline_at` 为空或 `deadline_at > now()`）、且**整条前驱链都已 `delivered`** 的指令中，
  取 **ID 最小**的一条。前驱未全部送达或已过窗的指令一律等待/跳过，绝不预发租约；
  `delivered`/`failed`/`blocked`/`expired` 终态指令不可领取。未填写截止时刻时，
  领取顺序与旧行为完全一致。
- 并发安全：底层是对 `commands` 的 `SELECT ... ORDER BY id FOR UPDATE SKIP LOCKED`
  行锁扫描，前驱资格由传递闭包反连接在扫描中逐行求值，
  同一时刻并发的任意多个领取者中**恰有一个**能拿到同一条指令（等待中的后继不会被预发）。
- `200`：

  ```json
  {
    "command_id": 1,
    "generation": 1,
    "lease_token": "8d221c1e…(256bit 随机数的十六进制，不透明)",
    "lease_expires_at": "2026-09-18T00:00:01Z",
    "lease_duration_ms": 1000
  }
  ```

  每次领取都会推进该指令的 `lease_generation` 并签发**全新随机令牌**，
  新令牌必然不同于任何历史令牌（同时由数据库唯一约束兜底）。
- `204`：当前没有可领取的指令（无响应体）。

### `POST /commands/{id}/ack` — 凭租约确认终态

请求：`{"lease_token": "…", "result": "delivered"}`

- `result` 仅允许 `"delivered"` 或 `"failed"`，否则 `422 invalid_result`；
  令牌缺失为 `422 missing_lease_token`。
- 只有**未过期且属于当前代次、且数据库时钟仍早于执行截止点**的令牌可以结算。
  裁决顺序（行锁内原子完成）：
  1. 指令已在终态（含 `blocked`、`expired`）→ `409 already_settled`（重复结算/覆盖一律拒绝）；
  2. 令牌从未对该指令签发 → `409 invalid_lease_token`；
  3. 令牌属于旧代次 → `409 lease_expired`（失联者迟到）；
  4. 令牌已过期 → `409 lease_expired`（以**数据库时钟**判定，实例本地时钟偏差不影响结论）；
  5. 数据库时钟已到执行截止点 → `409 deadline_reached`（旧租约即使尚未达到自身租期
     也不能送达；状态保持 `pending`，由到期结算接管）。
- 成功 `200` 返回指令详情，`status` 变为 `delivered` 或 `failed`。
- `result="failed"` 在同一事务内把尚未送达的整条后继链原子置为只读 `blocked`，
  每条后继的 `blocked_by` 记录失败根编号；不写租约、不伪造结算。传播只作用于仍
  `pending` 的后继，已 `delivered` 的节点不受影响。
- 任意失败路径都**不会改变状态**。
- 未知编号：`404 unknown_command`（非数字 ID 同样按 404 处理）。

### `GET /commands/{id}` — 观察领取代次

`200` 返回指令详情：

```json
{
  "id": 1,
  "payload": {"seq": 1},
  "status": "failed",
  "deadline_at": "2026-09-26T12:00:00Z",
  "lease_generation": 3,
  "current_lease_expires_at": "2026-09-18T00:00:03Z",
  "created_at": "…",
  "updated_at": "…",
  "leases": [
    { "generation": 1, "claimed_at": "…", "expires_at": "…" },
    { "generation": 2, "claimed_at": "…", "expires_at": "…" },
    { "generation": 3, "claimed_at": "…", "expires_at": "…" }
  ],
  "settlement": { "generation": 3, "result": "failed", "settled_at": "…" }
}
```

- 有前驱时返回 `predecessor_id`；被阻断时返回 `blocked_by`（阻断来源根编号）；
  填写截止时刻时返回 `deadline_at`。未填写/未阻断时这些字段省略（旧指令响应形态不变）。
- 指令状态、`leases`、`settlement` 在同一个 `REPEATABLE READ` 只读事务快照中读取：
  三者必然对应同一可解释的状态，查询跨过并发领取/回执/到期也不会拼出矛盾视图。
- `leases` 给出**每一代领取记录**，可观察代次推进；GET 永不返回令牌原文；
  从未领取（含 `blocked`）时为空数组 `[]`。
- 未结算时 `settlement` 为 `null`（`blocked` 同样为 `null`）。`expired` 是正式结算：
  `settlement = {"generation": null, "result": "expired", "settled_at": "…"}`
  ——有结算行记录到期根因，但不伪造任何租约回执；到期前若领取过，租约历史照常保留。
  未知编号 `404 unknown_command`。

`GET /commands` 按创建顺序返回指令列表。`GET /healthz` 为存活探针。

### 状态码汇总

| 场景 | 状态码 | error.code |
| --- | --- | --- |
| 无可用指令可领取 | `204` | —（无体） |
| 未知指令编号 | `404` | `unknown_command` |
| 未知路由 / 方法不允许 | `404` / `405` | `route_not_found` / `method_not_allowed` |
| 请求体非法 JSON | `400` | `invalid_json` |
| 租期越界（<100 或 >5000ms）/ 缺失 | `422` | `invalid_lease_duration` / `missing_lease_duration` |
| 结果非法 / 令牌缺失 / 载荷缺失 | `422` | `invalid_result` / `missing_lease_token` / `missing_payload` |
| `predecessor_id` 非正整数 | `422` | `invalid_predecessor_id` |
| 前驱编号不存在或晚于本条 | `422` | `predecessor_not_found` |
| `deadline_at` 不是合法 RFC3339 时间字符串 | `422` | `invalid_deadline` |
| 旧代次或已过期令牌迟到 | `409` | `lease_expired` |
| 令牌从未对该指令签发 | `409` | `invalid_lease_token` |
| 数据库时钟已过执行截止点仍试图回执 | `409` | `deadline_reached` |
| 到期结算被提前调用（无截止时刻或未到点） | `409` | `deadline_not_reached` |
| 重复结算 / 覆盖终态 / 确认 `blocked`/`expired` / 重复到期 | `409` | `already_settled` |

## 数据模型

迁移 SQL 内嵌于 `internal/store/migrations_sql/`，启动时自动应用：

- `commands`：指令本体、当前 `status`（`pending|delivered|failed|blocked|expired`）、
  唯一 `predecessor_id`、阻断来源 `blocked_by`（与 `blocked` 状态由 CHECK 约束强制
  同生同灭）、可选执行截止时刻 `deadline_at`、当前 `lease_generation`、当前令牌与
  到期时间。
- `command_closure`：前驱关系的**传递闭包**，每条指令与其每个祖先各一行
  （`(command_id, ancestor_id)` 主键）。领取资格因此是普通反连接
  （“不存在状态非 delivered 的祖先”），可在 `commands` 的 `FOR UPDATE SKIP LOCKED`
  行锁扫描中逐行求值——避免“递归 CTE 先物化候选再加锁”在高并发下让两个领取者
  同时选中同一行的锁洞。
- `leases`：每一代租约（`(command_id, generation)` 唯一，`lease_token` 全局唯一），
  构成可观察的领取历史。`blocked` 指令从不写入租约。
- `settlements`：以 `command_id` 为主键——即使应用层存在疏漏，数据库层面也**物理禁止
  插入第二条终态**，是“不重复结算”的最后防线。`result` 允许
  `delivered|failed|expired`；`blocked` 不产生结算行（没有伪造结算）。到期结算行
  `result='expired'` 且 `generation/lease_token` 为 NULL（不伪造回执），该同生同灭
  关系由 CHECK 约束强制。

领取在单事务内完成（行锁扫描 + 闭包/截止资格 + 代次推进 + 租约写入），
确认在单事务内完成（行锁 + 终态裁决 + 截止点裁决 + `settlements` 插入 + 状态翻转 +
后继链原子转 `blocked`），到期结算在单事务内完成（同一行锁 + 到点/未送达裁决 +
`settlements` 插入 + 状态翻转 + 后继链原子转 `blocked`），创建带前驱/截止时刻的指令
也在单事务内完成（逐行锁定整条前驱链 + 状态裁决 + 闭包写入）。
因此创建与前驱结算交错时由数据库行锁裁决，回执与到期结算也在同一行锁下互斥，
任何崩溃/重启都不会留下“前驱已失败/到期、后继却仍可领取”，或半完成的裁决。

详情查询（`GET /commands/{id}`）在 `REPEATABLE READ` 只读事务的单快照中读取
`commands`/`leases`/`settlements` 三张表，响应内部必然自洽。所有时间戳
（`created_at`/`claimed_at`/`expires_at`/`settled_at`/`deadline_at`）、租约有效性
判定与截止点判定都使用数据库时钟，多实例部署时实例本地时钟偏差不参与任何裁决。

## 验收交错（`cmd/verify`）

`verify` 是一次性服务，断言完整生命周期：

1. 输入校验契约（422/404/400/204 与稳定错误码）；
2. **前驱链契约**：前驱未送达的后继不被预发租约、根送达后逐节解锁、链上 `failed`
   原子传播为后继 `blocked`（含阻断来源、无租约无结算）、失败后新建后继出生即
   `blocked`、无前驱旧指令照常领取；
3. 创建唯一指令，**16 个请求并发抢领**，断言恰有 1 个 `200`、其余全为 `204`；
4. 等待第一代租约到期后重新领取，断言代次推进、新令牌不同；
5. 旧令牌迟到确认 → `409 lease_expired` 且状态仍为 `pending`；
6. 再等第二代到期、领取第三代，用第二代旧令牌逆序确认 → `409`，状态不变；
7. 当前持有者置 `failed`；旧/新/持有者令牌再次确认全部 `409`，终态不被覆盖；
8. 直连 PostgreSQL 带外核对：`settlements` 恰 1 行、`leases` 恰 3 代、状态唯一；
9. **重启 API 进程**（同一数据库、新进程重新迁移连接），核对终态 `failed/gen=3`
   保持不变，且重启后重复确认仍为 `409`、终态指令仍不可领取；
10. **读一致性**：写协程以可控的 100ms 短租约连推 10 代并结算（另起一条链做失败
    阻断传播），18 个读协程高频并发 GET，断言任何响应都不存在跨代租约或
    “待处理且已结算”的组合；失败后下游为 `blocked`、无租约无结算，与结算记录相符；
11. **实例时钟偏移**：额外启动两台 API 实例，实例本地时钟分别偏移 ±1 小时
    （`DEEPSPACE_CLOCK_OFFSET_MS` 注入），断言尚在有效期的凭证在时钟超前的实例上
    同样被接受、已过期凭证在时钟落后的实例上同样被拒绝（过期边界跨实例一致）；
12. **执行截止时刻**：`deadline_at` 输入校验（非法值 422、缺省/null 旧行为）；过窗指令
    领取时被跳过（含“从未领取”和“持有未到期长租约”两种情形），无截止指令领取顺序
    不变；截止后持未到期租约的回执 → `409 deadline_reached` 且状态不变；截止前送达
    不被到期追改；到点到期结算原子 `expired`（无回执结算）、后继链原子 `blocked`
    （根因=到期根）、到期后新建后继出生即 `blocked`；重复到期不产生第二行结算；
    带外核对 `settlements` 恰一行；重启进程后终态与阻断保持、重复到期仍 `409`。

退出码 0 即全部通过。

## 项目结构

```
cmd/api/main.go                 API 入口（迁移、连接重试、优雅关停）
cmd/verify/main.go              一次性验收服务
internal/httpapi/               路由、处理器、稳定 JSON 错误信封
internal/store/                 PostgreSQL 持久化与租约裁决
internal/store/migrations_sql/  内嵌迁移 SQL
internal/testdb/                测试用临时数据库辅助
```
