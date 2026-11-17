# 电表日结 SQL 核对 API

数仓工程师改写电表日结的关联 SQL 后，用本服务确认读数**没有漏计或重复计入**。
提交两条只读 `SELECT`，服务在同一已提交快照上并行读取两侧结果，按
**行内容 + 出现次数**做多重集核对（忽略行序，`NULL` 与空串视为不同），
返回一致结论或最多 20 种差异行及两侧次数。

服务同时提供**采集员补传读数的批量接入**接口：按批次号幂等、整批原子
写入 `readings`，回执与读数在同一事务内提交；写入后原有核对 SQL 照常读取。

## 1. 快速启动

```bash
docker compose up --build
# API:   http://localhost:8000
# 文档:  http://localhost:8000/docs   （Swagger UI，可直接发请求演示）
```

启动后 PostgreSQL 自动执行 `sql/init.sql`，建好 `meters` / `readings`
源表、`ingestion_receipts` 回执表并写入示例数据；文件末尾附有可直接粘贴到
`/docs` 的两条等价示例查询。`/docs` 里的 **读数接入** 分组可直接演示
`POST /readings/batches`（请求体已带示例值）。

停止并清理：

```bash
docker compose down -v
```

## 2. 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET`  | `/health` | 存活探针，不触碰数据库 |
| `POST` | `/readings/batches` | 采集员补传读数（批量接入，见第 3 节） |
| `POST` | `/jobs` | 提交核对任务（同时只允许一组运行，占用中返回 `409`） |
| `GET`  | `/jobs` | 列出近期任务（含终态），状态查询始终可用 |
| `GET`  | `/jobs/{job_id}` | 查询单个任务状态与结论 |
| `POST` | `/jobs/{job_id}/cancel` | 请求取消，及时受理 |

任务状态：`pending → running →（cancelling）→ succeeded | failed | cancelled | timeout`。
失败、取消、超时一律 **不发布比较结论**（`result` 为 `null`），仅给出原因。

### 请求示例

```bash
curl -X POST http://localhost:8000/jobs -H 'Content-Type: application/json' -d '{
  "left_sql":  "SELECT meter_id::bigint, wh::bigint FROM readings",
  "right_sql": "SELECT meter_id::bigint, wh::bigint FROM readings",
  "name": "daily-settlement-v2"
}'
```

### 结论结构（成功时）

```json
{
  "status": "succeeded",
  "result": {
    "identical": false,
    "columns": ["meter_id", "wh"],
    "column_types": ["bigint", "bigint"],
    "left_row_total": 504013,
    "right_row_total": 504012,
    "matched_group_count": 504011,
    "diff_group_total": 1,
    "diffs_shown": 1,
    "diffs": [
      {"values": [1001, 1150], "left_count": 2, "right_count": 1}
    ]
  }
}
```

## 3. 采集员补传：批量接入读数

`POST /readings/batches` 一次提交批次号和 1～500 条读数，原子写入
`readings`，并返回本次新增条数与“同键同值已存在”的条数。

```bash
curl -X POST http://localhost:8000/readings/batches \
  -H 'Content-Type: application/json' -d '{
  "batch_id": "collector-20261115-001",
  "readings": [
    {"meter_id": 1001, "read_time": "2026-11-15T12:00:00Z", "wh": 1480},
    {"meter_id": 1002, "read_time": "2026-11-15T12:00:00Z", "wh": 770}
  ]
}'
# 201 {"batch_id":"collector-20261115-001","inserted":2,"existing":0}
```

同批次同内容重试：响应体**与首次回执完全一致**（`inserted`/`existing`
保持首次落库时的数字），并带响应头 `Idempotent-Replay: true`；
此时服务不再触碰 `readings`，即使读数后来被直连 SQL 更正也不会被回写。

| 情况 | 结果 |
| --- | --- |
| 批内同一 `(meter_id, read_time)` 出现多次 | `422`，整批不落库 |
| 批内条数为 0 或超过 500；`read_time` 不带时区 | `422` |
| 读数条数 1～500，表号在 `meters` 中不存在 | `422`，整批不落库 |
| 库中同键读数与提交值不同 | `409`，整批不落库 |
| 同 `batch_id` 已存在但内容不同 | `409`，不覆盖原回执与读数 |
| 同 `batch_id` 同内容（顺序不计） | `201`，返回原回执 + `Idempotent-Replay: true` |
| 并发锁等待超时 / 数据库暂时不可用 | `409` / `503`，整批未生效，可重试 |

**语义要点**

- **入仓时实际值判定**：按 `(meter_id, read_time)` 固定顺序 `SELECT … FOR UPDATE`。
  若同键在库中值相同则计入 `existing`；不同则整批拒绝。其他批次或直连 SQL
  在提交过程中并发写入同一键时，会等待其事务结束后按**最终提交值**判定，
  不会出现“先到者覆盖后到者”。
- **回执只读**：同一批次号只有一条回执。重放路径只读 `ingestion_receipts`，
  不重新比对当前读数，因此直连 SQL 的事后更正不会影响回执，也不会被覆盖。
- **原子性**：读数与回执在同一事务内写入并一起提交；任何校验失败、并发
  冲突或数据库错误都整体回滚，不会留下半批读数或孤立回执。
- **可核对**：`readings` 表结构不变，新入仓数据可被原有日结核对 SQL
  直接读取，用于确认补传后日结没有漏计或重复计入。

## 4. 对提交 SQL 的约束

- 必须是**单条只读 SELECT**（允许 `WITH` / `UNION` / `INTERSECT` / `EXCEPT`），
  静态解析拒绝多语句与任何写操作 / DDL / `SELECT INTO` / `LOCK` / `CALL` 等；
- 结果列只允许 `bigint` 与 `text`，两侧**列数相同、对应位置类型相同**，
  否则本轮失败且不给出比较结论；
- 数据库连接再叠加一层纵深防御：`READ ONLY` 事务、`statement_timeout`、
  `lock_timeout`，即使静态校验被绕过也无法写入或长时间持锁。

## 5. 关键设计

### 同一已提交快照，源表仍可更正

每条任务开三条独立连接：快照连接执行
`BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY` + `pg_export_snapshot()`，
两条读取连接分别 `SET TRANSACTION SNAPSHOT` 绑定**同一个快照**。
核对期间其他会话对 `readings` / `meters` 的更正照常提交，但对本轮不可见。

### 内存有界的外部归并（不全量载入、不靠哈希判等）

1. 命名服务端游标按 `itersize` 分批拉取，客户端从不缓存整表；
2. 每 `SORT_CHUNK_ROWS`（默认 2 万）行在进程内排序，逐行流式 pickle 落盘为一个 run；
3. 归并阶段对两侧 run 做单轮 k 路堆归并（堆中每流只驻留一行），
   键随行走，按全序键聚合等值组后比较两侧次数；
4. 差异明细最多保留 20 种，但总行数、差异组总数等计数覆盖全量数据；
5. 全程不使用整表哈希作为判等依据。

### 单名额、30 秒时限与终止语义

- 进程内单名额信号量：全局同时只运行一组，冲突返回 `409`；
- 看门狗按 `JOB_TIMEOUT_SECONDS`（默认 30）判定超时；
- 取消/超时/任一侧失败后：`conn.cancel()` 请求中止本轮全部 SQL，
  必要时 `pg_terminate_backend` 兜底；
- 严格按顺序收尾：**中止 SQL → 关闭连接 → 清空暂存目录 → 释放名额 → 才落终态**，
  因此状态查询一旦看到终态，名额必然已经释放、暂存必然已经清空。

## 6. 配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PG_DSN` | `postgresql://recon:recon@postgres:5432/recon` | 数据库连接串 |
| `JOB_TIMEOUT_SECONDS` | `30` | 单轮硬时限 |
| `PG_STATEMENT_TIMEOUT_MS` | `28000` | 服务端单语句超时（留 2s 收尾余量） |
| `SORT_CHUNK_ROWS` | `20000` | 单个排序块行数 |
| `MAX_DIFF_TYPES` | `20` | 差异展示上限 |
| `WORK_DIR` | `/tmp/recon-work` | 外部排序暂存目录（独立卷） |
| `INGEST_LOCK_TIMEOUT_MS` | `20000` | 批量接入遇到并发冲突时的最长等待；超时整批回滚、可重试 |

## 7. 本地自测

```bash
# 单元 + 集成测试（集成测试需要一个 PostgreSQL；可用 PG_DSN 指定）
python -m pytest tests/ -q
# 仅批量接入的幂等 / 原子性 / 并发用例
python -m pytest tests/test_ingest.py -q

# 50 万行级端到端压测（内存峰值、暂存占用、结论正确性、清理与名额释放）
python scripts/load_test.py
```

实测（受限沙箱环境，容器内通常更快）：两侧各 504,012 行、
含一次重复计入差异，端到端约 16 秒，进程峰值 RSS 约 68 MiB，
暂存峰值约 44 MiB，结束后暂存目录清空、名额释放。

## 8. 目录结构

```
app/
  config.py          运行期配置
  ingest.py          批量接入：幂等回执、原子写入、并发判定
  sqlvalidation.py   只读 SELECT 静态校验
  db.py              快照会话、只读连接、命名游标流式拉取、列类型闸门
  external_merge.py  流式 run、k 路归并、多重集核对
  manager.py         单名额编排、看门狗、取消/超时/失败的统一收尾
  models.py          API 模型
  main.py            FastAPI 路由
sql/init.sql         源表、回执表、示例数据、示例查询
scripts/             压测数据生成与负载脚本
tests/               单元与集成测试
docker-compose.yml   应用（128MiB 上限）+ PostgreSQL
Dockerfile
```
