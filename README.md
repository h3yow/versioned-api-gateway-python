# Versioned API Gateway

面向外部调用方的版本化 API 网关。核心逻辑在 `gateway/` 包（纯 Python、可单测），
`main.py` 是 FastAPI HTTP 适配层。

## 架构与请求管线

每个请求按固定顺序经过四级判定，任一环节拒绝即返回统一错误结构：

```
版本协商 → 配额限流 → 幂等去重 → 下游处理
```

每次判定都会输出日志，包含调用方、版本、幂等标识与最终判定依据，例如：

```
caller=anon-1 version=v3 idem=key-1 verdict=rejected basis=QUOTA_EXCEEDED
caller=anon-1 version=v3 idem=key-1 verdict=replay   basis=completed-record
caller=anon-1 version=v2 idem=key-1 verdict=replay   basis=failed-record
```

判定动词三种：`executed`（放行并执行下游）、`replay`（重放已有结论，
含成功结果与失败结论）、`rejected`（拒绝）。默认测试命令即可看到这些日志。

## 版本兼容规则

- 客户端必须通过 `X-API-Version` 头声明版本；同一业务能力按声明版本返回对应结构。
- 版本有三种状态：`active`（正常服务）、`deprecated`（仍服务，响应带
  `Deprecation: true` 头）、`sunset`（已下线，拒绝服务）。
- 拒绝原因可区分：

  | 情形       | 错误码             | HTTP |
  |------------|--------------------|------|
  | 未声明版本 | `VERSION_MISSING`  | 400  |
  | 版本不存在 | `VERSION_UNKNOWN`  | 404  |
  | 版本已下线 | `VERSION_SUNSET`   | 410  |

- 新增版本只注册新 handler / renderer，既有版本的响应结构不变（见
  `main.py` 中 v2/v3 并存），由 `test_versioning.py` 回归保证。

## 配额口径

- 按 **(调用方层级, 调用方 ID)** 分桶，固定窗口计数（默认 60 秒）。
- 三个层级各自独立：`anonymous` / `standard` / `privileged`，
  通过 `X-Caller-Tier` 与 `X-Caller-Id` 头标识。
- 超限返回 429 + `QUOTA_EXCEEDED`，错误体与 `Retry-After` 头给出
  可解释的重试等待秒数。
- 检查与计数在同一把锁内原子完成，并发请求不会少计或重复计数
  （`test_concurrent_requests_counted_exactly` 验证 100 并发恰好放行限额数）。

## 幂等语义

- 写请求携带 `X-Idempotency-Key`；载荷以规范化 JSON 的 SHA-256 作为指纹。
- 同一标识的并发重复提交：仅一个请求成为 leader 执行下游，其余等待后
  **重放首次结果**（响应带 `X-Idempotent-Replay: true`），下游只执行一次。
- 同一标识携带**不同载荷**：返回 409 + `IDEMPOTENCY_CONFLICT`，
  明确拒绝，绝不静默覆盖。
- 下游异常 → 502 `DOWNSTREAM_ERROR`；下游超时 → 504 `DOWNSTREAM_TIMEOUT`
  （带重试提示）。两者可区分，且内部异常细节（连接串、堆栈等）不会泄漏给调用方。
- 失败的尝试允许**相同载荷**安全重试（自动接替成为新 leader）；
  已完成记录在 TTL（默认 1 小时）后过期，标识可复用。

### 跨版本重复提交

客户端升级期间，新旧版本可能用**同一个幂等标识**先后或并发提交同一笔写操作：

- 下游写操作仍然只执行一次，存储在标识下的是**与版本无关的业务结果**
  （canonical result，含业务主键与写入内容）。
- 每个请求拿到的响应由**它自己声明的版本**的 renderer 渲染：v2 调用方永远
  拿到 v2 结构，v3 调用方永远拿到 v3 结构，与"谁先执行"无关。
- 各版本结构指向同一笔业务结果：业务主键（如 `order_id`）与内容完全一致，
  不会各写各的。
- 跨版本的载荷冲突检测不变：同标识不同载荷依旧 409。

实现上，下游 handler 只负责执行并返回 canonical result；响应形态由
`Gateway(renderers={版本: 渲染函数})` 按版本渲染。未注册 renderer 的版本
回退为恒等渲染（canonical 原样返回），与旧的单形态行为完全兼容——
既有调用方不注册 renderer 时行为不变。

### 并发失败收敛

同一标识的一批并发请求中，若最先执行的 leader 下游**失败或超时**：

- 这批里所有等待中的请求收敛到**同一次失败结论**：相同的错误码、相同的
  错误消息、相同的重试提示（`Retry-After`），日志记为
  `verdict=replay basis=failed-record`。
- 整批请求下游只被真正触发一次，不会出现"一部分人看到失败、另一部分人
  又被放行重打下游"。
- 批次**之后**新到达的请求不受失败结论约束：相同载荷可安全重试
  （接替成为新 leader），与既有重试语义一致。

## 状态回收与容量上限

服务长期运行时，幂等记录与配额桶不会无界堆积：

- **幂等记录**（`IdempotencyStore`）：
  - 终态记录（完成/失败）超过 TTL 后可回收；`sweep()` 主动清扫，
    记录数达到 `max_records` 时也会在写入路径上自动触发清扫。
  - 处理中（IN_FLIGHT）记录**永不**因 TTL 被回收；仅当超过
    `inflight_timeout_seconds`（leader 崩溃留下的孤儿记录）才可回收。
  - 仍在有效期内、后续判重还要用的记录**绝不**被驱逐；容量满且没有
    可回收记录时允许短暂超限（上限退化为活跃工作集大小），判定结果不受影响。
- **配额桶**（`QuotaLimiter`）：过期窗口的桶对判定无影响（下次访问本来就会
  重置），可安全回收；`sweep()` 或桶数达到 `max_buckets` 时自动清扫，
  常驻桶数收敛到当前窗口的活跃调用方数量。
- 回收只删除"对判定不再有影响"的状态，因此**不改变**任何对调用方给出的
  判定结果（`test_reclamation.py` 逐项验证）。

## 本地验证

```bash
# 运行测试（使用 FakeClock 可控时钟，结果可复现；
# 默认即输出每次判定的 caller/version/idem/verdict 日志）
uv run pytest

# 启动服务
uv run python main.py   # 或 uv run uvicorn main:app --port 8000

# 手动验证
curl -i -X POST localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -H 'X-API-Version: v3' -H 'X-Caller-Tier: standard' -H 'X-Caller-Id: demo' \
  -H 'X-Idempotency-Key: order-42' \
  -d '{"item": "book", "qty": 1}'
# 再次发送同一请求 → 200 + X-Idempotent-Replay: true（v3 结构）
# 改用 X-API-Version: v2 重发同一 key → 200 + 重放，但为 v2 结构，
#   且 order_id 与首次一致（跨版本重放）
# 改载荷重发同一 key → 409 IDEMPOTENCY_CONFLICT
# 不带 X-API-Version → 400 VERSION_MISSING；X-API-Version: v1 → 410 VERSION_SUNSET
```

## 目录结构

```
gateway/
  clock.py        # Clock 抽象：SystemClock / FakeClock（测试用可控时钟）
  errors.py       # 统一错误码与错误结构
  versioning.py   # 版本注册表与协商
  quota.py        # 分层级固定窗口限流（线程安全，过期窗口桶可回收）
  idempotency.py  # 幂等记录状态机（leader/wait/replay/conflict，TTL+容量回收）
  service.py      # 网关编排管线（canonical result + 按版本渲染，失败收敛）
main.py           # FastAPI HTTP 适配层
tests/            # 版本/配额/幂等/下游异常/跨版本/并发失败/状态回收场景测试
```
