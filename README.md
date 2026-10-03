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
```

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

- 新增版本只注册新 handler，既有版本的 handler 与响应结构不变（见
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
- 失败的尝试允许**相同载荷**在该批并发落定之后安全重试（自动接替成为新
  leader）；已完成记录在 TTL（默认 1 小时）后过期，标识可复用。

### 跨版本重复提交（客户端升级期间新旧版本并存）

同一个幂等标识可能被不同版本的客户端先后或并发提交，语义如下：

- **一笔业务结果**：幂等记录里存的是**版本无关的业务结果**——业务主键
  （`WriteResult.business_key`）+ 规范实体（`WriteResult.entity`）。所有
  版本的 handler 对同一业务返回同一主键与同一份规范内容，不会各写各的。
- **各版本看到自己声明的形态**：重放时以**请求方声明的版本**渲染规范实体
  （`renderers` 注册表，每版本一个渲染函数）。v2 调用方拿到 v2 的扁平
  结构，v3 调用方拿到 v3 的信封结构，但两边指向的 `order_id`、写入内容
  完全一致。最先到达者的版本不影响后来者的响应形态。
- **判重仍按业务载荷**：指纹只取载荷、与版本无关。同一 key + 同一载荷跨
  版本重放；同一 key + 不同载荷（即使来自另一个版本）仍然 409 冲突。
- 兼容性：handler 仍可直接返回普通 dict（等价于规范实体、业务主键为空），
  旧 handler 无需改动；只有需要跨版本渲染时才返回 `WriteResult` 并注册
  renderer（见 `main.py` 与 `tests/test_cross_version_idempotency.py`）。

### 并发失败时结论收敛

同一标识的一批并发请求里，leader 首次就失败（502）或超时（504）时：

- 整批等待者**收敛到同一次失败结论**：错误码、错误文案、`Retry-After`
  重试提示完全一致，响应里带 `convergence=leader-failure` 判定日志。
- **下游在这一批里只真正触发一次**，等待者不会被放行去打第二次下游，也
  不会挂起或看到伪造的成功。
- 收敛只作用于"同一批并发"：等这批全部落定之后，后续到达的相同载荷请求
  仍是一次全新尝试，可接替成为新 leader（原有失败重试语义不变）。

## 状态回收与容量上限

幂等记录与配额桶都有回收能力，常驻内存不随历史请求数无界增长：

- **幂等记录**（`IdempotencyStore(ttl_seconds, max_records)`）：
  - 终态（completed/failed）记录在最后一次访问后经过 TTL 自动过期；每次
    重放会**滑动续期**，仍在被用来判重的 key 不会过期；长期没人再用的
    冷记录到期即释放。
  - **`in_flight` 记录永不回收**（对应一次正在进行的下游调用），有效期内
    的终态记录也不会被清理，因此回收绝不改变判定结果。
  - 过期项在 `begin()` 时按 key 惰性清理，并按时间节流做全量扫描；
    `max_records` 是额外的硬上限安全阀（正常情况下只靠 TTL 就够，只有
    存活记录数真的超过上限时才淘汰最接近过期的终态记录）。
- **配额桶**（`QuotaLimiter(..., max_buckets)`）：桶属于固定窗口，窗口
  一旦翻过，旧桶即失效并在随后自动释放；当前窗口内的桶保留、计数判定不
  变。`max_buckets` 为单窗口内不同调用方数量提供硬上限。
- 时间全部走 `Clock` 抽象（生产用 `SystemClock`，测试用 `FakeClock`
  手动拨钟），回收行为可复现，无需真实等待。

## 本地验证

```bash
# 裸跑默认命令即可：实时输出每次判定（调用方/版本/幂等标识 + 放行/重放/拒绝）
uv run pytest

# 旧写法等价
uv run pytest tests/ -q

# 启动服务
uv run python main.py   # 或 uv run uvicorn main:app --port 8000

# 手动验证（跨版本重放）
curl -i -X POST localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -H 'X-API-Version: v3' -H 'X-Caller-Tier: standard' -H 'X-Caller-Id: demo' \
  -H 'X-Idempotency-Key: order-42' \
  -d '{"item": "book", "qty": 1}'
# 同 key 同载荷改声明 X-API-Version: v2 再发
#   → 200 + X-Idempotent-Replay: true，形态为 v2，但 order_id 与 v3 一致
# 改载荷重发同一 key → 409 IDEMPOTENCY_CONFLICT
# 不带 X-API-Version → 400 VERSION_MISSING；X-API-Version: v1 → 410 VERSION_SUNSET
```

新增场景测试：

| 文件 | 覆盖场景 |
|------|----------|
| `tests/test_cross_version_idempotency.py` | 跨版本顺序/并发重放、形态各异但业务主键一致、跨版本载荷冲突 |
| `tests/test_concurrent_failure_convergence.py` | 并发批次首次 502/504 时整批收敛、下游仅触发一次、失败后再重试可放行 |
| `tests/test_state_retention.py` | TTL 过期、滑动续期、in-flight 保护、容量硬上限、配额窗口滚动回收 |

## 目录结构

```
gateway/
  clock.py        # Clock 抽象：SystemClock / FakeClock（测试用可控时钟）
  errors.py       # 统一错误码与错误结构
  versioning.py   # 版本注册表与协商
  quota.py        # 分层级固定窗口限流（线程安全，窗口回收 + 容量上限）
  idempotency.py  # 幂等记录状态机（leader/wait/replay/conflict，TTL 回收）
  service.py      # 网关编排管线（跨版本渲染 + 并发失败收敛 + WriteResult）
main.py           # FastAPI HTTP 适配层
tests/            # 场景测试：版本/配额/幂等/下游异常/跨版本/失败收敛/回收
```
