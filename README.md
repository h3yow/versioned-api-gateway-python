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
- 失败的尝试允许**相同载荷**安全重试（自动接替成为新 leader）；
  已完成记录在 TTL（默认 1 小时）后过期，标识可复用。

## 本地验证

```bash
# 运行测试（使用 FakeClock 可控时钟，结果可复现）
uv run pytest tests/ -q

# 查看判定日志
uv run pytest tests/ -q --log-cli-level=INFO

# 启动服务
uv run python main.py   # 或 uv run uvicorn main:app --port 8000

# 手动验证
curl -i -X POST localhost:8000/orders \
  -H 'Content-Type: application/json' \
  -H 'X-API-Version: v3' -H 'X-Caller-Tier: standard' -H 'X-Caller-Id: demo' \
  -H 'X-Idempotency-Key: order-42' \
  -d '{"item": "book", "qty": 1}'
# 再次发送同一请求 → 200 + X-Idempotent-Replay: true
# 改载荷重发同一 key → 409 IDEMPOTENCY_CONFLICT
# 不带 X-API-Version → 400 VERSION_MISSING；X-API-Version: v1 → 410 VERSION_SUNSET
```

## 目录结构

```
gateway/
  clock.py        # Clock 抽象：SystemClock / FakeClock（测试用可控时钟）
  errors.py       # 统一错误码与错误结构
  versioning.py   # 版本注册表与协商
  quota.py        # 分层级固定窗口限流（线程安全）
  idempotency.py  # 幂等记录状态机（leader/wait/replay/conflict）
  service.py      # 网关编排管线
main.py           # FastAPI HTTP 适配层
tests/            # 四类场景测试
```
