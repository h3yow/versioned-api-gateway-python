# 版本化 API 网关（Python / FastAPI）

在既有 FastAPI 服务之上，补了一套面向外部调用方的版本化 API 网关能力：
**接口版本协商与废弃策略**、**基于调用方身份的限流与配额**、
**写请求幂等与并发去重**、以及**统一可区分的错误结果**。

## 目录结构

```
gateway/
  errors.py        # 统一错误码与 GatewayError（不外泄内部细节）
  clock.py         # 时钟协议：SystemClock / 可控 FakeClock
  versions.py      # 版本注册、协商（缺失/未知/已下线）与状态（active/deprecated/sunset）
  quota.py         # 调用方身份、配额策略、固定窗口并发安全计数器
  idempotency.py   # 幂等记录、并发去重（条件变量）、载荷哈希
  downstream.py    # 下游协议、内部异常类型、默认模拟实现
  core.py          # 编排：协商 -> 配额 -> 幂等 -> 下游 -> 版本化响应
  api.py           # FastAPI 接入与错误码 -> HTTP 状态映射
main.py            # 服务入口
tests/             # 四类场景测试 + HTTP 端到端 + 可控替身
```

## 1. 版本兼容规则

客户端通过请求头 `X-API-Version` 显式声明版本（以 `orders` 能力为例）：

| 版本        | 状态        | 行为 |
|-------------|-------------|------|
| `2024-01-01` | sunset（已下线） | 拒绝，错误码 `version_sunset`（HTTP 410），消息提示继任版本 |
| `2025-01-01` | deprecated（废弃中） | 仍可调用，响应附带 `warning` 字段与升级建议 |
| `2026-01-01` | active（当前） | 正常 |

- 未声明版本：`version_missing`（HTTP 400）。
- 声明系统从未发布的版本：`version_unknown`（HTTP 400）。
- 三类拒绝原因彼此可区分，消息不混用。
- **向后兼容保证**：内部下游结果与版本无关，由 `core.render_response(version, internal)`
  按版本独立渲染。新增版本只新增一个渲染分支，**绝不修改既有版本的输出结构**。
  例如 `2025-01-01` 返回平铺结构且 `qty` 为字符串，`2026-01-01` 返回嵌套 `data`
  且 `qty` 为数值；同一订单在两个版本下订单号一致，仅结构不同。
- 废弃版本的响应带 `warning`；下线版本彻底拒绝，二者不等价。

## 2. 配额口径

按调用方身份分档，三档使用**各自独立**的固定窗口配额（见 `quota.DEFAULT_POLICIES`）：

| 身份 | 调用方式 | 额度（默认） |
|------|----------|--------------|
| anonymous（匿名） | 不带 `X-Caller-Id` | 2 次 / 60 秒 |
| normal（普通） | 带 `X-Caller-Id` | 10 次 / 60 秒 |
| privileged（高权限） | `X-Caller-Id` + `X-Caller-Tier: privileged` | 100 次 / 60 秒 |

- 计数以调用方 ID 为键，不同调用方互不影响；匿名调用方统一计入 `anonymous`。
- 计数器在锁内完成“读窗口 → 判定 → 自增”，并发请求不会少计或重复计数
  （见 `tests/test_quota.py::test_concurrent_requests_count_exactly_once`）。
- 超限返回 `quota_exceeded`（HTTP 429），响应消息给出可解释的等待秒数，
  并通过 `Retry-After` 响应头返回整数秒；窗口滑过之后计数自动重置。

## 3. 幂等语义

写请求（`POST /orders`）通过请求头 `Idempotency-Key` 声明幂等标识：

- **并发去重**：同一幂等键的并发重复提交，只有一个请求真正进入下游；
  其余请求在条件变量上阻塞，首个请求完成后被唤醒并**重放首次结果**，
  因此下游对同一键**恰好执行一次**。
- **同键不同载荷**：对载荷做规范化 SHA-256 哈希，键相同但载荷不同时返回
  `idempotency_conflict`（HTTP 409），明确拒绝而非静默覆盖。
- **首次失败**：下游异常/超时会记录失败原因并唤醒所有等待者，
  大家拿到**同一可区分错误码**；随后移除该键的占用，允许客户端之后用同键重试。
- 成功结果在 TTL 内保留并可重放，过期后同键视为新请求。
- 读请求（无幂等键 / 非 POST）不参与幂等。

## 4. 错误结果与下游隔离

所有失败统一为 `GatewayError`，响应体形如
`{"error": {"code": ..., "message": ...}}`，只暴露稳定错误码与对外说明：

| 错误码 | HTTP | 含义 |
|--------|------|------|
| `version_missing` / `version_unknown` | 400 | 版本缺失 / 未知 |
| `version_sunset` | 410 | 版本已下线 |
| `quota_exceeded` | 429 | 配额耗尽（带 `Retry-After`） |
| `idempotency_conflict` | 409 | 同键不同载荷 |
| `downstream_error` | 502 | 下游业务异常（含未预期异常的兜底转换） |
| `downstream_timeout` | 504 | 下游超时或超过执行期限 |

下游内部异常类型、堆栈、敏感字段（如密钥）不会出现在对外消息中。

## 5. 可解释日志

每次处理都会打印一条判定日志，包含：
**调用方、身份、版本、幂等标识、最终判定依据**。例如：

```
INFO gateway 调用方=user-1 身份=privileged 版本=2026-01-01 幂等键=idem-100 判定=首次提交，已执行下游并记录结果
INFO gateway 调用方=anon 身份=anonymous 版本=2026-01-01 幂等键=anon-3 判定=拒绝:quota_exceeded
INFO gateway 调用方=user-v 身份=privileged 版本=(缺失) 幂等键=v-missing 判定=拒绝:version_missing
```

## 6. 本地验证

依赖管理使用 [uv](https://docs.astral.sh/uv/)（要求 Python ≥ 3.14）。

```bash
# 安装依赖（含 pytest 开发依赖）
uv sync

# 运行全部测试；--log-cli-level=INFO 已在 pyproject 中默认开启，
# 单测日志会打印调用方/版本/幂等键/最终判定。
uv run pytest

# 只跑四类核心场景
uv run pytest tests/test_concurrency.py   # 并发重复提交（下游仅一次 / 同键不同载荷 / 失败重放后可重试）
uv run pytest tests/test_versions.py      # 版本缺失/未知/下线/废弃/多版本结构
uv run pytest tests/test_quota.py         # 三档配额/独立口径/并发计数/窗口重置与 Retry-After
uv run pytest tests/test_downstream.py    # 下游异常/超时统一转换、不泄漏细节

# HTTP 端到端（错误码、状态码、Retry-After 头）
uv run pytest tests/test_api_http.py

# 启动服务
uv run python main.py        # http://127.0.0.1:8000  （文档 /docs）
```

手动调用示例：

```bash
curl -s -X POST http://127.0.0.1:8000/orders \
  -H 'Content-Type: application/json' \
  -H 'X-API-Version: 2026-01-01' \
  -H 'X-Caller-Id: alice' -H 'X-Caller-Tier: normal' \
  -H 'Idempotency-Key: key-001' \
  -d '{"item":"book","qty":2}'
```

## 7. 测试的确定性手段

- 时间统一走 `Clock` 协议；测试注入 `FakeClock`，配额窗口、TTL、执行期限
  都通过手动 `advance()` 精确推进，结果可复现、不依赖真实睡眠。
- 并发用例通过 `FakeDownstream` 的 `entered` / `release` 两个事件，
  保证首个请求已进入下游并阻塞、其余并发请求确实堆叠在幂等等待上之后再统一点放行。
- 下游故障通过替身显式注入 `DownstreamError` / `DownstreamTimeout`，
  避免依赖真实网络与超时。
