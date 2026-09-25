"""场景一：同一幂等标识的并发重复提交只能生效一次。

确定性手段：FakeDownstream 用 Event 阻塞首个进入下游的请求，
其余并发请求必然堆叠在幂等等待上，再由测试线程统一点放行。
"""

from __future__ import annotations

import threading

import pytest

from gateway.core import GatewayRequest
from gateway.downstream import DownstreamError
from gateway.errors import ErrorCode, GatewayError
from gateway.quota import Caller, CallerTier

from .fakes import FakeDownstream

CALLER = Caller(caller_id="user-1", tier=CallerTier.PRIVILEGED)
PAYLOAD = {"item": "book", "qty": 2}
VERSION = "2026-01-01"


def _post(gateway, key, payload, box, idx):
    try:
        resp = gateway.handle(
            CALLER,
            GatewayRequest("orders", VERSION, payload, idempotency_key=key, method="POST"),
        )
        box[idx] = ("ok", resp)
    except GatewayError as exc:
        box[idx] = ("err", exc)


def _start_concurrent(gateway, key, payload, n):
    box: list = [None] * n
    threads = [
        threading.Thread(target=_post, args=(gateway, key, payload, box, i))
        for i in range(n)
    ]
    for t in threads:
        t.start()
    return threads, box


def test_concurrent_duplicate_submission_executes_once(make_gateway):
    downstream = FakeDownstream(block=True)
    gateway, ds = make_gateway(downstream)
    n = 8

    threads, box = _start_concurrent(gateway, "idem-100", PAYLOAD, n)
    assert downstream.entered.wait(timeout=2.0), "首个请求应进入下游"
    # 放行后，首个请求完成，其余 7 个等待者被唤醒并拿到同一结果。
    downstream.release.set()
    for t in threads:
        t.join(timeout=2.0)

    assert ds.calls == 1, "下游只应被执行一次"
    responses = [item for item in box if item and item[0] == "ok"]
    assert len(responses) == n
    order_ids = {item[1].body["data"]["order_id"] for item in responses}
    assert order_ids == {"ord-000001"}, "所有重放必须返回首次结果"
    reasons = [item[1].reason for item in responses]
    assert sum("首次提交" in r for r in reasons) == 1
    assert sum("重放" in r for r in reasons) == n - 1


def test_same_key_different_payload_is_rejected(make_gateway):
    gateway, ds = make_gateway(FakeDownstream())

    first = gateway.handle(
        CALLER,
        GatewayRequest("orders", VERSION, PAYLOAD, idempotency_key="idem-200"),
    )
    assert first.status == "ok"

    with pytest.raises(GatewayError) as exc_info:
        gateway.handle(
            CALLER,
            GatewayRequest(
                "orders", VERSION, {"item": "pen", "qty": 9}, idempotency_key="idem-200"
            ),
        )
    assert exc_info.value.code is ErrorCode.IDEMPOTENCY_CONFLICT
    assert ds.calls == 1, "载荷冲突不得触发第二次下游调用"


def test_concurrent_failure_replays_error_then_allows_retry(make_gateway):
    # 仅第一次（首个执行线程）抛下游异常，之后恢复成功。
    downstream = FakeDownstream(block=True, fail=DownstreamError, fail_times=1)
    gateway, ds = make_gateway(downstream)
    n = 3

    threads, box = _start_concurrent(gateway, "idem-300", PAYLOAD, n)
    assert downstream.entered.wait(timeout=2.0)
    downstream.release.set()
    for t in threads:
        t.join(timeout=2.0)

    errors = [item for item in box if item and item[0] == "err"]
    assert len(errors) == n
    assert all(exc.code is ErrorCode.DOWNSTREAM_ERROR for _, exc in errors)
    assert ds.calls == 1

    # 首次失败后记录移除：同键同载荷允许重新执行。
    retry = gateway.handle(
        CALLER,
        GatewayRequest("orders", VERSION, PAYLOAD, idempotency_key="idem-300"),
    )
    assert retry.status == "ok"
    assert ds.calls == 2
