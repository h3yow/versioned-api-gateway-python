"""场景四：下游异常或超时的统一错误转换。

覆盖：
- 下游业务异常 -> DOWNSTREAM_ERROR(502 语义)，不泄漏内部异常文本；
- 下游超时 / 超出执行期限 -> DOWNSTREAM_TIMEOUT(504 语义)；
- 非预期异常也被兜底转换，不传播内部堆栈；
- 并发下首次失败，所有重放返回同一可区分错误（见 test_concurrency 互补用例）。
"""

from __future__ import annotations

import threading

import pytest

from gateway.core import GatewayRequest
from gateway.downstream import DownstreamError, DownstreamTimeout
from gateway.errors import ErrorCode, GatewayError

from .fakes import FakeDownstream

CALLER_ID = "user-d"
from gateway.quota import Caller, CallerTier

CALLER = Caller(caller_id=CALLER_ID, tier=CallerTier.PRIVILEGED)
PAYLOAD = {"item": "book", "qty": 1}


def _post(gateway, key):
    return gateway.handle(
        CALLER,
        GatewayRequest("orders", "2026-01-01", PAYLOAD, idempotency_key=key),
    )


def test_downstream_error_is_mapped_without_internal_detail(make_gateway):
    gateway, ds = make_gateway(FakeDownstream(fail=DownstreamError))
    with pytest.raises(GatewayError) as ei:
        _post(gateway, "d-1")
    assert ei.value.code is ErrorCode.DOWNSTREAM_ERROR
    assert "injected error" not in ei.value.message
    assert "DownstreamError" not in ei.value.message
    assert ds.calls == 1


def test_downstream_timeout_is_distinct(make_gateway):
    gateway, _ = make_gateway(FakeDownstream(timeout=True))
    with pytest.raises(GatewayError) as ei:
        _post(gateway, "d-2")
    assert ei.value.code is ErrorCode.DOWNSTREAM_TIMEOUT
    assert ei.value.message


def test_unexpected_exception_is_sanitized(make_gateway):
    class Boom(RuntimeError):
        pass

    gateway, ds = make_gateway(FakeDownstream(fail=Boom("db password=secret")))
    with pytest.raises(GatewayError) as ei:
        _post(gateway, "d-3")
    assert ei.value.code is ErrorCode.DOWNSTREAM_ERROR
    assert "secret" not in ei.value.message
    assert "db password" not in ei.value.message
    assert ds.calls == 1


def test_concurrent_failure_replays_same_distinct_error(make_gateway):
    downstream = FakeDownstream(block=True, timeout=True)
    gateway, ds = make_gateway(downstream)
    n = 4
    box: list = []
    lock = threading.Lock()

    def worker():
        try:
            _post(gateway, "d-4")
            with lock:
                box.append(("ok", None))
        except GatewayError as exc:
            with lock:
                box.append(("err", exc))

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    assert downstream.entered.wait(timeout=2.0)
    downstream.release.set()
    for t in threads:
        t.join(timeout=2.0)

    assert ds.calls == 1
    assert len(box) == n
    assert all(kind == "err" and exc.code is ErrorCode.DOWNSTREAM_TIMEOUT
               for kind, exc in box)
