"""测试用可控下游替身。

通过 Event 让首个进入下游的请求阻塞，测试可精确制造并发窗口；
可注入 DownstreamError / DownstreamTimeout，并统计真实调用次数。
"""

from __future__ import annotations

import itertools
import threading

from gateway.downstream import DownstreamError, DownstreamTimeout


class FakeDownstream:
    def __init__(
        self,
        *,
        fail: Exception | type[Exception] | None = None,
        fail_times: int = 10**9,
        block: bool = False,
        timeout: bool = False,
        timeout_times: int = 10**9,
    ) -> None:
        self._fail = fail
        self._fail_times = fail_times
        self._block = block
        self._timeout = timeout
        self._timeout_times = timeout_times
        self._counter = itertools.count(1)
        self.calls = 0
        self._lock = threading.Lock()
        self.entered = threading.Event()
        self.release = threading.Event()

    def handle(self, payload: dict) -> dict:
        with self._lock:
            self.calls += 1
            call_no = self.calls
        self.entered.set()
        if self._block:
            # 等待测试线程显式放行，保证并发请求确实堆叠在幂等等待上。
            self.release.wait(timeout=5.0)
        if self._timeout and call_no <= self._timeout_times:
            raise DownstreamTimeout("injected timeout (internal detail)")
        if self._fail is not None and call_no <= self._fail_times:
            if isinstance(self._fail, Exception):
                raise self._fail
            raise DownstreamError("injected error (internal detail)")
        return {
            "order_id": f"ord-{next(self._counter):06d}",
            "item": str(payload.get("item", "unknown")),
            "qty": int(payload.get("qty", 1)),
            "status": "created",
        }
