"""可控时钟，用于配额窗口与超时判定（骨架）。"""

from __future__ import annotations

import time
from typing import Protocol


class Clock(Protocol):
    """时钟协议，测试中可替换为可控时钟。"""

    def now(self) -> float:
        ...

    def sleep(self, seconds: float) -> None:
        ...


class SystemClock:
    """系统真实时钟。"""

    def now(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


class FakeClock:
    """可控虚拟时钟，测试中手动推进时间。"""

    def __init__(self, start: float = 0.0) -> None:
        self._now = start

    def now(self) -> float:
        return self._now

    def sleep(self, seconds: float) -> None:
        self._now += seconds

    def advance(self, seconds: float) -> None:
        self._now += seconds
