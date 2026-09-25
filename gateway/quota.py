"""基于调用方身份的限流与配额。

口径说明：
- 匿名(anonymous)、普通(normal)、高权限(privileged)三档使用各自独立的
  (limit, window) 策略；计数以调用方 ID 为键，不同调用方互不影响。
- 固定时间窗口计数；窗口滑动后计数清零。
- acquire 在锁内完成"读窗口 -> 判定 -> 计数"，并发请求不会少计或重复计数。
- 超限时返回 retry_after（距当前窗口重置的剩余秒数，下限为正），
  供调用方做可解释的退避重试。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from enum import Enum

from .clock import Clock


class CallerTier(str, Enum):
    ANONYMOUS = "anonymous"
    NORMAL = "normal"
    PRIVILEGED = "privileged"


@dataclass(frozen=True)
class Caller:
    caller_id: str
    tier: CallerTier


@dataclass(frozen=True)
class QuotaPolicy:
    limit: int          # 窗口内允许的请求数
    window_seconds: float


# 各身份独立的默认配额口径。
DEFAULT_POLICIES: dict[CallerTier, QuotaPolicy] = {
    CallerTier.ANONYMOUS: QuotaPolicy(limit=2, window_seconds=60.0),
    CallerTier.NORMAL: QuotaPolicy(limit=10, window_seconds=60.0),
    CallerTier.PRIVILEGED: QuotaPolicy(limit=100, window_seconds=60.0),
}


@dataclass
class _Window:
    started_at: float
    count: int = 0


class QuotaManager:
    """固定窗口配额计数器，按调用方独立口径，加锁保证并发计数准确。"""

    def __init__(self, policies: dict[CallerTier, QuotaPolicy], clock: Clock) -> None:
        self._policies = dict(policies)
        self._clock = clock
        self._windows: dict[str, _Window] = {}
        self._lock = threading.Lock()

    def acquire(self, caller: Caller) -> float | None:
        """尝试占用一个配额。

        成功返回 None；超限返回建议重试等待秒数（窗口剩余时间）。
        """
        policy = self._policies[caller.tier]
        now = self._clock.now()
        with self._lock:
            window = self._windows.get(caller.caller_id)
            if window is None or now - window.started_at >= policy.window_seconds:
                window = _Window(started_at=now, count=0)
                self._windows[caller.caller_id] = window

            if window.count >= policy.limit:
                remaining = policy.window_seconds - (now - window.started_at)
                return max(remaining, 0.001)

            window.count += 1
            return None
