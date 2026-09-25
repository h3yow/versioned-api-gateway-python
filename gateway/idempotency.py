"""写请求幂等与并发去重。

语义：
- 同一 idempotency_key 的并发重复提交，只有一个请求真正进入下游；
  其余请求阻塞等待，完成后拿到首次结果（重放）。
- 同一 key 携带不同载荷：抛出 IDEMPOTENCY_CONFLICT，明确拒绝而非静默覆盖。
- 首次请求成功 -> 重放返回首次成功结果。
- 首次请求失败（下游异常/超时）：唤醒等待者返回同一可区分错误，
  随后移除记录，允许客户端之后用同键重试。
- 已完成记录超过 TTL 自动过期。
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from enum import Enum

from .clock import Clock
from .errors import ErrorCode, GatewayError


class RequestState(str, Enum):
    IN_FLIGHT = "in_flight"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class StoredRequest:
    key: str
    payload_hash: str
    state: RequestState
    result: dict | None = None
    error_code: str | None = None
    stored_at: float = 0.0


def hash_payload(payload: dict) -> str:
    """对载荷做稳定哈希（键排序、分隔符防拼接歧义）。"""
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return f"sha256:{__import__('hashlib').sha256(canonical.encode('utf-8')).hexdigest()}"


class IdempotencyStore:
    """幂等记录存储；同一 key 并发只允许一个请求进入下游。"""

    def __init__(self, clock: Clock, ttl_seconds: float = 3600.0) -> None:
        self._clock = clock
        self._ttl = ttl_seconds
        self._records: dict[str, StoredRequest] = {}
        self._conditions: dict[str, threading.Condition] = {}
        self._lock = threading.Lock()

    def _condition_for(self, key: str) -> threading.Condition:
        cond = self._conditions.get(key)
        if cond is None:
            cond = threading.Condition(self._lock)
            self._conditions[key] = cond
        return cond

    def begin(self, key: str, payload_hash: str) -> StoredRequest:
        """登记请求。

        返回 state == IN_FLIGHT 表示本线程获得下游执行权；
        返回已 COMPLETED/FAILED 的记录表示应重放其结果；
        载荷不一致时抛出 IDEMPOTENCY_CONFLICT。
        """
        with self._lock:
            cond = self._condition_for(key)
            existing = self._records.get(key)
            now = self._clock.now()

            if existing is not None:
                # 已完成记录过期则视为不存在。
                expired = (
                    existing.state is RequestState.COMPLETED
                    and now - existing.stored_at >= self._ttl
                )
                if expired:
                    existing = None
                    self._records.pop(key, None)

            if existing is None:
                record = StoredRequest(
                    key=key,
                    payload_hash=payload_hash,
                    state=RequestState.IN_FLIGHT,
                    stored_at=now,
                )
                self._records[key] = record
                return record

            if existing.payload_hash != payload_hash:
                raise GatewayError(
                    ErrorCode.IDEMPOTENCY_CONFLICT,
                    "幂等标识已被用于不同的请求载荷，拒绝处理以防静默覆盖",
                )

            # 同键同载荷：等待首个请求结束后重放。
            while existing.state is RequestState.IN_FLIGHT:
                cond.wait()
            return existing

    def complete(self, key: str, result: dict) -> None:
        """由首个执行线程记录成功结果并唤醒等待者。"""
        with self._lock:
            record = self._records.get(key)
            if record is None:
                return
            record.state = RequestState.COMPLETED
            record.result = result
            record.stored_at = self._clock.now()
            self._condition_for(key).notify_all()

    def fail(self, key: str, error_code: str) -> None:
        """由首个执行线程记录失败、唤醒等待者，随后移除记录允许重试。"""
        with self._lock:
            record = self._records.get(key)
            if record is None:
                return
            record.state = RequestState.FAILED
            record.error_code = error_code
            self._condition_for(key).notify_all()
            self._records.pop(key, None)
