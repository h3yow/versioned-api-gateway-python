"""统一错误码与错误结果定义（骨架）。"""

from __future__ import annotations

from enum import Enum


class ErrorCode(str, Enum):
    """对外可区分的拒绝/失败原因。"""

    VERSION_MISSING = "version_missing"
    VERSION_UNKNOWN = "version_unknown"
    VERSION_SUNSET = "version_sunset"
    QUOTA_EXCEEDED = "quota_exceeded"
    IDEMPOTENCY_CONFLICT = "idempotency_conflict"
    DOWNSTREAM_ERROR = "downstream_error"
    DOWNSTREAM_TIMEOUT = "downstream_timeout"


class GatewayError(Exception):
    """网关统一错误：携带错误码与对外说明，不泄漏内部细节。"""

    def __init__(self, code: ErrorCode, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.retry_after = retry_after
