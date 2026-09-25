"""网关编排：版本协商 -> 配额 -> 幂等去重 -> 下游 -> 版本化响应。

判定依据（reason）贯穿每次处理，供日志解释最终结果；
下游异常/超时在此统一转换，绝不把内部异常细节透传给调用方。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from .clock import Clock
from .downstream import (
    DownstreamError,
    DownstreamService,
    DownstreamTimeout,
)
from .errors import ErrorCode, GatewayError
from .idempotency import (
    RequestState,
    hash_payload,
)
from .idempotency import IdempotencyStore
from .quota import Caller, QuotaManager
from .versions import (
    VersionSpec,
    VersionStatus,
    negotiate,
    register_versions,
)

logger = logging.getLogger("gateway")

# 默认下游执行期限（秒），超时转换为 DOWNSTREAM_TIMEOUT。
DEFAULT_DEADLINE_SECONDS = 2.0


@dataclass
class GatewayRequest:
    capability: str
    version: str | None
    payload: dict
    idempotency_key: str | None = None
    method: str = "POST"  # POST 为写请求，参与幂等；GET 为读请求


@dataclass
class GatewayResponse:
    status: str            # "ok" / "deprecated"
    version: str
    body: dict
    reason: str            # 最终判定依据，供日志与可解释性


def _register_default_versions() -> None:
    register_versions(
        "orders",
        [
            VersionSpec("2024-01-01", VersionStatus.SUNSET, successor="2025-01-01"),
            VersionSpec("2025-01-01", VersionStatus.DEPRECATED, successor="2026-01-01"),
            VersionSpec("2026-01-01", VersionStatus.ACTIVE),
        ],
    )


_register_default_versions()


def render_response(version: str, internal: dict) -> dict:
    """按协商版本把版本无关的内部结果渲染为对应结构。

    每个版本独立渲染，新增版本不修改既有版本的输出，保证向后兼容。
    """
    if version == "2025-01-01":
        # 旧版：平铺结构，qty 为字符串（保留历史行为，不得改动）。
        return {
            "orderId": internal["order_id"],
            "item": internal["item"],
            "qty": str(internal["qty"]),
            "state": internal["status"],
        }
    if version == "2026-01-01":
        # 新版：嵌套 data + 数值型数量。
        return {
            "data": {
                "order_id": internal["order_id"],
                "item": internal["item"],
                "qty": internal["qty"],
                "status": internal["status"],
            }
        }
    # 未知版本不应到达这里（协商阶段已拒绝）；保底返回内部结果。
    return dict(internal)


class Gateway:
    def __init__(
        self,
        clock: Clock,
        quota_manager: QuotaManager,
        idempotency_store: IdempotencyStore,
        downstream: DownstreamService,
        deadline_seconds: float = DEFAULT_DEADLINE_SECONDS,
    ) -> None:
        self._clock = clock
        self._quota = quota_manager
        self._idempotency = idempotency_store
        self._downstream = downstream
        self._deadline = deadline_seconds

    # ---- 内部步骤 -------------------------------------------------------

    def _check_quota(self, caller: Caller) -> None:
        retry_after = self._quota.acquire(caller)
        if retry_after is not None:
            raise GatewayError(
                ErrorCode.QUOTA_EXCEEDED,
                (
                    f"调用方 {caller.caller_id}（{caller.tier.value}）配额已耗尽，"
                    f"请在 {retry_after:.3f} 秒后重试"
                ),
                retry_after=retry_after,
            )

    def _invoke_downstream(self, payload: dict) -> dict:
        """调用下游并把内部异常统一转换为可区分错误，不泄漏细节。"""
        started = self._clock.now()
        try:
            result = self._downstream.handle(payload)
        except DownstreamTimeout:
            raise GatewayError(
                ErrorCode.DOWNSTREAM_TIMEOUT,
                "下游服务处理超时，请稍后重试",
            )
        except DownstreamError:
            raise GatewayError(
                ErrorCode.DOWNSTREAM_ERROR,
                "下游服务处理失败，请稍后重试",
            )
        except Exception:  # noqa: BLE001 - 兜底：绝不泄漏内部异常细节
            raise GatewayError(
                ErrorCode.DOWNSTREAM_ERROR,
                "下游服务处理失败，请稍后重试",
            )
        elapsed = self._clock.now() - started
        if elapsed > self._deadline:
            raise GatewayError(
                ErrorCode.DOWNSTREAM_TIMEOUT,
                "下游服务处理超时，请稍后重试",
            )
        return result

    def _build_response(
        self, spec: VersionSpec, internal: dict, reason: str
    ) -> GatewayResponse:
        body = render_response(spec.version, internal)
        status = "ok"
        if spec.status is VersionStatus.DEPRECATED:
            status = "deprecated"
            body["warning"] = (
                f"版本 {spec.version} 已废弃"
                + (f"，请升级到 {spec.successor}" if spec.successor else "")
            )
        return GatewayResponse(
            status=status, version=spec.version, body=body, reason=reason
        )

    # ---- 对外入口 -------------------------------------------------------

    def handle(self, caller: Caller, request: GatewayRequest) -> GatewayResponse:
        try:
            return self._handle(caller, request)
        except GatewayError as exc:
            # 统一记录最终拒绝判定（含版本协商/配额阶段的早期拒绝）。
            self._log_rejection(caller, request, exc.code.value)
            raise

    def _handle(self, caller: Caller, request: GatewayRequest) -> GatewayResponse:
        # 1) 版本协商（缺失/未知/已下线在此被可区分地拒绝）。
        spec = negotiate(request.capability, request.version)

        # 2) 配额（按调用方身份独立口径）。
        self._check_quota(caller)

        # 3) 读请求：直接下游处理。
        if request.method.upper() != "POST" or not request.idempotency_key:
            internal = self._invoke_downstream(request.payload)
            reason = "非幂等请求，直接执行下游"
            resp = self._build_response(spec, internal, reason)
            self._log(caller, spec, request.idempotency_key, resp)
            return resp

        # 4) 写请求：幂等去重。
        key = request.idempotency_key
        payload_hash = hash_payload(request.payload)
        record = self._idempotency.begin(key, payload_hash)

        if record.state is RequestState.IN_FLIGHT:
            # 本线程获得首次执行权。
            try:
                internal = self._invoke_downstream(request.payload)
            except GatewayError as exc:
                # 记录失败原因供并发等待者重放；拒绝日志由外层 handle 统一打印。
                self._idempotency.fail(key, exc.code.value)
                raise
            self._idempotency.complete(key, internal)
            reason = "首次提交，已执行下游并记录结果"
            resp = self._build_response(spec, internal, reason)
            self._log(caller, spec, key, resp)
            return resp

        # 并发/后续重放：复用首次结果（成功或失败）。
        if record.state is RequestState.COMPLETED and record.result is not None:
            reason = "重复提交，重放首次成功结果（下游仅执行一次）"
            resp = self._build_response(spec, record.result, reason)
            self._log(caller, spec, key, resp)
            return resp

        # 首次执行失败：返回与首次一致的可区分错误，并允许之后重试。
        # 拒绝判定由外层 handle 统一记录日志。
        code_value = record.error_code or ErrorCode.DOWNSTREAM_ERROR.value
        raise GatewayError(
            ErrorCode(code_value),
            "首次请求处理失败，本次重放返回相同结果，可稍后重试",
        )

    @staticmethod
    def _emit(
        caller: Caller,
        version: str,
        idempotency_key: str | None,
        decision: str,
    ) -> None:
        logger.info(
            "调用方=%s 身份=%s 版本=%s 幂等键=%s 判定=%s",
            caller.caller_id,
            caller.tier.value,
            version,
            idempotency_key or "-",
            decision,
        )

    def _log_rejection(
        self, caller: Caller, request: GatewayRequest, code_value: str
    ) -> None:
        self._emit(
            caller,
            request.version or "(缺失)",
            request.idempotency_key,
            f"拒绝:{code_value}",
        )

    @staticmethod
    def _log(
        caller: Caller,
        spec: VersionSpec,
        idempotency_key: str | None,
        response: GatewayResponse,
    ) -> None:
        Gateway._emit(caller, spec.version, idempotency_key, response.reason)
