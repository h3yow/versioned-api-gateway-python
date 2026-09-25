"""接口版本协商与废弃策略。

规则：
- 客户端必须显式声明版本（请求头 X-API-Version）；缺失 -> version_missing。
- 声明了系统从未发布过的版本 -> version_unknown。
- 声明的版本已下线（sunset）-> version_sunset，并在消息中提示继任版本。
- 废弃中（deprecated）的版本仍可调用，但响应会带废弃告警与继任提示；
  新版本上线不得修改既有版本的响应结构（渲染按版本分别实现，见 core.render_response）。
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from .errors import ErrorCode, GatewayError


class VersionStatus(str, Enum):
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    SUNSET = "sunset"


@dataclass(frozen=True)
class VersionSpec:
    version: str
    status: VersionStatus
    successor: str | None = None


# 能力 -> {版本字符串: 版本规格}
VERSIONS: dict[str, dict[str, VersionSpec]] = {}


def register_versions(capability: str, specs: list[VersionSpec]) -> None:
    """注册某业务能力支持的版本列表。"""
    VERSIONS[capability] = {spec.version: spec for spec in specs}


def negotiate(capability: str, requested_version: str | None) -> VersionSpec:
    """协商版本。

    成功返回 VersionSpec（ACTIVE 或 DEPRECATED 都放行）；
    缺失/未知/已下线分别抛出可区分的 GatewayError。
    """
    if requested_version is None or not requested_version.strip():
        raise GatewayError(
            ErrorCode.VERSION_MISSING,
            "缺少 API 版本声明，请通过 X-API-Version 请求头指定版本",
        )

    version = requested_version.strip()
    table = VERSIONS.get(capability)
    spec = table.get(version) if table is not None else None
    if spec is None:
            raise GatewayError(
                ErrorCode.VERSION_UNKNOWN,
                f"未知的 API 版本: {version}，请使用当前受支持的版本",
            )

    if spec.status is VersionStatus.SUNSET:
        hint = f"，请升级到继任版本 {spec.successor}" if spec.successor else ""
        raise GatewayError(
            ErrorCode.VERSION_SUNSET,
            f"API 版本 {version} 已下线，不再提供服务{hint}",
        )

    return spec
