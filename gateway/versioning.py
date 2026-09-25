"""API version negotiation and sunset policy."""

from __future__ import annotations

import enum
from dataclasses import dataclass

from .errors import ErrorCode, GatewayError


class VersionStatus(enum.Enum):
    ACTIVE = "active"
    DEPRECATED = "deprecated"  # still served, but flagged
    SUNSET = "sunset"  # no longer served


@dataclass(frozen=True)
class VersionInfo:
    name: str
    status: VersionStatus


@dataclass(frozen=True)
class NegotiatedVersion:
    info: VersionInfo
    deprecated: bool


class VersionRegistry:
    """Registry of supported versions; negotiation entry point."""

    def __init__(self, versions: list[VersionInfo]) -> None:
        self._versions = {v.name: v for v in versions}

    def negotiate(self, requested: str | None) -> NegotiatedVersion | GatewayError:
        """Resolve the client-declared version to a servable version.

        Distinguishable rejections:
        - VERSION_MISSING: client did not declare a version.
        - VERSION_UNKNOWN: version was never registered.
        - VERSION_SUNSET: version existed but has been taken offline.
        """
        if requested is None or requested.strip() == "":
            return GatewayError(
                code=ErrorCode.VERSION_MISSING,
                message="Missing API version. Declare one via the 'X-API-Version' header.",
            )
        info = self._versions.get(requested)
        if info is None:
            return GatewayError(
                code=ErrorCode.VERSION_UNKNOWN,
                message=f"Unknown API version '{requested}'.",
            )
        if info.status is VersionStatus.SUNSET:
            return GatewayError(
                code=ErrorCode.VERSION_SUNSET,
                message=f"API version '{requested}' has been sunset and is no longer served.",
            )
        return NegotiatedVersion(info=info, deprecated=info.status is VersionStatus.DEPRECATED)
