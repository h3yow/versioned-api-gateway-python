"""API version negotiation and sunset policy."""

from __future__ import annotations

import enum
from dataclasses import dataclass

from .errors import GatewayError


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
        raise NotImplementedError
