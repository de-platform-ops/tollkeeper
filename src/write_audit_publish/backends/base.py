from __future__ import annotations

from abc import ABC, abstractmethod


class Backend(ABC):
    @abstractmethod
    def create_version(self, table: str) -> str:
        """Create an isolated staging version. Returns a version reference (branch name, snapshot ID, etc.) the user's operator writes to."""

    @abstractmethod
    def publish_version(self, table: str, version_ref: str) -> None:
        """Promote the staged version to production (pointer swap)."""

    @abstractmethod
    def rollback_version(self, table: str, version_ref: str) -> None:
        """Revert a published version."""
