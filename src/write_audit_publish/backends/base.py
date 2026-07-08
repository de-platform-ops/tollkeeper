from __future__ import annotations

from abc import ABC, abstractmethod


class Backend(ABC):
    """Abstract base for WAP storage backends.

    A backend manages the lifecycle of a versioned table: creating an isolated
    staging area, promoting it to production, or rolling it back. Implement this
    to add support for a new storage layer (CSV files, Iceberg, Delta, etc.).
    """

    @abstractmethod
    def create_version(self, table: str) -> str:
        """Create an isolated staging version and return a version reference.

        The reference is backend-specific: a file path, branch name, snapshot ID, etc.
        The caller writes data to whatever the reference points at.

        Args:
            table: Logical table name (e.g. ``"sales"``).

        Returns:
            An opaque version reference string passed to subsequent methods.
        """

    @abstractmethod
    def publish_version(self, table: str, version_ref: str) -> None:
        """Promote the staged version to production.

        Args:
            table: Logical table name.
            version_ref: Reference returned by ``create_version``.
        """

    @abstractmethod
    def rollback_version(self, table: str, version_ref: str) -> None:
        """Discard a staged version without publishing.

        Args:
            table: Logical table name.
            version_ref: Reference returned by ``create_version``.
        """
