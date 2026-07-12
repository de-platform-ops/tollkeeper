from __future__ import annotations

import logging

from tollkeeper.backends.base import Backend

logger = logging.getLogger(__name__)


class SqlPassthroughBackend(Backend):
    """Backend for standard SQL tables with no physical versioning.

    Writes go directly to the production table. Tollkeeper still enforces
    the audit/signal lifecycle, but isolation is orchestration-level only
    (downstream sensors gate on the signal, not on a physical branch).
    """

    def create_version(self, table: str) -> str:
        return table

    def publish_version(self, table: str, version_ref: str) -> None:
        pass

    def rollback_version(self, table: str, version_ref: str) -> None:
        logger.warning(
            "SqlPassthroughBackend cannot undo writes to '%s'. "
            "Data is already in the table. The DAG will halt, "
            "preventing downstream signal emission.",
            table,
        )
