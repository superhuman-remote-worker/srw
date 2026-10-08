"""``srw.kb/v1``: an OKF Knowledge Base repository SRW indexes centrally.

The agent never sees the repository: its binding carries the index id, the
display metadata and the normalized config, read-only, with no URL and no
credentials.  Every write that can change what is indexed marks the index
pending and schedules a full rebuild; delete goes through the index fence
(``orchestrator.services.knowledge_index``); index status and reindex are
the driver's operations.

A project's own KB row (``config.native_project_id``) is a management
surface for the project's vault: it has no repository to validate and is
never indexed under its datasource id.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from uuid import UUID

from fastapi import HTTPException

# Called through the module so a patched function is the one that runs.
from orchestrator.services import knowledge_index as kb_index
from orchestrator.services.connector_drivers import knowledge_note
from orchestrator.services.connector_drivers.base import (
    BindContext,
    CheckContext,
    ConnectorDraft,
    DatasourceDriver,
    NormalizedConnector,
    ValidationContext,
    payload_entry,
    probe_failure,
)
from orchestrator.services.datasource_config import (
    normalize_kb_config,
    validate_kb_repository_auth,
    validate_kb_repository_url,
)
from shared.connectors.builtin import KB_SPEC
from shared.native_kb import NATIVE_PROJECT_CONFIG_KEY, native_kb_project_id


class KnowledgeBaseDriver(DatasourceDriver):
    def __init__(self) -> None:
        super().__init__(KB_SPEC)

    def knowledge_note(self, row: Mapping[str, Any]) -> str:
        return knowledge_note.knowledge_base_note(row)

    def retrieval_messages(self, row: Mapping[str, Any]) -> list[str]:
        return knowledge_note.knowledge_base_phrases(row)

    async def validate(
        self,
        draft: ConnectorDraft,
        *,
        existing: Mapping[str, Any] | None,
        ctx: ValidationContext,
    ) -> NormalizedConnector:
        if draft.read_only is False:
            raise HTTPException(
                status_code=400,
                detail="Knowledge-base connectors are always read-only",
            )
        if existing is None:
            connection_url = validate_kb_repository_url(draft.connection_url)
            config = normalize_kb_config(draft.config)
            credentials = self.stored_credentials(draft, existing)
            validate_kb_repository_auth(connection_url, credentials)
            return NormalizedConnector(connection_url, config, credentials)

        credentials = self.stored_credentials(draft, existing)
        config = draft.config
        native_project = native_kb_project_id(existing)
        if native_project:
            # No repository to validate and nothing to rebuild. Config still
            # goes through the normalizer (unknown keys stay rejected) and the
            # server-owned marker is re-attached, so editing the root path
            # cannot quietly promote the vault into the external sweep and
            # index every note a second time.
            if config is not None:
                config = normalize_kb_config(config)
                config[NATIVE_PROJECT_CONFIG_KEY] = native_project
            # Nothing to point at; leave the column alone.
            return NormalizedConnector(None, config, credentials)

        connection_url = draft.connection_url
        reindex_required = False
        if connection_url is not None:
            connection_url = validate_kb_repository_url(connection_url)
            reindex_required = (
                connection_url != str(existing.get("connection_url") or "").strip()
            )
        if config is not None:
            config = normalize_kb_config(config)
            reindex_required = reindex_required or (
                config != normalize_kb_config(existing.get("config"))
            )
        if credentials is not None:
            reindex_required = reindex_required or (
                credentials != (existing.get("credentials") or {})
            )
        if draft.default_branch is not None:
            reindex_required = reindex_required or (
                (draft.default_branch or None)
                != (existing.get("default_branch") or None)
            )
        validate_kb_repository_auth(
            connection_url or str(existing.get("connection_url") or ""),
            credentials
            if credentials is not None
            else (existing.get("credentials") or {}),
        )
        return NormalizedConnector(
            connection_url, config, credentials, reindex_required=reindex_required
        )

    async def after_write(
        self,
        datasource_id: str,
        normalized: NormalizedConnector,
        *,
        created: bool,
        knowledge_index: kb_index.KnowledgeIndexDependencies,
    ) -> None:
        """A create, or an edit that changes what is indexed, rebuilds it."""
        if not (created or normalized.reindex_required):
            return
        await kb_index.mark_kb_datasource_pending(
            datasource_id, dependencies=knowledge_index
        )
        kb_index.schedule_kb_datasource_reindex(
            datasource_id, force_full=True, dependencies=knowledge_index
        )

    async def check(
        self, row: Mapping[str, Any], credentials: dict[str, Any], *, ctx: CheckContext
    ) -> dict[str, Any]:
        from orchestrator.services.kb_datasources import test_kb_datasource

        try:
            return await test_kb_datasource(row)
        except Exception:
            return probe_failure("Knowledge base probe failed", self.type_id)

    def effective_access(self, row: Mapping[str, Any]) -> str | None:
        return "ReadOnly"

    def bind(
        self, row: Mapping[str, Any], credentials: Any, *, ctx: BindContext
    ) -> dict[str, Any] | None:
        return payload_entry(
            row,
            credentials={},
            read_only=True,
            connection_url=None,
            fields={
                "datasource_id": str(row["id"]),
                # stored=True keeps the native-project marker: the agent's
                # binding builder collapses a project's own KB row into the
                # writable native binding instead of adding a second,
                # read-only binding for the same notes.
                "config": normalize_kb_config(row.get("config"), stored=True),
            },
        )

    # -- index operations ---------------------------------------------------

    async def delete_with_index(
        self,
        datasource_id: str,
        *,
        authority_project_scope_id: str | None,
        deleted_by: str,
        knowledge_index: kb_index.KnowledgeIndexDependencies,
    ) -> bool:
        return await kb_index.delete_kb_datasource_with_index(
            datasource_id,
            authority_project_scope_id=authority_project_scope_id,
            deleted_by=deleted_by,
            dependencies=knowledge_index,
        )

    async def index_status(
        self, row: Mapping[str, Any], datasource_id: str, *, vector_db: Any
    ) -> dict[str, Any]:
        """Credential-free indexing state."""
        from shared.runtime.services.knowledge_store import KnowledgeStore

        from orchestrator.services.kb_datasources import index_status_payload

        watermark_id = native_kb_project_id(row) or datasource_id
        watermark = await KnowledgeStore(
            db=vector_db, embedding_service=None
        ).get_watermark(UUID(watermark_id))
        return index_status_payload(datasource_id, watermark)

    async def reindex(
        self,
        row: Mapping[str, Any],
        *,
        full: bool,
        knowledge_index: kb_index.KnowledgeIndexDependencies,
    ) -> dict[str, Any]:
        if native_kb_project_id(row):
            # Indexing it here would write its project's notes a second time
            # under this datasource's id and duplicate every search hit.
            raise HTTPException(
                status_code=400,
                detail=(
                    "This connector mirrors the project's own knowledge base; "
                    "reindex it from the project instead"
                ),
            )
        return await kb_index.reindex_kb_datasource_now(
            row, force_full=full, dependencies=knowledge_index
        )
