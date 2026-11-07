"""Knowledge search, mutation and projection request contracts."""

from typing import Literal

from pydantic import BaseModel, Field


class KnowledgeSearchRequest(BaseModel):
    """Request body for hybrid knowledge search."""

    query: str = Field(..., description="Search query text")
    limit: int = Field(10, ge=1, le=50, description="Max results to return")


class KnowledgeNoteUpdate(BaseModel):
    """Request body for updating a knowledge note."""

    status: str | None = Field(
        None, description="New status: active, resolved, superseded, archived"
    )
    add_tags: list[str] | None = Field(None, description="Tags to add")
    remove_tags: list[str] | None = Field(None, description="Tags to remove")


class KnowledgeMaterializeRequest(BaseModel):
    """Request body for materialising one note into the project's KB repo."""

    slug: str = Field(
        ..., description="Note id — becomes knowledge/<slug>.md in the KB repo"
    )
    content: str = Field(..., description="Fully rendered OKF note markdown")
    job_id: str | None = Field(
        None, description="Writing job UUID, for per-job commit attribution"
    )
    expected_blob_sha: str | None = Field(
        None,
        description=(
            "Compare-and-swap token: the note's blob SHA as the caller read it. "
            "When set, the write is refused (failed/precondition-failed) if the "
            "KB repo holds a different blob — or none — at the path."
        ),
    )
    retrieval_messages: list[str] | None = Field(
        None,
        description=(
            "Synthetic retrieval queries for this note. Omitted/None leaves "
            "any already-indexed value alone — OKF frontmatter carries no "
            "such field, so only an explicit caller has an opinion."
        ),
    )
    mode: Literal["create", "update"] | None = Field(
        None,
        description=(
            "'create' refuses (failed/already-exists) when the note already "
            "exists anywhere in the vault, so a create never overwrites; "
            "'update' refuses (failed/precondition-failed) when it is gone. "
            "Omitted: the historical upsert. Either way an existing note is "
            "written where it lives, which may be a subfolder."
        ),
    )


class KnowledgeDeleteRequest(BaseModel):
    """Request body for the internal purge of one note (agent/purge callers)."""

    slug: str = Field(..., description="Note id — knowledge/<slug>.md in the KB repo")
    job_id: str | None = Field(
        None, description="Calling job UUID, for commit attribution"
    )
    reason: str | None = Field(
        None, description="Why — journaled in the commit message"
    )
    expected_blob_sha: str | None = Field(
        None,
        description=(
            "Compare-and-swap token: the blob SHA the caller read. The delete is "
            "refused (failed/precondition-failed) if the repo holds another blob."
        ),
    )


class KnowledgeProjectionRequest(BaseModel):
    """Internal report of the projection leg of a canonical mutation."""

    synced: bool
    error: str | None = None
