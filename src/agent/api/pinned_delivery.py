"""Bind a pinned Job's accepted wire identity to its running agent process."""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from shared.pinned_job_delivery import pinned_job_projection_digest


def accepted_pinned_job_delivery(request: Any, client: Any, *, retry: bool) -> dict:
    """Return the exact receipt echo, or refuse a changed same-Job retry."""

    delivery_id = (
        str(request.pinned_delivery_id) if request.pinned_delivery_id else None
    )
    digest = request.pinned_projection_digest
    proof = request.pinned_delivery_proof
    if not (
        (delivery_id is None and digest is None and proof is None)
        or (delivery_id is not None and digest is not None and proof is not None)
    ):
        raise HTTPException(409, {"code": "pinned_delivery_incomplete"})
    if delivery_id is not None:
        actual = pinned_job_projection_digest(
            request.model_dump(mode="json", exclude_none=True)
        )
        if actual != digest or client is None:
            raise HTTPException(409, {"code": "pinned_projection_mismatch"})
    if retry:
        if (
            getattr(client, "pinned_delivery_job_id", None) != request.job_id
            or getattr(client, "pinned_delivery_id", None) != delivery_id
            or getattr(client, "pinned_projection_digest", None) != digest
            or getattr(client, "pinned_delivery_proof", None) != proof
        ):
            raise HTTPException(409, {"code": "pinned_delivery_changed"})
    elif client is not None:
        client.pinned_delivery_job_id = request.job_id
        client.pinned_delivery_id = delivery_id
        client.pinned_projection_digest = digest
        client.pinned_delivery_proof = proof
    return {
        "pinned_delivery_id": delivery_id,
        "pinned_projection_digest": digest,
    }
