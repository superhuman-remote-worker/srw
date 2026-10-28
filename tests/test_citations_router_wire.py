"""Wire contracts for the extracted citation/source-library router.

The behaviours pinned here are the ones the move could quietly change:

* ``user_can_access_job_or_thread`` is the citation gate on purpose — a
  session citation's ``job_id`` is a *thread* id with no ``jobs`` row, and a
  plain job check would refuse it. Denial answers **404**, not 403, so a probe
  cannot learn that the citation exists;
* ``/api/citations/snapshot`` is the internal (``X-Internal-Key``) write
  boundary and nothing else, including its 503/400 order;
* cross-job source enumeration stays admin-only, and a source is visible only
  through a job the caller can reach (403 there, because the source id itself
  was already known to the caller);
* the paging bounds every one of these reads declares.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from tests._mounted_router import mount_router


JOB_ID = "00000000-0000-0000-0000-0000000000a1"
THREAD_ID = "00000000-0000-0000-0000-0000000000e1"
PROJECT_ID = "00000000-0000-0000-0000-0000000000b1"
USER = {"id": "00000000-0000-0000-0000-0000000000c1", "is_admin": False}
ADMIN = {"id": "00000000-0000-0000-0000-0000000000ad", "is_admin": True}


class _Acquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


def _pool(conn):
    return SimpleNamespace(acquire=lambda: _Acquire(conn))


def _conn(**over):
    conn = SimpleNamespace(
        fetch=AsyncMock(return_value=[]),
        fetchrow=AsyncMock(return_value=None),
        fetchval=AsyncMock(return_value=None),
        execute=AsyncMock(return_value=None),
    )
    for key, value in over.items():
        setattr(conn, key, value)
    return conn


def _wire(
    *,
    conn=None,
    user=USER,
    job_or_thread=True,
    any_job=True,
    job_gate=None,
    internal_gate=None,
    snapshot_service=None,
    main_cloud_router=None,
):
    from orchestrator.routers.citations import CitationsDependencies as RouteDeps
    from orchestrator.routers.citations import router
    from orchestrator.services.citations import CitationDependencies as OpDeps

    connection = conn if conn is not None else _conn()
    db = SimpleNamespace()
    calls = SimpleNamespace(job=[], internal=[], job_or_thread=[], any_job=[])

    async def approved(_request, _store):
        return user

    async def job_access(request, _store, job_id):
        calls.job.append(job_id)
        if job_gate is not None:
            return await job_gate(request, _store, job_id)
        return {"id": job_id}

    async def project_member(_request, _store, project_id, **_kwargs):
        return (user, {"id": project_id})

    async def internal(request):
        calls.internal.append(request.url.path)
        if internal_gate is not None:
            return await internal_gate(request)
        return None

    async def can_access_job_or_thread(_caller, _store, entity_id):
        calls.job_or_thread.append(entity_id)
        return job_or_thread

    async def can_access_any_job(_caller, _store, job_ids):
        calls.any_job.append(list(job_ids))
        return any_job

    ops = OpDeps(
        store=db,
        vector_db=_pool(connection),
        snapshot_service=snapshot_service
        or SimpleNamespace(
            is_available=True,
            save_blob=AsyncMock(return_value="citations/ab/abcd"),
            get_blob=AsyncMock(return_value=b"bytes"),
        ),
        main_cloud_router=main_cloud_router or MagicMock(),
        logger=MagicMock(),
    )
    deps = RouteDeps(
        store=db,
        operations=ops,
        require_approved_user=approved,
        require_job_access=job_access,
        require_project_member=project_member,
        require_internal=internal,
        user_can_access_any_job=can_access_any_job,
        user_can_access_job_or_thread=can_access_job_or_thread,
    )
    app = mount_router(
        router, factories={"citations_dependencies_factory": lambda: deps}
    )
    return SimpleNamespace(
        client=TestClient(app), conn=connection, calls=calls, operations=ops
    )


def _citation_row(job_id=THREAD_ID, **over):
    row = {
        "id": 7,
        "job_id": job_id,
        "claim": "c",
        "source_name": "S",
        "source_type": "web",
    }
    row.update(over)
    return row


# =============================================================================
# The job-or-thread boundary
# =============================================================================


def test_a_session_citation_is_reachable_through_its_thread():
    """The citation's job_id is a thread id: only the job-or-thread resolver
    can authorize it, and it is the gate this route calls."""
    conn = _conn(fetchrow=AsyncMock(return_value=_citation_row()))
    wired = _wire(conn=conn, job_or_thread=True)

    response = wired.client.get("/api/citations/7")

    assert response.status_code == 200
    assert response.json()["job_id"] == THREAD_ID
    assert wired.calls.job_or_thread == [THREAD_ID]
    # The plain job gate is never consulted for a citation.
    assert wired.calls.job == []


@pytest.mark.parametrize(
    "path", ["/api/citations/7", "/api/citations/7/snapshot", "/api/citations/7/drift"]
)
def test_a_denied_citation_is_404_not_403(path):
    """404 over 403 so a probe cannot learn the citation exists."""
    conn = _conn(
        fetchrow=AsyncMock(
            return_value={
                "id": 7,
                "job_id": JOB_ID,
                "source_name": "S",
                "metadata": {"cloud": {"snapshot_blob_key": "k"}},
            }
        )
    )
    wired = _wire(conn=conn, job_or_thread=False)

    response = wired.client.get(path)

    assert response.status_code == 404
    assert response.json()["detail"] == "Citation 7 not found"
    assert wired.calls.job_or_thread == [JOB_ID]


def test_an_unknown_citation_and_a_denied_one_are_indistinguishable():
    unknown = _wire(conn=_conn(fetchrow=AsyncMock(return_value=None)))
    denied = _wire(
        conn=_conn(fetchrow=AsyncMock(return_value=_citation_row())),
        job_or_thread=False,
    )

    a = unknown.client.get("/api/citations/7")
    b = denied.client.get("/api/citations/7")

    assert a.status_code == b.status_code == 404
    assert a.json() == b.json()


def test_a_citation_with_a_null_job_still_reaches_the_gate_with_none():
    conn = _conn(fetchrow=AsyncMock(return_value=_citation_row(job_id=None)))
    wired = _wire(conn=conn, job_or_thread=False)

    response = wired.client.get("/api/citations/7")

    assert response.status_code == 404
    assert wired.calls.job_or_thread == [None]


def test_snapshot_without_a_stored_blob_is_404_after_authorization():
    conn = _conn(
        fetchrow=AsyncMock(
            return_value={"job_id": JOB_ID, "source_name": "S", "metadata": {}}
        )
    )
    wired = _wire(conn=conn, job_or_thread=True)

    response = wired.client.get("/api/citations/7/snapshot")

    assert response.status_code == 404
    assert response.json()["detail"] == "No snapshot stored for this citation"


def _snapshot_response(source_name, content_type="application/pdf"):
    conn = _conn(
        fetchrow=AsyncMock(
            return_value={
                "job_id": JOB_ID,
                "source_name": source_name,
                "metadata": {
                    "cloud": {
                        "snapshot_blob_key": "citations/ab/abcd",
                        "content_type": content_type,
                    }
                },
            }
        )
    )
    return _wire(conn=conn).client.get("/api/citations/7/snapshot")


def test_snapshot_serves_the_stored_bytes_as_an_attachment():
    response = _snapshot_response('we"ird.pdf')

    assert response.status_code == 200
    assert response.content == b"bytes"
    assert response.headers["content-type"] == "application/pdf"
    assert response.headers["x-content-type-options"] == "nosniff"
    # The quote cannot break out of the header value.
    assert response.headers["content-disposition"] == (
        "attachment; filename=\"we_ird.pdf\"; filename*=UTF-8''we%22ird.pdf"
    )


def test_an_html_snapshot_is_never_rendered_on_the_api_origin():
    """N51: a cited ``text/html`` file came back inline and sniffable."""
    response = _snapshot_response("page.html", content_type="text/html")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["content-disposition"].startswith("attachment;")


@pytest.mark.parametrize(
    "source_name,expected",
    [
        (
            "Bericht März 报告.pdf",
            'attachment; filename="Bericht M_rz __.pdf"; '
            "filename*=UTF-8''Bericht%20M%C3%A4rz%20%E6%8A%A5%E5%91%8A.pdf",
        ),
        (
            "a\r\nSet-Cookie: x=1.html",
            'attachment; filename="aSet-Cookie: x=1.html"; '
            "filename*=UTF-8''aSet-Cookie%3A%20x%3D1.html",
        ),
        (
            "../../etc\\passwd",
            "attachment; filename=\"passwd\"; filename*=UTF-8''passwd",
        ),
        (
            None,
            "attachment; filename=\"citation-7\"; filename*=UTF-8''citation-7",
        ),
    ],
)
def test_the_snapshot_filename_is_safe_for_any_source_name(source_name, expected):
    response = _snapshot_response(source_name)

    assert response.status_code == 200
    assert response.headers["content-disposition"] == expected


def test_drift_on_a_non_cloud_citation_is_400():
    conn = _conn(fetchrow=AsyncMock(return_value={"job_id": JOB_ID, "metadata": {}}))
    wired = _wire(conn=conn)

    response = wired.client.get("/api/citations/7/drift")

    assert response.status_code == 400
    assert response.json()["detail"] == "Citation has no cloud source to drift-check"


def test_drift_falls_back_to_unreachable_when_the_file_is_not_in_the_users_home():
    conn = _conn(
        fetchrow=AsyncMock(
            return_value={
                "job_id": JOB_ID,
                "metadata": {
                    "cloud": {
                        "webdav_url": "https://other.example/dav/x.md",
                        "file_sha256": "abc",
                        "snapshot_blob_key": "k",
                    }
                },
            }
        )
    )
    backend = SimpleNamespace(
        get_user_home=AsyncMock(
            return_value=SimpleNamespace(webdav_url="https://c.ex/dav/u/", handle="h")
        ),
        get_project_folder_file_bytes=AsyncMock(),
    )
    router = SimpleNamespace(for_owner=MagicMock(return_value=backend))
    wired = _wire(conn=conn, main_cloud_router=router)

    with patch(
        "orchestrator.services.citations.resolve_user_identity_cached",
        AsyncMock(return_value="cloud-user"),
    ):
        response = wired.client.get("/api/citations/7/drift")

    assert response.status_code == 200
    body = response.json()
    assert body["live_state"] == "unreachable"
    assert body["reason"] == "live source not reachable from your account"
    assert body["snapshot_available"] is True
    backend.get_project_folder_file_bytes.assert_not_awaited()


# =============================================================================
# The internal snapshot-store boundary
# =============================================================================


def test_storing_a_snapshot_requires_the_internal_key():
    async def deny(_request):
        raise HTTPException(status_code=403, detail="Internal key required")

    wired = _wire(internal_gate=deny)

    response = wired.client.post("/api/citations/snapshot", content=b"payload")

    assert response.status_code == 403
    wired.operations.snapshot_service.save_blob.assert_not_awaited()


def test_snapshot_store_unavailability_is_503_before_the_body_is_read():
    snapshot = SimpleNamespace(
        is_available=False, save_blob=AsyncMock(), get_blob=AsyncMock()
    )
    wired = _wire(snapshot_service=snapshot)

    response = wired.client.post("/api/citations/snapshot", content=b"payload")

    assert response.status_code == 503
    assert response.json()["detail"] == "Snapshot store unavailable"
    snapshot.save_blob.assert_not_awaited()


def test_an_empty_snapshot_body_is_400():
    wired = _wire()

    response = wired.client.post("/api/citations/snapshot")

    assert response.status_code == 400
    assert response.json()["detail"] == "Empty body"


def test_a_stored_snapshot_returns_its_content_addressed_key():
    wired = _wire()

    response = wired.client.post(
        "/api/citations/snapshot",
        params={"content_type": "application/pdf"},
        content=b"payload",
    )

    assert response.status_code == 200
    assert response.json() == {
        "snapshot_blob_key": "citations/ab/abcd",
        "size_bytes": 7,
    }
    assert wired.operations.snapshot_service.save_blob.await_args.kwargs == {
        "prefix": "citations",
        "content_type": "application/pdf",
    }


def test_a_snapshot_without_a_content_type_defaults_to_octet_stream():
    wired = _wire()

    wired.client.post("/api/citations/snapshot", content=b"x")

    assert (
        wired.operations.snapshot_service.save_blob.await_args.kwargs["content_type"]
        == "application/octet-stream"
    )


def test_a_failed_snapshot_write_is_500():
    snapshot = SimpleNamespace(
        is_available=True, save_blob=AsyncMock(return_value=None), get_blob=AsyncMock()
    )
    wired = _wire(snapshot_service=snapshot)

    response = wired.client.post("/api/citations/snapshot", content=b"x")

    assert response.status_code == 500
    assert response.json()["detail"] == "Snapshot store write failed"


# =============================================================================
# Source visibility
# =============================================================================


def test_cross_job_source_listing_is_admin_only():
    wired = _wire(user=USER)

    response = wired.client.get("/api/sources")

    assert response.status_code == 403
    assert response.json()["detail"].startswith(
        "Cross-job source listing requires admin role;"
    )
    wired.conn.fetch.assert_not_awaited()


def test_source_listing_scoped_to_a_job_uses_the_job_gate():
    conn = _conn(fetchrow=AsyncMock(return_value={"total": 0}))
    wired = _wire(conn=conn, user=USER)

    response = wired.client.get("/api/sources", params={"job_id": JOB_ID})

    assert response.status_code == 200
    assert wired.calls.job == [JOB_ID]


def test_an_admin_may_enumerate_across_jobs():
    conn = _conn(fetchrow=AsyncMock(return_value={"total": 0}))
    wired = _wire(conn=conn, user=ADMIN)

    response = wired.client.get("/api/sources")

    assert response.status_code == 200
    assert wired.calls.job == []


def test_a_source_the_caller_shares_no_job_with_is_403():
    conn = _conn(
        fetchrow=AsyncMock(return_value={"id": 5, "full_content_length": 10}),
        fetch=AsyncMock(return_value=[{"job_id": JOB_ID}]),
    )
    wired = _wire(conn=conn, any_job=False)

    response = wired.client.get("/api/sources/5")

    assert response.status_code == 403
    assert response.json()["detail"] == "Not authorized to access this source"
    assert wired.calls.any_job == [[JOB_ID]]


def test_a_missing_source_is_404_before_the_visibility_check():
    wired = _wire(conn=_conn(fetchrow=AsyncMock(return_value=None)), any_job=False)

    response = wired.client.get("/api/sources/5")

    assert response.status_code == 404
    assert response.json()["detail"] == "Source 5 not found"
    assert wired.calls.any_job == []


def test_source_detail_reports_truncation_against_the_content_limit():
    conn = _conn(
        fetchrow=AsyncMock(
            return_value={"id": 5, "content": "abc", "full_content_length": 9000}
        ),
        fetch=AsyncMock(return_value=[]),
    )
    wired = _wire(conn=conn, any_job=True)

    response = wired.client.get("/api/sources/5", params={"content_limit": 10})

    assert response.status_code == 200
    assert response.json()["content_truncated"] is True
    assert response.json()["job_ids"] == []


# =============================================================================
# Paging and query bounds
# =============================================================================


@pytest.mark.parametrize(
    "path,params",
    [
        ("/api/sources", {"limit": 0}),
        ("/api/sources", {"limit": 501}),
        ("/api/sources", {"offset": -1}),
        ("/api/sources/5", {"content_limit": -1}),
        ("/api/sources/5", {"content_limit": 100001}),
        (f"/api/jobs/{JOB_ID}/citations", {"limit": 0}),
        (f"/api/jobs/{JOB_ID}/citations", {"limit": 501}),
        (f"/api/jobs/{JOB_ID}/memories", {"limit": 0}),
        (f"/api/jobs/{JOB_ID}/memories", {"limit": 201}),
        (f"/api/jobs/{JOB_ID}/memories", {"offset": -1}),
        (f"/api/jobs/{JOB_ID}/sources/search", {"query": "x", "top_k": 0}),
        (f"/api/jobs/{JOB_ID}/sources/search", {"query": "x", "top_k": 51}),
    ],
)
def test_out_of_range_paging_is_rejected_by_validation(path, params):
    wired = _wire(user=ADMIN)

    assert wired.client.get(path, params=params).status_code == 422


def test_source_search_requires_a_query():
    wired = _wire()

    assert wired.client.get(f"/api/jobs/{JOB_ID}/sources/search").status_code == 422


def test_memory_listing_falls_back_to_a_safe_sort_for_an_unknown_field():
    """The sort field is interpolated into SQL, so an unknown one must not be."""
    conn = _conn(fetchrow=AsyncMock(return_value={"cnt": 0}))
    wired = _wire(conn=conn)

    response = wired.client.get(
        f"/api/jobs/{JOB_ID}/memories",
        params={"sort_by": "1; DROP TABLE memories", "sort_order": "sideways"},
    )

    assert response.status_code == 200
    page_sql = conn.fetch.await_args.args[0]
    assert "ORDER BY created_at desc" in page_sql
    assert "DROP TABLE" not in page_sql


@pytest.mark.parametrize(
    "path",
    [
        f"/api/jobs/{JOB_ID}/citations",
        f"/api/jobs/{JOB_ID}/citations/stats",
        f"/api/jobs/{JOB_ID}/memories",
        f"/api/jobs/{JOB_ID}/memory/stats",
        f"/api/jobs/{JOB_ID}/sources/1/annotations",
        f"/api/jobs/{JOB_ID}/sources/1/tags",
    ],
)
def test_job_scoped_reads_refuse_before_touching_the_vector_pool(path):
    async def deny(*_a, **_k):
        raise HTTPException(status_code=403, detail="Not authorized")

    wired = _wire(job_gate=deny)

    assert wired.client.get(path).status_code == 403
    wired.conn.fetch.assert_not_awaited()
    wired.conn.fetchrow.assert_not_awaited()


def test_project_memory_stats_returns_the_empty_shape_when_there_is_no_row():
    wired = _wire(conn=_conn(fetchrow=AsyncMock(return_value=None)))

    response = wired.client.get(f"/api/projects/{PROJECT_ID}/memory/stats")

    assert response.status_code == 200
    assert response.json() == {
        "project_id": PROJECT_ID,
        "total": 0,
        "total_tokens": 0,
    }
