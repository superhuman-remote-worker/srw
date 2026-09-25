"""E4 — evidence manifest security matrix (officer_supervision_surface §3.3).

The gate: path-traversal, current-revision, and oversize denials, plus the
route-level project authorization on every read. Evidence is a manifest, not
a filesystem browser — nothing in here may hand the caller a path they chose.
"""

from __future__ import annotations

import hashlib
import io
import json
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from PIL import Image

from orchestrator.services.job_evidence import (
    DIFF_LINE_LIMIT,
    MAX_IMAGES_PER_JOB,
    READ_PAGE_CHARS,
    TEXT_LIMIT_BYTES,
    build_evidence_manifest,
    find_entry,
    parse_manifest,
    public_manifest,
    read_evidence_entry,
)
from orchestrator.application import jobs as jobs_composition

JOB_ID = "11111111-2222-3333-4444-555555555555"
HEAD_SHA = "abc123def4567890abc123def4567890abc123de"


def _png_bytes(color: tuple[int, int, int] = (0, 120, 255)) -> bytes:
    output = io.BytesIO()
    Image.new("RGB", (2, 2), color).save(output, format="PNG")
    return output.getvalue()


def _animated_gif_over_pixel_limit() -> bytes:
    """Two static-compressed frames whose aggregate exceeds 40M pixels."""
    size = (4500, 4500)
    output = io.BytesIO()
    first = Image.new("P", size, 0)
    second = Image.new("P", size, 1)
    first.save(
        output,
        format="GIF",
        save_all=True,
        append_images=[second],
        duration=100,
        loop=0,
    )
    return output.getvalue()


def _job(**overrides) -> dict:
    job = {
        "id": JOB_ID,
        "status": "processing",
        "repo_name": "job-repo",
        "branch_name": None,
        "project_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "context": {
            "evidence_manifest": {
                "version": 1,
                "job_id": JOB_ID,
                "source_revision": HEAD_SHA,
                "source_repository": "job-repo",
                "source_ref": "main",
            }
        },
        "freeze_data": None,
    }
    job.update(overrides)
    return job


def _read_job(**overrides) -> dict:
    overrides.setdefault(
        "freeze_data",
        {
            "evidence": [
                {
                    "kind": "test_report",
                    "label": "entry",
                    "media_type": "text/plain",
                    "source": "output/pytest.txt",
                }
            ]
        },
    )
    return _job(**overrides)


def _result(evidence=None, summary="did the thing") -> dict:
    freeze: dict = {
        "freeze_type": "job_complete",
        "summary": summary,
        "deliverables": ["output/report.md"],
        "confidence": 0.9,
    }
    if evidence is not None:
        freeze["evidence"] = evidence
    return {"should_stop": True, "goal_achieved": True, "freeze_data": freeze}


def _gitea(files: dict[tuple[str, str], bytes] | None = None, head=HEAD_SHA):
    gitea = SimpleNamespace()
    gitea.is_initialized = True
    gitea.get_branch_head_sha = AsyncMock(return_value=head)

    async def get_file_bytes(repo, path, ref=None, *, redact_coordinates=False):
        return (files or {}).get((path, ref))

    gitea.get_file_bytes = AsyncMock(side_effect=get_file_bytes)
    return gitea


def _db():
    db = SimpleNamespace()
    db.get_job = AsyncMock(return_value=None)
    return db


# ---------------------------------------------------------------------------
# Manifest construction
# ---------------------------------------------------------------------------


class TestManifestBuild:
    @pytest.mark.asyncio
    async def test_server_entries_report_and_gate_stamp(self):
        manifest = await build_evidence_manifest(
            _job(
                context={"deliverable_gate": {"passed": True, "commit_sha": HEAD_SHA}}
            ),
            _result(),
            db=_db(),
            gitea=_gitea(),
        )
        kinds = [entry["kind"] for entry in manifest["entries"]]
        assert kinds == ["completion_report", "deliverable_check"]
        report = manifest["entries"][0]
        assert report["producer"] == "server"
        assert report["byte_size"] > 0
        assert report["sha256"]
        assert json.loads(report["inline_content"])["summary"] == "did the thing"
        assert manifest["source_revision"] == HEAD_SHA

    @pytest.mark.asyncio
    async def test_worker_entry_resolved_measured_and_pinned(self):
        content = b"1 passed in 0.1s\n"
        manifest = await build_evidence_manifest(
            _job(),
            _result(
                evidence=[
                    {
                        "kind": "test_report",
                        "label": "pytest run",
                        "media_type": "text/plain",
                        "source": "output/pytest.txt",
                    }
                ]
            ),
            db=_db(),
            gitea=_gitea(files={("output/pytest.txt", HEAD_SHA): content}),
        )
        entry = manifest["entries"][-1]
        assert entry["kind"] == "test_report"
        assert entry["producer"] == "worker"
        assert entry["availability"] == "available"
        assert entry["byte_size"] == len(content)
        assert entry["sha256"] == hashlib.sha256(content).hexdigest()
        assert entry["source"]["revision"] == HEAD_SHA
        assert entry["id"].startswith("ev_")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "bad_path",
        [
            "../secrets.env",
            "/etc/passwd",
            "a/../../b.txt",
            "C:\\windows\\system32",
            "notes\\..\\..\\x",
            "~/.ssh/id_rsa",
            "",
            None,
        ],
    )
    async def test_path_traversal_declarations_are_rejected(self, bad_path):
        manifest = await build_evidence_manifest(
            _job(),
            _result(
                evidence=[
                    {
                        "kind": "test_report",
                        "label": "bad",
                        "media_type": "text/plain",
                        "source": bad_path,
                    }
                ]
            ),
            db=_db(),
            gitea=_gitea(),
        )
        entry = manifest["entries"][-1]
        assert entry["availability"] == "rejected_path"
        # Gitea is never consulted for a rejected path.
        assert entry["byte_size"] is None

    @pytest.mark.asyncio
    async def test_worker_cannot_forge_server_kinds(self):
        manifest = await build_evidence_manifest(
            _job(),
            _result(
                evidence=[
                    {
                        "kind": "completion_report",
                        "label": "forged",
                        "media_type": "application/json",
                        "source": "output/fake.json",
                    }
                ]
            ),
            db=_db(),
            gitea=_gitea(),
        )
        forged = manifest["entries"][-1]
        assert forged["availability"] == "rejected_kind"

    @pytest.mark.asyncio
    async def test_oversize_text_and_diff_line_bounds(self):
        big = b"x" * (TEXT_LIMIT_BYTES + 1)
        long_diff = b"\n".join(b"+line" for _ in range(DIFF_LINE_LIMIT + 1))
        manifest = await build_evidence_manifest(
            _job(),
            _result(
                evidence=[
                    {
                        "kind": "test_report",
                        "label": "huge",
                        "media_type": "text/plain",
                        "source": "output/huge.txt",
                    },
                    {
                        "kind": "change_summary",
                        "label": "long diff",
                        "media_type": "text/x-diff",
                        "source": "output/change.diff",
                    },
                ]
            ),
            db=_db(),
            gitea=_gitea(
                files={
                    ("output/huge.txt", HEAD_SHA): big,
                    ("output/change.diff", HEAD_SHA): long_diff,
                }
            ),
        )
        huge, diff = manifest["entries"][-2:]
        assert huge["availability"] == "oversize"
        assert diff["availability"] == "oversize"
        assert str(DIFF_LINE_LIMIT) in diff["availability_reason"]

    @pytest.mark.asyncio
    async def test_image_cap_is_five_per_job(self):
        files = {
            (f"shots/{i}.png", HEAD_SHA): _png_bytes((i, 120, 255))
            for i in range(MAX_IMAGES_PER_JOB + 1)
        }
        manifest = await build_evidence_manifest(
            _job(),
            _result(
                evidence=[
                    {
                        "kind": "screenshot",
                        "label": f"shot {i}",
                        "media_type": "image/png",
                        "source": f"shots/{i}.png",
                    }
                    for i in range(MAX_IMAGES_PER_JOB + 1)
                ]
            ),
            db=_db(),
            gitea=_gitea(files=files),
        )
        screenshots = [e for e in manifest["entries"] if e["kind"] == "screenshot"]
        assert len(screenshots) == MAX_IMAGES_PER_JOB + 1
        assert [e["availability"] for e in screenshots].count("available") == (
            MAX_IMAGES_PER_JOB
        )
        assert screenshots[-1]["availability"] == "rejected_image_cap"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("content", "declared_media"),
        [
            (b"not-an-image", "image/png"),
            (_png_bytes(), "image/jpeg"),
        ],
    )
    async def test_bad_or_mismatched_image_media_fails_closed(
        self, content, declared_media
    ):
        manifest = await build_evidence_manifest(
            _job(),
            _result(
                evidence=[
                    {
                        "kind": "screenshot",
                        "label": "untrusted",
                        "media_type": declared_media,
                        "source": "shots/x.png",
                    }
                ]
            ),
            db=_db(),
            gitea=_gitea(files={("shots/x.png", HEAD_SHA): content}),
        )
        assert manifest["entries"][-1]["availability"] == "bad_media"

    @pytest.mark.asyncio
    async def test_animated_image_over_aggregate_pixel_ceiling_is_refused(self):
        content = _animated_gif_over_pixel_limit()
        assert len(content) < 8 * 1024 * 1024
        manifest = await build_evidence_manifest(
            _job(),
            _result(
                evidence=[
                    {
                        "kind": "screenshot",
                        "label": "animated bomb",
                        "media_type": "image/gif",
                        "source": "shots/animated.gif",
                    }
                ]
            ),
            db=_db(),
            gitea=_gitea(files={("shots/animated.gif", HEAD_SHA): content}),
        )
        assert manifest["entries"][-1]["availability"] == "bad_media"

    @pytest.mark.asyncio
    async def test_decompression_bomb_error_is_a_safe_manifest_rejection(self):
        with patch(
            "orchestrator.services.job_evidence.Image.open",
            side_effect=Image.DecompressionBombError("private/path.png"),
        ):
            manifest = await build_evidence_manifest(
                _job(),
                _result(
                    evidence=[
                        {
                            "kind": "screenshot",
                            "label": "bomb",
                            "media_type": "image/png",
                            "source": "shots/bomb.png",
                        }
                    ]
                ),
                db=_db(),
                gitea=_gitea(files={("shots/bomb.png", HEAD_SHA): _png_bytes()}),
            )
        assert manifest["entries"][-1]["availability"] == "bad_media"

    @pytest.mark.asyncio
    async def test_unresolvable_declaration_is_recorded_honestly(self):
        manifest = await build_evidence_manifest(
            _job(),
            _result(
                evidence=[
                    {
                        "kind": "test_report",
                        "label": "ghost",
                        "media_type": "text/plain",
                        "source": "output/never-pushed.txt",
                    }
                ]
            ),
            db=_db(),
            gitea=_gitea(files={}),
        )
        entry = manifest["entries"][-1]
        assert entry["availability"] == "unresolved"
        assert "pinned completion revision" in entry["availability_reason"]


# ---------------------------------------------------------------------------
# Reads — pinned revision, tamper detection, bounds
# ---------------------------------------------------------------------------


def _available_entry(content: bytes, *, kind="test_report", media="text/plain") -> dict:
    return {
        "id": "ev_abc123abc123",
        "kind": kind,
        "label": "entry",
        "media_type": media,
        "byte_size": len(content),
        "sha256": hashlib.sha256(content).hexdigest(),
        "source": {
            "type": "job_repo",
            "repo": "job-repo",
            "path": "output/pytest.txt",
            "ref": "main",
            "revision": HEAD_SHA,
        },
        "captured_at": "2026-08-14T12:00:00+00:00",
        "producer": "worker",
        "availability": "available",
    }


class TestEvidenceRead:
    @pytest.mark.asyncio
    async def test_private_gitea_failure_logs_hide_repository_and_path(self, caplog):
        from orchestrator.services.gitea import GiteaClient

        client = GiteaClient()
        client._initialized = True
        client._url = "http://gitea.invalid"
        client._client = SimpleNamespace(
            is_closed=False,
            get=AsyncMock(return_value=SimpleNamespace(status_code=500)),
        )
        assert (
            await client.get_file_bytes(
                "victim-private-repo",
                "private/report.png",
                ref=HEAD_SHA,
                redact_coordinates=True,
            )
            is None
        )
        assert (
            await client.get_branch_head_sha(
                "victim-private-repo",
                "private-branch",
                redact_coordinates=True,
            )
            is None
        )
        assert "victim-private-repo" not in caplog.text
        assert "private/report.png" not in caplog.text
        assert "private-branch" not in caplog.text

    @pytest.mark.asyncio
    async def test_read_resolves_only_the_pinned_revision(self):
        """The branch has moved on — the read must fetch the pinned sha, and
        the caller cannot switch revisions (there is no ref parameter)."""
        old = b"old pinned content"
        gitea = _gitea(
            files={
                ("output/pytest.txt", HEAD_SHA): old,
                ("output/pytest.txt", "newer-sha"): b"NEW head content",
            }
        )
        result = await read_evidence_entry(
            _read_job(), _available_entry(old), offset=0, gitea=gitea
        )
        assert result["content"] == old.decode()
        assert result["entry"]["source"] == {
            "type": "job_repo",
            "revision": HEAD_SHA,
        }
        requested_refs = [
            call.kwargs.get("ref") or call.args[2]
            if len(call.args) > 2
            else call.kwargs.get("ref")
            for call in gitea.get_file_bytes.await_args_list
        ]
        assert requested_refs == [HEAD_SHA]

    @pytest.mark.asyncio
    async def test_sha_mismatch_refuses_content_as_tampered(self):
        gitea = _gitea(files={("output/pytest.txt", HEAD_SHA): b"rewritten bytes"})
        entry = _available_entry(b"the original bytes")
        result = await read_evidence_entry(_read_job(), entry, offset=0, gitea=gitea)
        assert "content" not in result
        assert "tampered" in result["note"]

    @pytest.mark.asyncio
    async def test_object_store_exception_is_generic_and_base64_free(self, caplog):
        secret_blob = "c2VjcmV0LWJ5dGVz"
        gitea = _gitea()
        gitea.get_file_bytes = AsyncMock(
            side_effect=RuntimeError(f"backend leaked {secret_blob}")
        )
        result = await read_evidence_entry(
            _read_job(), _available_entry(b"original"), offset=0, gitea=gitea
        )
        assert (
            result["note"] == "content unavailable: pinned evidence store read failed"
        )
        assert secret_blob not in str(result)
        assert secret_blob not in caplog.text

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "source_patch",
        [
            {"path": "../secret.png"},
            {"revision": "b" * 40},
            {"repo": "foreign-project-repo"},
            {"ref": "foreign-branch"},
        ],
    )
    async def test_stored_source_substitution_fails_closed(self, source_patch):
        content = _png_bytes()
        entry = _available_entry(content, kind="screenshot", media="image/png")
        entry["source"].update(source_patch)
        job = _read_job(
            context={
                "evidence_manifest": {
                    "version": 1,
                    "job_id": JOB_ID,
                    "source_revision": HEAD_SHA,
                    "source_repository": "job-repo",
                    "source_ref": "main",
                    "entries": [entry],
                }
            }
        )
        gitea = _gitea(files={("output/pytest.txt", HEAD_SHA): content})
        result = await read_evidence_entry(job, entry, offset=0, gitea=gitea)
        assert "attachment" not in result
        assert "REFUSED" in result["note"]
        gitea.get_file_bytes.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_job_repo_read_requires_both_server_pinned_manifest_anchors(self):
        content = _png_bytes()
        entry = _available_entry(content, kind="screenshot", media="image/png")
        for incomplete_manifest in (
            {
                "version": 1,
                "job_id": JOB_ID,
                "source_ref": "main",
                "source_revision": HEAD_SHA,
            },
            {
                "version": 1,
                "job_id": JOB_ID,
                "source_ref": "main",
                "source_repository": "job-repo",
            },
        ):
            gitea = _gitea(files={("output/pytest.txt", HEAD_SHA): content})
            result = await read_evidence_entry(
                _read_job(context={"evidence_manifest": incomplete_manifest}),
                entry,
                gitea=gitea,
            )
            assert "REFUSED" in result["note"]
            gitea.get_file_bytes.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_forged_foreign_repository_is_refused_before_object_read(self):
        content = b"private victim bytes"
        entry = _available_entry(content)
        entry["source"].update(
            {
                "repo": "victim-private-repo",
                "path": "private/report.txt",
            }
        )
        job = _read_job(
            context={
                "evidence_manifest": {
                    "version": 1,
                    "job_id": JOB_ID,
                    "source_repository": "victim-private-repo",
                    "source_ref": "main",
                    "source_revision": HEAD_SHA,
                    "entries": [entry],
                }
            }
        )
        gitea = _gitea(files={("private/report.txt", HEAD_SHA): content})
        result = await read_evidence_entry(job, entry, db=_db(), gitea=gitea)
        assert result["note"] == (
            "content REFUSED: evidence provenance is not authoritative"
        )
        assert "victim-private-repo" not in json.dumps(result)
        assert "private/report.txt" not in json.dumps(result)
        gitea.get_file_bytes.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_forged_undeclared_path_in_canonical_repo_is_refused(self):
        content = b"not declared as evidence"
        entry = _available_entry(content)
        entry["source"]["path"] = "private/undeclared.txt"
        job = _read_job(
            context={
                "evidence_manifest": {
                    "version": 1,
                    "job_id": JOB_ID,
                    "source_repository": "job-repo",
                    "source_ref": "main",
                    "source_revision": HEAD_SHA,
                    "entries": [entry],
                }
            }
        )
        gitea = _gitea(files={("private/undeclared.txt", HEAD_SHA): content})
        result = await read_evidence_entry(job, entry, db=_db(), gitea=gitea)
        assert "REFUSED" in result["note"]
        assert "private/undeclared.txt" not in json.dumps(result)
        gitea.get_file_bytes.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("missing", ["sha256", "byte_size"])
    async def test_file_entry_requires_recorded_digest_and_size(self, missing):
        content = b"measured"
        entry = _available_entry(content)
        entry.pop(missing)
        gitea = _gitea(files={("output/pytest.txt", HEAD_SHA): content})
        result = await read_evidence_entry(_read_job(), entry, db=_db(), gitea=gitea)
        assert "REFUSED" in result["note"]
        gitea.get_file_bytes.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_malformed_source_shape_is_a_refusal_not_a_500(self):
        entry = _available_entry(b"measured")
        entry["source"] = "victim-private-repo/private/report.txt"
        result = await read_evidence_entry(_read_job(), entry, db=_db(), gitea=_gitea())
        assert result["note"] == (
            "content REFUSED: evidence provenance is not authoritative"
        )
        assert "victim-private-repo" not in json.dumps(result)

    @pytest.mark.asyncio
    async def test_server_built_manifest_for_canonical_repository_reads(self):
        content = b"server measured content\n"
        job = _read_job(context={})
        manifest = await build_evidence_manifest(
            job,
            _result(
                evidence=[
                    {
                        "kind": "test_report",
                        "label": "report",
                        "media_type": "text/plain",
                        "source": "output/pytest.txt",
                    }
                ]
            ),
            db=_db(),
            gitea=_gitea(files={("output/pytest.txt", HEAD_SHA): content}),
        )
        job["context"] = {"evidence_manifest": manifest}
        entry = manifest["entries"][-1]
        result = await read_evidence_entry(
            job,
            entry,
            db=_db(),
            gitea=_gitea(files={("output/pytest.txt", HEAD_SHA): content}),
        )
        assert result["content"] == content.decode()

    @pytest.mark.asyncio
    async def test_subjob_uses_authoritative_root_repository_and_own_branch(self):
        content = b"subjob report\n"
        parent_id = "99999999-2222-3333-4444-555555555555"
        job = _read_job(
            repo_name=None,
            branch_name="job/child",
            parent_job_id=parent_id,
            context={},
        )
        db = _db()
        db.get_job = AsyncMock(return_value={"id": parent_id, "repo_name": "root-repo"})
        gitea = _gitea(files={("output/pytest.txt", HEAD_SHA): content})
        manifest = await build_evidence_manifest(
            job,
            _result(
                evidence=[
                    {
                        "kind": "test_report",
                        "label": "child",
                        "media_type": "text/plain",
                        "source": "output/pytest.txt",
                    }
                ]
            ),
            db=db,
            gitea=gitea,
        )
        assert manifest["source_repository"] == "root-repo"
        assert manifest["source_ref"] == "job/child"
        job["context"] = {"evidence_manifest": manifest}
        result = await read_evidence_entry(
            job, manifest["entries"][-1], db=db, gitea=gitea
        )
        assert result["content"] == content.decode()

    @pytest.mark.asyncio
    async def test_oversize_entry_is_denied_with_honest_metadata(self):
        entry = _available_entry(b"irrelevant")
        entry["availability"] = "oversize"
        entry["availability_reason"] = "too big"
        gitea = _gitea(files={("output/pytest.txt", HEAD_SHA): b"irrelevant"})
        result = await read_evidence_entry(_read_job(), entry, offset=0, gitea=gitea)
        assert "content" not in result
        assert "availability=oversize" in result["note"]
        gitea.get_file_bytes.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_binary_screenshot_returns_one_verified_attachment(self):
        content = _png_bytes()
        gitea = _gitea(files={("output/pytest.txt", HEAD_SHA): content})
        entry = _available_entry(content, kind="screenshot", media="image/png")
        result = await read_evidence_entry(_read_job(), entry, offset=0, gitea=gitea)
        assert "content" not in result
        assert result["attachment"]["type"] == "image"
        assert result["attachment"]["media_type"] == "image/png"
        assert result["attachment"]["byte_size"] == len(content)
        assert result["attachment"]["width"] == 2
        assert result["attachment"]["height"] == 2
        assert "output/pytest.txt" not in json.dumps(result["attachment"])

    @pytest.mark.asyncio
    async def test_screenshot_manifest_size_mismatch_is_refused(self):
        content = _png_bytes()
        gitea = _gitea(files={("output/pytest.txt", HEAD_SHA): content})
        entry = _available_entry(content, kind="screenshot", media="image/png")
        entry["byte_size"] += 1
        result = await read_evidence_entry(_read_job(), entry, offset=0, gitea=gitea)
        assert "attachment" not in result
        assert "recorded measurement" in result["note"]

    @pytest.mark.asyncio
    async def test_decompression_bomb_error_is_a_safe_read_refusal(self):
        content = _png_bytes()
        entry = _available_entry(content, kind="screenshot", media="image/png")
        with patch(
            "orchestrator.services.job_evidence.Image.open",
            side_effect=Image.DecompressionBombError("private/path.png"),
        ):
            result = await read_evidence_entry(
                _read_job(),
                entry,
                db=_db(),
                gitea=_gitea(files={("output/pytest.txt", HEAD_SHA): content}),
            )
        assert "attachment" not in result
        assert result["note"] == (
            "content REFUSED: bytes are not the recorded allowed image type"
        )
        assert "private/path.png" not in json.dumps(result)

    @pytest.mark.asyncio
    async def test_text_reads_paginate_with_offset(self):
        content = ("A" * READ_PAGE_CHARS + "B" * 10).encode()
        gitea = _gitea(files={("output/pytest.txt", HEAD_SHA): content})
        entry = _available_entry(content)
        first = await read_evidence_entry(_read_job(), entry, offset=0, gitea=gitea)
        assert len(first["content"]) == READ_PAGE_CHARS
        assert first["truncated"] is True
        second = await read_evidence_entry(
            _read_job(), entry, offset=READ_PAGE_CHARS, gitea=gitea
        )
        assert second["content"] == "B" * 10
        assert second["truncated"] is False

    @pytest.mark.asyncio
    async def test_inline_entries_read_without_gitea(self):
        inline = json.dumps({"summary": "done"})
        entry = {
            "id": "ev_inline000001",
            "kind": "completion_report",
            "label": "report",
            "media_type": "application/json",
            "byte_size": len(inline.encode()),
            "sha256": hashlib.sha256(inline.encode()).hexdigest(),
            "availability": "available",
            "inline_content": inline,
            "source": {"type": "inline", "revision": HEAD_SHA},
            "producer": "server",
        }
        result = await read_evidence_entry(_read_job(), entry, offset=0, gitea=None)
        assert json.loads(result["content"]) == {"summary": "done"}
        # inline payloads never leak through the public entry echo
        assert "inline_content" not in result["entry"]

    def test_public_manifest_strips_payloads_and_object_plane_coordinates(self):
        manifest = {
            "recorded_at": "t",
            "source_repository": "private-repo",
            "source_ref": "secret-branch",
            "entries": [
                {
                    "id": "ev_1",
                    "inline_content": "secret",
                    "kind": "x",
                    "source": {
                        "type": "job_repo",
                        "repo": "private-repo",
                        "path": "screens/private.png",
                        "ref": "secret-branch",
                        "revision": HEAD_SHA,
                    },
                }
            ],
        }
        public = public_manifest(manifest)
        assert "inline_content" not in public["entries"][0]
        assert public["entries"][0]["source"] == {
            "type": "job_repo",
            "revision": HEAD_SHA,
        }
        assert "source_repository" not in public
        assert "source_ref" not in public

    def test_find_entry_only_matches_this_jobs_manifest(self):
        manifest = {"entries": ["malformed", {"id": "ev_1"}]}
        assert find_entry(manifest, "ev_1") == {"id": "ev_1"}
        assert find_entry(manifest, "ev_2") is None
        assert public_manifest(manifest)["entries"] == [{"id": "ev_1"}]
        assert parse_manifest({"context": "not-json"}) is None
        assert (
            parse_manifest(
                _job(
                    context={
                        "evidence_manifest": {
                            "version": 1,
                            "job_id": "foreign-job",
                            "entries": [],
                        }
                    }
                )
            )
            is None
        )


# ---------------------------------------------------------------------------
# Route authorization — (caller project, job project, evidence job)
# ---------------------------------------------------------------------------


def _patch_caller_and_db(user: dict, db):
    stack = ExitStack()
    stack.enter_context(
        patch(
            "orchestrator.security.auth.require_approved_user",
            AsyncMock(return_value=user),
        )
    )
    stack.enter_context(
        patch(
            "orchestrator.security.access.require_approved_user",
            AsyncMock(return_value=user),
        )
    )
    stack.enter_context(patch("orchestrator.main.app.state.resources.postgres_db", db))
    return stack


def _scoped(user: dict, scope: str) -> dict:
    out = dict(user)
    out["scopes"] = [scope]
    out["auth_method"] = "mcp"
    return out


class TestEvidenceRouteAuthorization:
    @pytest.mark.asyncio
    async def test_cross_project_scope_denied_even_with_valid_id(
        self, user_a, fake_db, fake_request, job_a, project_b
    ):
        """A guessed/leaked evidence ID from another project is denied by the
        server scope gate before the manifest is even parsed."""
        import orchestrator.main
        from orchestrator.routers.job_artifacts import read_job_evidence_route

        job_a["context"] = {
            "evidence_manifest": {"recorded_at": "t", "entries": [{"id": "ev_1"}]}
        }
        scoped_user = _scoped(user_a, f"project:{project_b['id']}")
        with _patch_caller_and_db(scoped_user, fake_db):
            with pytest.raises(HTTPException) as excinfo:
                await read_job_evidence_route(
                    fake_request,
                    str(job_a["id"]),
                    "ev_1",
                    offset=0,
                    dependencies=jobs_composition.job_artifacts_dependencies(
                        orchestrator.main.app.state.resources
                    ),
                )
        assert excinfo.value.status_code == 403

    @pytest.mark.asyncio
    async def test_non_member_denied_listing(
        self, user_b, fake_db, fake_request, job_a
    ):
        import orchestrator.main
        from orchestrator.routers.job_artifacts import list_job_evidence_route

        with _patch_caller_and_db(user_b, fake_db):
            with pytest.raises(HTTPException) as excinfo:
                await list_job_evidence_route(
                    fake_request,
                    str(job_a["id"]),
                    dependencies=jobs_composition.job_artifacts_dependencies(
                        orchestrator.main.app.state.resources
                    ),
                )
        assert excinfo.value.status_code == 403

    @pytest.mark.asyncio
    async def test_owner_reads_manifest_and_unknown_id_404s(
        self, user_a, fake_db, fake_request, job_a
    ):
        import orchestrator.main
        from orchestrator.routers.job_artifacts import (
            list_job_evidence_route,
            read_job_evidence_route,
        )

        job_a["context"] = {
            "evidence_manifest": {
                "version": 1,
                "recorded_at": "t",
                "job_id": str(job_a["id"]),
                "entries": [
                    {
                        "id": "ev_1",
                        "kind": "completion_report",
                        "availability": "available",
                        "inline_content": "{}",
                    }
                ],
            }
        }
        with _patch_caller_and_db(user_a, fake_db):
            listing = await list_job_evidence_route(
                fake_request,
                str(job_a["id"]),
                dependencies=jobs_composition.job_artifacts_dependencies(
                    orchestrator.main.app.state.resources
                ),
            )
            assert [e["id"] for e in listing["entries"]] == ["ev_1"]
            assert "inline_content" not in listing["entries"][0]
            with pytest.raises(HTTPException) as excinfo:
                await read_job_evidence_route(
                    fake_request,
                    str(job_a["id"]),
                    "ev_does_not_exist",
                    offset=0,
                    dependencies=jobs_composition.job_artifacts_dependencies(
                        orchestrator.main.app.state.resources
                    ),
                )
        assert excinfo.value.status_code == 404

    @pytest.mark.asyncio
    async def test_route_failure_never_exposes_private_coordinates(
        self, user_a, fake_db, fake_request, job_a, caplog
    ):
        import orchestrator.main
        from orchestrator.routers.job_artifacts import read_job_evidence_route

        job_a["context"] = {
            "evidence_manifest": {
                "version": 1,
                "job_id": str(job_a["id"]),
                "recorded_at": "2026-08-19T00:00:00+00:00",
                "entries": [{"id": "ev_1"}],
            }
        }
        private_detail = "victim-private-repo/private/report.png"
        with _patch_caller_and_db(user_a, fake_db):
            with patch(
                "orchestrator.services.job_evidence.read_evidence_entry",
                AsyncMock(side_effect=RuntimeError(private_detail)),
            ):
                with pytest.raises(HTTPException) as excinfo:
                    await read_job_evidence_route(
                        fake_request,
                        str(job_a["id"]),
                        "ev_1",
                        offset=0,
                        dependencies=jobs_composition.job_artifacts_dependencies(
                            orchestrator.main.app.state.resources
                        ),
                    )
        assert excinfo.value.status_code == 500
        assert private_detail not in str(excinfo.value.detail)
        assert private_detail not in caplog.text

    @pytest.mark.asyncio
    async def test_completion_report_route_404_when_absent(
        self, user_a, fake_db, fake_request, job_a
    ):
        import orchestrator.main
        from orchestrator.routers.job_artifacts import get_job_completion_report_route

        with _patch_caller_and_db(user_a, fake_db):
            with pytest.raises(HTTPException) as excinfo:
                await get_job_completion_report_route(
                    fake_request,
                    str(job_a["id"]),
                    dependencies=jobs_composition.job_artifacts_dependencies(
                        orchestrator.main.app.state.resources
                    ),
                )
        assert excinfo.value.status_code == 404
