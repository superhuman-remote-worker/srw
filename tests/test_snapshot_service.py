"""Tests for the C2/C3 verifiable-capture + no-clobber primitives on
``SnapshotService``:

- ``verify_snapshot`` — the fail-safe gate a later task (C4) calls before the
  irreversible ``delete_workspace_pvc``. Anything unverifiable (no manifest,
  missing object, size mismatch, deep-verify with no stored checksum, hash
  mismatch) must return ``(False, reason)``; only an all-good result returns
  ``(True, "ok")``. A buggy ``True`` here would let a caller destroy the only
  remaining copy of unrecoverable data, so every negative branch is exercised
  explicitly rather than inferred from the happy path.
- ``_streaming_sha256_from_s3`` — the O(1)-memory re-hash helper backing the
  deep check. Never buffers the whole (possibly multi-GB) object.
- The post-upload size check in ``upload_snapshot`` — a truncated/partial
  multipart upload must never advertise ``"available"``.
- §C3 no-clobber: ``upload_snapshot``'s canonical write now stages, verifies,
  then promotes (never overwriting canonical in place), keeping the last
  ``SNAPSHOT_KEEP_GENERATIONS`` generations under ``history/``. These tests
  use a dict-backed fake S3 (``FakeS3``) rather than a bare ``MagicMock`` so
  assertions can check the *actual bucket contents* after a bad upload, not
  just what was called.

See knowledge-base/knowledge/features/workspace_durability_tiering.md §C2/§C3.
"""

import hashlib
import json
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

from orchestrator.services.snapshot_service import SnapshotService

# A payload bigger than one 1 MB read-chunk, so a correct streaming
# implementation is forced to call .read() more than once — a single
# `body.read()` (whole object) would silently "work" on a small fixture and
# only blow up in production on a multi-GB archive.
_BIG_BYTES = b"x" * (1024 * 1024) + b"tail-bytes-not-a-full-chunk"
_BIG_SHA = hashlib.sha256(_BIG_BYTES).hexdigest()

GOOD_MANIFEST = {
    "checksum_sha256": _BIG_SHA,
    "size_compressed_bytes": len(_BIG_BYTES),
}


def _not_found(op_name: str = "HeadObject") -> ClientError:
    return ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, op_name)


class _FakeS3Body:
    """Stand-in for boto3's ``StreamingBody``.

    ``read(n)`` is mandatory (no default) — a regression to an unbounded
    ``body.read()`` call (no size arg) raises ``TypeError`` here, failing
    loudly instead of silently "working" against a tiny test fixture. Each
    call returns at most ``n`` bytes and the stream naturally empties over
    successive calls, exactly like the real S3 object body contract that
    ``_streaming_sha256_from_s3``'s chunked-read loop depends on.
    """

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos = 0
        self.calls: list[int] = []

    def read(self, n: int) -> bytes:
        self.calls.append(n)
        chunk = self._data[self._pos : self._pos + n]
        self._pos += len(chunk)
        return chunk


class FakeS3:
    """Minimal in-memory S3 double, dict-backed so a test can assert on the
    actual key set a staging -> verify -> promote -> prune sequence leaves
    behind. A ``MagicMock`` only proves *what was called*; it can't prove
    *what the bucket looks like afterwards* — exactly the gap the C3
    no-clobber guarantee needs closed (a bad upload must leave canonical
    bytes untouched, not just "call head_object once").

    Covers the calls ``upload_snapshot``'s stage/verify/promote/prune path
    and the read-side methods (``download_snapshot``, ``get_manifest``,
    ``get_storage_stats``) make: ``upload_file``, ``put_object``, ``copy``,
    ``head_object``, ``get_object``, ``download_file``, ``delete_object``,
    and the ``list_objects_v2`` paginator. Not a general boto3 substitute —
    just enough surface for this file's snapshot flows.
    """

    def __init__(self) -> None:
        self.store: dict[str, bytes] = {}

    # -- writes ---------------------------------------------------------

    def upload_file(self, Filename: str, Bucket: str, Key: str) -> None:
        with open(Filename, "rb") as f:
            self.store[Key] = f.read()

    def put_object(self, Bucket: str, Key: str, Body: bytes, **kwargs) -> None:
        self.store[Key] = bytes(Body)

    def copy(self, CopySource: dict, Bucket: str, Key: str, **kwargs) -> None:
        """Stand-in for boto3's managed, multipart-capable ``copy()``.

        Real S3 PUT/COPY is atomic at the destination key: a copy that
        raises never leaves a partial object behind, it just leaves
        whatever was already there. Raising *before* touching ``store``
        (rather than after) mirrors that — it's what lets a test simulate
        "the copy itself blew up" and still assert canonical survived.
        """
        src_key = CopySource["Key"]
        if src_key not in self.store:
            raise _not_found("CopyObject")
        self.store[Key] = self.store[src_key]

    def delete_object(self, Bucket: str, Key: str) -> None:
        self.store.pop(Key, None)

    # -- reads ------------------------------------------------------------

    def head_object(self, Bucket: str, Key: str) -> dict:
        if Key not in self.store:
            raise _not_found("HeadObject")
        return {"ContentLength": len(self.store[Key])}

    def get_object(self, Bucket: str, Key: str) -> dict:
        if Key not in self.store:
            raise _not_found("GetObject")
        return {
            "Body": _FakeS3Body(self.store[Key]),
            "ContentLength": len(self.store[Key]),
        }

    def download_file(self, Bucket: str, Key: str, Filename: str) -> None:
        if Key not in self.store:
            raise _not_found("HeadObject")
        with open(Filename, "wb") as f:
            f.write(self.store[Key])

    def get_paginator(self, operation_name: str) -> "_FakeListObjectsPaginator":
        assert operation_name == "list_objects_v2"
        return _FakeListObjectsPaginator(self)


class _FakeListObjectsPaginator:
    """Stand-in for boto3's ``list_objects_v2`` paginator: a single page,
    sorted keys, mirroring only the ``Contents[].Key``/``Size`` shape the
    service code reads.
    """

    def __init__(self, fake_s3: FakeS3) -> None:
        self._fake_s3 = fake_s3

    def paginate(self, Bucket: str, Prefix: str = "") -> list:
        contents = [
            {"Key": key, "Size": len(data)}
            for key, data in sorted(self._fake_s3.store.items())
            if key.startswith(Prefix)
        ]
        return [{"Contents": contents}]


@pytest.fixture
def fake_s3() -> FakeS3:
    return FakeS3()


@pytest.fixture
def fake_svc(fake_s3) -> SnapshotService:
    """A ``SnapshotService`` wired to the dict-backed ``FakeS3`` double —
    used by the §C3 staging/promote/prune tests below, which need to
    assert on real post-upload bucket state rather than mock call args.
    """
    s = SnapshotService()
    s._available = True
    s._bucket = "test-bucket"
    s._s3 = fake_s3
    s._set_snapshot_context = AsyncMock()
    return s


def _write_tar(tmp_path, name: str, data: bytes) -> str:
    p = tmp_path / name
    p.write_bytes(data)
    return str(p)


def _history_generations(fake_s3: FakeS3, prefix: str) -> set:
    """Distinct ``<ts>`` generation ids currently under ``{prefix}/history/``."""
    history_prefix = f"{prefix}/history/"
    return {
        key[len(history_prefix) :].split("/", 1)[0]
        for key in fake_s3.store
        if key.startswith(history_prefix)
    }


def _staging_keys(fake_s3: FakeS3, prefix: str) -> list:
    return [k for k in fake_s3.store if f"{prefix}/env.tar.zst.staging-" in k]


@pytest.fixture
def svc() -> SnapshotService:
    s = SnapshotService()
    s._available = True
    s._bucket = "test-bucket"
    s._s3 = MagicMock()
    return s


@pytest.fixture(autouse=True)
def _clean_verify_deep_env(monkeypatch):
    """Every test either passes ``deep=`` explicitly or opts into the env
    default deliberately — never let an ambient ``SNAPSHOT_VERIFY_DEEP``
    leak in from the outer environment.
    """
    monkeypatch.delenv("SNAPSHOT_VERIFY_DEEP", raising=False)


@pytest.mark.asyncio
async def test_upload_revalidates_authority_before_canonical_publication(
    fake_svc, fake_s3, tmp_path
):
    archive = _write_tar(tmp_path, "authority.tar.zst", b"captured-bytes")
    authority = AsyncMock(side_effect=[True, False])

    uploaded = await fake_svc.upload_snapshot(
        job_id="job-authority",
        tar_path=archive,
        manifest={"version": 1, "size_compressed_bytes": len(b"captured-bytes")},
        publication_authority=authority,
    )

    assert uploaded is False
    assert authority.await_count == 2
    assert not _staging_keys(fake_s3, "jobs/job-authority")
    assert "jobs/job-authority/env.tar.zst" not in fake_s3.store
    assert not _history_generations(fake_s3, "jobs/job-authority")
    fake_svc._set_snapshot_context.assert_not_awaited()


# =============================================================================
# _streaming_sha256_from_s3
# =============================================================================


class TestStreamingSha256FromS3:
    def test_returns_correct_hash_for_known_blob(self, svc):
        body = _FakeS3Body(_BIG_BYTES)
        svc._s3.get_object = MagicMock(return_value={"Body": body})

        got = svc._streaming_sha256_from_s3("jobs/j1/env.tar.zst")

        assert got == _BIG_SHA
        svc._s3.get_object.assert_called_once_with(
            Bucket="test-bucket", Key="jobs/j1/env.tar.zst"
        )

    def test_reads_body_in_bounded_chunks_not_all_at_once(self, svc):
        """O(1)-memory guard: repeated bounded reads, never a single
        unbounded read() of the whole object.
        """
        body = _FakeS3Body(_BIG_BYTES)
        svc._s3.get_object = MagicMock(return_value={"Body": body})

        svc._streaming_sha256_from_s3("jobs/j1/env.tar.zst")

        assert len(body.calls) >= 2, "expected multiple chunked reads"
        assert all(n == 1024 * 1024 for n in body.calls), (
            f"each read() must request a bounded 1 MB chunk, got {body.calls}"
        )


# =============================================================================
# verify_snapshot — fail-safe truth table
# =============================================================================


class TestVerifySnapshot:
    @pytest.mark.asyncio
    async def test_s3_unavailable_is_unverifiable(self, svc):
        svc._available = False
        svc.get_manifest = AsyncMock(return_value=dict(GOOD_MANIFEST))

        result = await svc.verify_snapshot("job-1")

        assert result == (False, "s3 unavailable")
        svc.get_manifest.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_manifest_is_unverifiable(self, svc):
        svc.get_manifest = AsyncMock(return_value=None)

        result = await svc.verify_snapshot("job-1")

        assert result == (False, "no manifest")

    @pytest.mark.asyncio
    async def test_get_manifest_raising_is_unverifiable_not_raised(self, svc):
        """``get_manifest`` wraps ``ClientError`` internally, but a
        non-ClientError (e.g. a corrupt-JSON decode error, or a transient
        connection failure) could still escape it. The gate must still
        return a verdict rather than propagate — a caller authorizing a
        PVC delete on this result must never have to handle a raise itself.
        """
        svc.get_manifest = AsyncMock(side_effect=ValueError("bad manifest json"))

        ok, reason = await svc.verify_snapshot("job-1")

        assert ok is False
        assert "verify error" in reason

    @pytest.mark.asyncio
    async def test_missing_object_is_unverifiable(self, svc):
        svc.get_manifest = AsyncMock(return_value=dict(GOOD_MANIFEST))
        svc._s3.head_object = MagicMock(side_effect=_not_found())

        result = await svc.verify_snapshot("job-1")

        assert result == (False, "object missing")

    @pytest.mark.asyncio
    async def test_head_object_connection_reset_is_unverifiable_not_raised(self, svc):
        """The HEAD's ``except ClientError`` only covers service-error
        responses (NoSuchKey/404/5xx) — a bubbled ``ConnectionResetError``
        is a plain ``Exception``, not a ``ClientError``, so it must be
        caught by a second, wider handler or it escapes the gate,
        contradicting ``verify_snapshot``'s "never raises" contract.
        """
        svc.get_manifest = AsyncMock(return_value=dict(GOOD_MANIFEST))
        svc._s3.head_object = MagicMock(
            side_effect=ConnectionResetError("connection reset by peer")
        )

        ok, reason = await svc.verify_snapshot("job-1")

        assert ok is False
        assert "verify error" in reason

    @pytest.mark.asyncio
    async def test_head_object_botocore_endpoint_error_is_unverifiable_not_raised(
        self, svc
    ):
        """Same guard, the specific family named in the review:
        ``botocore.exceptions.BotoCoreError`` (here, ``EndpointConnectionError``
        standing in for a connect/read timeout or DNS failure) is not a
        ``ClientError`` either — it must fail closed, not raise.
        """
        svc.get_manifest = AsyncMock(return_value=dict(GOOD_MANIFEST))
        svc._s3.head_object = MagicMock(
            side_effect=EndpointConnectionError(endpoint_url="https://s3.example.test")
        )

        ok, reason = await svc.verify_snapshot("job-1")

        assert ok is False
        assert "verify error" in reason

    @pytest.mark.asyncio
    async def test_size_mismatch_is_unverifiable(self, svc):
        svc.get_manifest = AsyncMock(return_value=dict(GOOD_MANIFEST))
        # Deliberately no "ETag" key at all: if the implementation ever
        # touched head["ETag"] for integrity (the multipart caveat this gate
        # must never regress on), this dict raises KeyError instead of
        # silently supplying a plausible-looking value.
        svc._s3.head_object = MagicMock(return_value={"ContentLength": 123})

        ok, reason = await svc.verify_snapshot("job-1")

        assert ok is False
        assert "size mismatch" in reason
        assert "123" in reason
        assert str(len(_BIG_BYTES)) in reason
        # Size check fails closed before any hash is attempted.
        svc._s3.get_object.assert_not_called()

    @pytest.mark.asyncio
    async def test_deep_with_no_stored_checksum_is_unverifiable(self, svc):
        """Legacy manifest (pre-checksum) — deep verify must fail closed,
        never treat "nothing to compare" as "verified".
        """
        legacy_manifest = {"size_compressed_bytes": len(_BIG_BYTES)}
        svc.get_manifest = AsyncMock(return_value=legacy_manifest)
        svc._s3.head_object = MagicMock(return_value={"ContentLength": len(_BIG_BYTES)})

        ok, reason = await svc.verify_snapshot("job-1", deep=True)

        assert ok is False
        assert "no checksum in manifest" in reason
        svc._s3.get_object.assert_not_called()

    @pytest.mark.asyncio
    async def test_deep_hash_mismatch_is_unverifiable(self, svc):
        svc.get_manifest = AsyncMock(return_value=dict(GOOD_MANIFEST))
        svc._s3.head_object = MagicMock(return_value={"ContentLength": len(_BIG_BYTES)})
        svc._s3.get_object = MagicMock(
            return_value={"Body": _FakeS3Body(b"corrupted-different-bytes")}
        )

        result = await svc.verify_snapshot("job-1", deep=True)

        assert result == (False, "sha256 mismatch")

    @pytest.mark.asyncio
    async def test_deep_verify_streaming_hash_client_error_is_unverifiable_not_raised(
        self, svc
    ):
        """TOCTOU guard: the object can vanish between the HEAD and the GET
        (e.g. a concurrent GC purge). The deep re-hash must fail closed —
        return ``(False, reason)`` — rather than let the ClientError escape
        the gate. A later task authorizes deleting a PVC on this verdict;
        an unhandled raise here would defer that safety decision to an
        unwritten caller.
        """
        svc.get_manifest = AsyncMock(return_value=dict(GOOD_MANIFEST))
        svc._s3.head_object = MagicMock(return_value={"ContentLength": len(_BIG_BYTES)})
        svc._streaming_sha256_from_s3 = MagicMock(side_effect=_not_found("GetObject"))

        ok, reason = await svc.verify_snapshot("job-1", deep=True)

        assert ok is False
        assert "verify error" in reason

    @pytest.mark.asyncio
    async def test_deep_verify_streaming_hash_generic_exception_is_unverifiable_not_raised(
        self, svc
    ):
        """Same guard, non-ClientError case: any transient S3 5xx/timeout/
        connection-reset during the deep re-hash must also fail closed
        instead of raising out of the gate.
        """
        svc.get_manifest = AsyncMock(return_value=dict(GOOD_MANIFEST))
        svc._s3.head_object = MagicMock(return_value={"ContentLength": len(_BIG_BYTES)})
        svc._streaming_sha256_from_s3 = MagicMock(
            side_effect=ConnectionResetError("connection reset by peer")
        )

        ok, reason = await svc.verify_snapshot("job-1", deep=True)

        assert ok is False
        assert "verify error" in reason

    @pytest.mark.asyncio
    async def test_all_good_deep_true_is_ok(self, svc):
        svc.get_manifest = AsyncMock(return_value=dict(GOOD_MANIFEST))
        svc._s3.head_object = MagicMock(return_value={"ContentLength": len(_BIG_BYTES)})
        svc._s3.get_object = MagicMock(return_value={"Body": _FakeS3Body(_BIG_BYTES)})

        result = await svc.verify_snapshot("job-1", deep=True)

        assert result == (True, "ok")
        svc.get_manifest.assert_awaited_once_with("job-1", entity_type="jobs")
        svc._s3.head_object.assert_called_once_with(
            Bucket="test-bucket", Key="jobs/job-1/env.tar.zst"
        )

    @pytest.mark.asyncio
    async def test_deep_false_skips_hash_and_passes_on_size_alone(self, svc):
        svc.get_manifest = AsyncMock(return_value=dict(GOOD_MANIFEST))
        svc._s3.head_object = MagicMock(return_value={"ContentLength": len(_BIG_BYTES)})
        svc._s3.get_object = MagicMock()  # must never be reached

        result = await svc.verify_snapshot("job-1", deep=False)

        assert result == (True, "ok")
        svc._s3.get_object.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_size_in_manifest_skips_size_check(self, svc):
        """``size_compressed_bytes`` absent => nothing to compare against,
        so the size check is a no-op (mirrors the "when present" guard in
        ``upload_snapshot``'s own post-upload check) and deep verify
        proceeds straight to the hash.
        """
        manifest = {"checksum_sha256": _BIG_SHA}  # no size_compressed_bytes
        svc.get_manifest = AsyncMock(return_value=manifest)
        # Deliberately mismatched — must never be consulted since want_size
        # is falsy.
        svc._s3.head_object = MagicMock(return_value={"ContentLength": 999999})
        svc._s3.get_object = MagicMock(return_value={"Body": _FakeS3Body(_BIG_BYTES)})

        result = await svc.verify_snapshot("job-1", deep=True)

        assert result == (True, "ok")

    @pytest.mark.asyncio
    async def test_default_deep_reads_env_true(self, svc, monkeypatch):
        monkeypatch.setenv("SNAPSHOT_VERIFY_DEEP", "true")
        svc.get_manifest = AsyncMock(return_value=dict(GOOD_MANIFEST))
        svc._s3.head_object = MagicMock(return_value={"ContentLength": len(_BIG_BYTES)})
        svc._s3.get_object = MagicMock(return_value={"Body": _FakeS3Body(_BIG_BYTES)})

        result = await svc.verify_snapshot("job-1")  # deep omitted

        assert result == (True, "ok")
        svc._s3.get_object.assert_called_once()

    @pytest.mark.asyncio
    async def test_default_deep_reads_env_false_skips_hash(self, svc, monkeypatch):
        monkeypatch.setenv("SNAPSHOT_VERIFY_DEEP", "false")
        svc.get_manifest = AsyncMock(return_value=dict(GOOD_MANIFEST))
        svc._s3.head_object = MagicMock(return_value={"ContentLength": len(_BIG_BYTES)})
        svc._s3.get_object = MagicMock()  # must never be reached

        result = await svc.verify_snapshot("job-1")  # deep omitted

        assert result == (True, "ok")
        svc._s3.get_object.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", ["1", "yes", "on", "YES", "On"])
    async def test_env_var_accepts_common_truthy_tokens(self, svc, monkeypatch, value):
        """An operator setting ``SNAPSHOT_VERIFY_DEEP=1`` (or ``yes``/``on``,
        any case) intends "enable" — treating it as falsy would silently
        disable deep verification and weaken a data-safety gate.
        """
        monkeypatch.setenv("SNAPSHOT_VERIFY_DEEP", value)
        svc.get_manifest = AsyncMock(return_value=dict(GOOD_MANIFEST))
        svc._s3.head_object = MagicMock(return_value={"ContentLength": len(_BIG_BYTES)})
        svc._s3.get_object = MagicMock(return_value={"Body": _FakeS3Body(_BIG_BYTES)})

        result = await svc.verify_snapshot("job-1")  # deep omitted

        assert result == (True, "ok")
        svc._s3.get_object.assert_called_once()

    @pytest.mark.asyncio
    async def test_entity_type_threads_changes_object_key(self, svc):
        svc.get_manifest = AsyncMock(return_value=dict(GOOD_MANIFEST))
        svc._s3.head_object = MagicMock(return_value={"ContentLength": len(_BIG_BYTES)})
        svc._s3.get_object = MagicMock(return_value={"Body": _FakeS3Body(_BIG_BYTES)})

        result = await svc.verify_snapshot("thread-1", entity_type="threads", deep=True)

        assert result == (True, "ok")
        svc.get_manifest.assert_awaited_once_with("thread-1", entity_type="threads")
        svc._s3.head_object.assert_called_once_with(
            Bucket="test-bucket", Key="threads/thread-1/env.tar.zst"
        )


# =============================================================================
# upload_snapshot — post-upload size check
# =============================================================================

_TAR_BYTES = b"fake-archive-bytes-for-testing-only"


class TestUploadSnapshotPostUploadSizeCheck:
    @pytest.fixture
    def tar_path(self, tmp_path) -> str:
        p = tmp_path / "env.tar.zst"
        p.write_bytes(_TAR_BYTES)
        return str(p)

    @pytest.fixture
    def svc(self) -> SnapshotService:
        s = SnapshotService()
        s._available = True
        s._bucket = "test-bucket"
        s._s3 = MagicMock()
        # §C3 adds a history-prune pass after every successful promote.
        # A bare, unconfigured MagicMock's paginator isn't iterable, so
        # give it an explicit empty page — these tests are about the
        # size-check gate, not pruning, and should exercise pruning as a
        # clean no-op rather than an incidentally-swallowed TypeError.
        s._s3.get_paginator.return_value.paginate.return_value = []
        s._set_snapshot_context = AsyncMock()
        return s

    @staticmethod
    def _statuses(mock_set_snapshot_context) -> list:
        return [
            call.args[1].get("status")
            for call in mock_set_snapshot_context.call_args_list
        ]

    @pytest.mark.asyncio
    async def test_size_mismatch_fails_closed_before_available(self, svc, tar_path):
        svc._s3.head_object = MagicMock(return_value={"ContentLength": 5})
        manifest = {"size_compressed_bytes": 999}  # != 5

        ok = await svc.upload_snapshot("job-1", tar_path, manifest)

        assert ok is False
        statuses = self._statuses(svc._set_snapshot_context)
        assert "capture_failed" in statuses
        assert "available" not in statuses
        # §C3: the HEAD-check now runs against the STAGING key, not
        # canonical directly, so a bad upload never touches canonical —
        # see TestUploadSnapshotNoClobber for the actual
        # canonical-survives-a-bad-upload proof against a real fake store.
        svc._s3.head_object.assert_called_once()
        called = svc._s3.head_object.call_args
        assert called.kwargs["Bucket"] == "test-bucket"
        assert called.kwargs["Key"].startswith("jobs/job-1/env.tar.zst.staging-")

    @pytest.mark.asyncio
    async def test_matching_size_proceeds_to_available(self, svc, tar_path):
        svc._s3.head_object = MagicMock(return_value={"ContentLength": len(_TAR_BYTES)})
        manifest = {"size_compressed_bytes": len(_TAR_BYTES)}

        ok = await svc.upload_snapshot("job-1", tar_path, manifest)

        assert ok is True
        statuses = self._statuses(svc._set_snapshot_context)
        assert "available" in statuses
        assert "capture_failed" not in statuses

    @pytest.mark.asyncio
    async def test_missing_size_in_manifest_skips_head_check(self, svc, tar_path):
        """No ``size_compressed_bytes`` in the manifest => nothing to
        compare against, so the post-upload check is a no-op and upload
        still succeeds (unchanged happy path for callers that don't set
        it).
        """
        svc._s3.head_object = MagicMock()
        manifest: dict = {}

        ok = await svc.upload_snapshot("job-1", tar_path, manifest)

        assert ok is True
        svc._s3.head_object.assert_not_called()


# =============================================================================
# upload_snapshot — §C3 no-clobber staging -> verify -> promote -> prune
# =============================================================================


class TestUploadSnapshotNoClobber:
    """The load-bearing C3 guarantee: a bad/truncated capture must never
    overwrite the last good canonical archive in place.
    """

    @pytest.mark.asyncio
    async def test_size_mismatch_leaves_good_canonical_untouched(
        self, fake_svc, fake_s3, tmp_path
    ):
        prefix = "jobs/job-1"
        fake_s3.store[f"{prefix}/env.tar.zst"] = b"GOOD"
        fake_s3.store[f"{prefix}/manifest.json"] = b'{"stale": "manifest"}'

        # Real bytes land in staging, but the manifest claims a size that
        # doesn't match them — simulating a truncated multipart upload
        # that the staging HEAD-check must catch before canonical is ever
        # touched.
        tar_path = _write_tar(tmp_path, "env.tar.zst", b"truncated-bytes")
        manifest = {"size_compressed_bytes": 999999}

        ok = await fake_svc.upload_snapshot("job-1", tar_path, manifest)

        assert ok is False
        # The core property: canonical is EXACTLY what it was before.
        assert fake_s3.store[f"{prefix}/env.tar.zst"] == b"GOOD"
        assert fake_s3.store[f"{prefix}/manifest.json"] == b'{"stale": "manifest"}'
        # No leftover staging object and no history generation created.
        assert _staging_keys(fake_s3, prefix) == []
        assert _history_generations(fake_s3, prefix) == set()

        statuses = [
            call.args[1].get("status")
            for call in fake_svc._set_snapshot_context.call_args_list
        ]
        assert statuses == ["capture_failed"]

    @pytest.mark.asyncio
    async def test_exception_during_history_copy_leaves_canonical_untouched(
        self, fake_svc, fake_s3, tmp_path
    ):
        """Defense in depth beyond the verify gate, and a direct regression
        guard for the history-first/canonical-last promote ordering: an
        unexpected failure raised while writing the ``history/<ts>/``
        generation (the FIRST promote step) must leave canonical
        completely untouched.

        The raise is keyed on the destination key containing ``/history/``
        — not "raise on the first call" — so this test can actually tell
        the two orderings apart. In the correct (history-first) order, the
        history copy happens first and canonical is never reached. If
        promote ever regressed to writing canonical before history,
        canonical would already hold the NEW bytes by the time this
        exception fires, and the byte-identical assertion below would
        catch that half-promoted state.
        """
        prefix = "jobs/job-1"
        fake_s3.store[f"{prefix}/env.tar.zst"] = b"GOOD"

        tar_path = _write_tar(tmp_path, "env.tar.zst", _TAR_BYTES)
        manifest = {"size_compressed_bytes": len(_TAR_BYTES)}

        real_copy = fake_s3.copy

        def _raise_on_history_copy(CopySource, Bucket, Key, **kwargs):
            if "/history/" in Key:
                raise ConnectionResetError("connection reset by peer")
            return real_copy(CopySource, Bucket, Key, **kwargs)

        fake_s3.copy = _raise_on_history_copy

        ok = await fake_svc.upload_snapshot("job-1", tar_path, manifest)

        assert ok is False
        # The core regression guard: canonical is BYTE-IDENTICAL to the
        # previously-seeded good object — never touched by the new
        # (also-good) bytes either, because the history write must fail
        # before canonical is ever reached.
        assert fake_s3.store[f"{prefix}/env.tar.zst"] == b"GOOD"
        assert _staging_keys(fake_s3, prefix) == []
        assert _history_generations(fake_s3, prefix) == set()

        statuses = [
            call.args[1].get("status")
            for call in fake_svc._set_snapshot_context.call_args_list
        ]
        assert "capture_failed" in statuses


class TestUploadSnapshotPromote:
    """Good-path promote: canonical updated, exactly one new history
    generation written, staging cleaned up.
    """

    @pytest.mark.asyncio
    async def test_good_upload_promotes_canonical_and_writes_one_generation(
        self, fake_svc, fake_s3, tmp_path
    ):
        prefix = "jobs/job-1"
        tar_path = _write_tar(tmp_path, "env.tar.zst", _TAR_BYTES)
        manifest = {
            "size_compressed_bytes": len(_TAR_BYTES),
            "created_at": "2026-01-01T00:00:00+00:00",
            "source_type": "vm",
        }

        ok = await fake_svc.upload_snapshot("job-1", tar_path, manifest)

        assert ok is True
        assert fake_s3.store[f"{prefix}/env.tar.zst"] == _TAR_BYTES

        generations = _history_generations(fake_s3, prefix)
        assert len(generations) == 1
        (generation,) = generations
        assert fake_s3.store[f"{prefix}/history/{generation}/env.tar.zst"] == _TAR_BYTES
        assert f"{prefix}/history/{generation}/manifest.json" in fake_s3.store

        assert _staging_keys(fake_s3, prefix) == []

        statuses = [
            call.args[1].get("status")
            for call in fake_svc._set_snapshot_context.call_args_list
        ]
        assert statuses == ["available"]


class TestTerminalSnapshotGeneration:
    GENERATION = "12345678-1234-4678-9abc-123456789abc"
    RUNTIME = "87654321-4321-4678-9abc-abcdefabcdef"
    FINGERPRINT = "SHA256:" + ("A" * 43)

    @staticmethod
    def _manifest(payload: bytes) -> dict:
        digest = hashlib.sha256(payload).hexdigest()
        return {
            "version": 1,
            "job_id": "job-1",
            "source_type": "pod",
            "created_at": "2026-08-13T01:02:03+00:00",
            "strict_terminal": True,
            "terminal_generation": TestTerminalSnapshotGeneration.GENERATION,
            "runtime_incarnation": TestTerminalSnapshotGeneration.RUNTIME,
            "ssh_host_key_fingerprint": TestTerminalSnapshotGeneration.FINGERPRINT,
            "size_compressed_bytes": len(payload),
            "sha256_compressed": digest,
            "checksum_sha256": digest,
        }

    @pytest.mark.asyncio
    async def test_command_key_replay_keeps_one_history_generation(
        self, fake_svc, fake_s3, tmp_path
    ):
        prefix = "jobs/job-1"
        payload = b"quiescent-terminal-archive"
        tar_path = _write_tar(tmp_path, "terminal.tar.zst", payload)

        for _ in range(2):
            assert await fake_svc.upload_snapshot(
                "job-1",
                tar_path,
                self._manifest(payload),
                terminal_generation=self.GENERATION,
            )

        assert _history_generations(fake_s3, prefix) == {
            f"completion-{self.GENERATION}"
        }

    @pytest.mark.asyncio
    async def test_complete_generation_repairs_canonical_before_success(
        self, fake_svc, fake_s3
    ):
        prefix = "jobs/job-1"
        history = f"{prefix}/history/completion-{self.GENERATION}"
        payload = b"strict-terminal"
        manifest = self._manifest(payload)
        fake_s3.store[f"{history}/env.tar.zst"] = payload
        fake_s3.store[f"{history}/manifest.json"] = json.dumps(manifest).encode()
        fake_s3.store[f"{prefix}/env.tar.zst"] = b"stale"
        fake_s3.store[f"{prefix}/manifest.json"] = b'{"stale":true}'
        fake_svc._streaming_sha256_from_s3 = MagicMock(
            return_value=hashlib.sha256(payload).hexdigest()
        )

        ok, state = await fake_svc.reconcile_terminal_snapshot_generation(
            "job-1",
            terminal_generation=self.GENERATION,
            expected_runtime_incarnation=self.RUNTIME,
            expected_host_key_fingerprint=self.FINGERPRINT,
        )

        assert (ok, state) == (True, "complete")
        assert fake_s3.store[f"{prefix}/env.tar.zst"] == payload
        assert json.loads(fake_s3.store[f"{prefix}/manifest.json"]) == manifest

    @pytest.mark.asyncio
    async def test_one_object_generation_is_partial_and_never_promoted(
        self, fake_svc, fake_s3
    ):
        prefix = "jobs/job-1"
        history = f"{prefix}/history/completion-{self.GENERATION}"
        fake_s3.store[f"{history}/env.tar.zst"] = b"orphaned-but-atomic"

        ok, state = await fake_svc.reconcile_terminal_snapshot_generation(
            "job-1",
            terminal_generation=self.GENERATION,
            expected_runtime_incarnation=self.RUNTIME,
            expected_host_key_fingerprint=self.FINGERPRINT,
        )

        assert (ok, state) == (False, "partial")
        assert f"{prefix}/env.tar.zst" not in fake_s3.store

    @pytest.mark.asyncio
    async def test_probe_permission_error_is_not_misclassified_as_missing(
        self, fake_svc, fake_s3
    ):
        fake_s3.head_object = MagicMock(
            side_effect=ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "denied"}},
                "HeadObject",
            )
        )

        ok, state = await fake_svc.reconcile_terminal_snapshot_generation(
            "job-1",
            terminal_generation=self.GENERATION,
            expected_runtime_incarnation=self.RUNTIME,
            expected_host_key_fingerprint=self.FINGERPRINT,
        )

        assert ok is False
        assert state.startswith("probe error:")


class TestUploadSnapshotHistoryPruning:
    """SNAPSHOT_KEEP_GENERATIONS bounds how many old generations survive."""

    @pytest.mark.asyncio
    async def test_keeps_only_newest_n_generations(
        self, fake_svc, fake_s3, tmp_path, monkeypatch
    ):
        monkeypatch.setenv("SNAPSHOT_KEEP_GENERATIONS", "2")
        prefix = "jobs/job-1"

        for day in ("01", "02", "03"):
            data = f"payload-{day}".encode()
            tar_path = _write_tar(tmp_path, f"env-{day}.tar.zst", data)
            manifest = {
                "size_compressed_bytes": len(data),
                "created_at": f"2026-01-{day}T00:00:00+00:00",
            }
            ok = await fake_svc.upload_snapshot("job-1", tar_path, manifest)
            assert ok is True

        generations = _history_generations(fake_s3, prefix)
        assert len(generations) == 2
        assert not any(g.startswith("2026-01-01T") for g in generations)
        assert any(g.startswith("2026-01-02T") for g in generations)
        assert any(g.startswith("2026-01-03T") for g in generations)

        # Canonical always reflects the LAST successful promote.
        assert fake_s3.store[f"{prefix}/env.tar.zst"] == b"payload-03"
        assert _staging_keys(fake_s3, prefix) == []


class TestUploadSnapshotBackwardCompat:
    """Restore reads (download_snapshot / get_manifest) are untouched:
    they still resolve the canonical keys after a §C3 promote.
    """

    @pytest.mark.asyncio
    async def test_download_snapshot_and_get_manifest_read_canonical_after_upload(
        self, fake_svc, fake_s3, tmp_path
    ):
        tar_path = _write_tar(tmp_path, "env.tar.zst", _TAR_BYTES)
        manifest = {
            "size_compressed_bytes": len(_TAR_BYTES),
            "created_at": "2026-01-01T00:00:00+00:00",
            "source_type": "vm",
        }

        ok = await fake_svc.upload_snapshot("job-1", tar_path, manifest)
        assert ok is True

        dest = tmp_path / "restored.tar.zst"
        downloaded = await fake_svc.download_snapshot("job-1", str(dest))
        assert downloaded is True
        assert dest.read_bytes() == _TAR_BYTES

        got_manifest = await fake_svc.get_manifest("job-1")
        assert got_manifest is not None
        assert got_manifest["size_compressed_bytes"] == len(_TAR_BYTES)
        assert got_manifest["source_type"] == "vm"
        assert "checksum_sha256" in got_manifest


class TestGetStorageStatsHistoryExclusion:
    """``history/`` generations must not inflate the snapshot count, but
    their bytes still consume real storage and must still be counted.
    """

    @pytest.mark.asyncio
    async def test_total_snapshots_excludes_history_but_bytes_include_it(
        self, fake_svc, fake_s3, tmp_path
    ):
        prefix = "jobs/job-1"
        tar_path = _write_tar(tmp_path, "env.tar.zst", _TAR_BYTES)
        manifest = {
            "size_compressed_bytes": len(_TAR_BYTES),
            "created_at": "2026-01-01T00:00:00+00:00",
        }

        ok = await fake_svc.upload_snapshot("job-1", tar_path, manifest)
        assert ok is True

        stats = await fake_svc.get_storage_stats()

        assert stats["total_snapshots"] == 1
        # Canonical tar + canonical manifest + history's own copies of
        # both — history bytes must still count toward storage even
        # though they're excluded from the snapshot count.
        canonical_bytes = len(fake_s3.store[f"{prefix}/env.tar.zst"])
        assert stats["total_size_bytes"] >= canonical_bytes * 2


class TestStoreReprobe:
    """A fresh ``helm install --wait`` probes before the Garage bootstrap
    hook grants the key: the first HeadBucket is a 403. The service keeps its
    configuration and comes up on a later probe without another connect().
    See the vault issue snapshot_service_startup_probe_never_retried.
    """

    @staticmethod
    def _forbidden() -> ClientError:
        return ClientError(
            {"Error": {"Code": "403", "Message": "Forbidden"}}, "HeadBucket"
        )

    @pytest.fixture
    def store(self, monkeypatch):
        from orchestrator.services import snapshot_service as module

        client = MagicMock()
        built = []

        def build(*args, **kwargs):
            built.append(kwargs["endpoint_url"])
            return client

        monkeypatch.setattr(module, "BOTO3_AVAILABLE", True)
        monkeypatch.setattr(module, "boto3", MagicMock(client=build))
        monkeypatch.setattr(module, "BotoConfig", MagicMock())
        monkeypatch.setenv("S3_ENDPOINT", "http://garage:3900")
        monkeypatch.setenv("S3_BUCKET", "srw-snapshots")
        return client, built

    @pytest.mark.asyncio
    async def test_a_403_then_200_flips_availability_without_another_connect(
        self, store
    ):
        client, built = store
        client.head_bucket.side_effect = [self._forbidden(), None]
        svc = SnapshotService()

        await svc.connect(db=None)

        assert svc.is_available is False
        assert svc.needs_reprobe is True
        assert svc._s3 is None

        assert await svc.reprobe() is True
        assert svc.is_available is True
        assert svc.needs_reprobe is False
        assert svc._s3 is client
        assert built == ["http://garage:3900", "http://garage:3900"]

    @pytest.mark.asyncio
    async def test_a_permanent_403_keeps_failing_with_a_rate_limited_warning(
        self, store, caplog
    ):
        client, _ = store
        client.head_bucket.side_effect = self._forbidden()
        svc = SnapshotService()
        await svc.connect(db=None)
        caplog.clear()
        caplog.set_level(logging.DEBUG, logger="orchestrator.services.snapshot_service")

        results = [await svc.reprobe() for _ in range(12)]

        assert results == [False] * 12
        assert svc.is_available is False
        assert svc._s3 is None
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert [r.getMessage().split(" failed")[0] for r in warnings] == [
            "Snapshot service: S3 re-probe 1",
            "Snapshot service: S3 re-probe 11",
        ]

    @pytest.mark.asyncio
    async def test_an_unconfigured_store_is_never_reprobed(self, monkeypatch):
        monkeypatch.delenv("S3_ENDPOINT", raising=False)
        svc = SnapshotService()

        await svc.connect(db=None)

        assert svc.needs_reprobe is False
        assert await svc.reprobe() is False
