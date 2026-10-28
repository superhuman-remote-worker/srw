"""A permanent End whose cloud installation is unresolvable answers a typed 503.

An orchestrator that started before its Nextcloud has no cached instance for
the session's folder, so ``MainCloudRouter.for_thread`` raises
``FeatureNotAvailable``. That used to escape ``_delete_auxiliary_state`` as a
500 on the stateless lane. It is now a 503 with a code the caller can retry,
raised before the thread row is deleted. See the vault issue
main_cloud_authority_never_retried_after_startup_race.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

from fastapi import HTTPException
import pytest

from orchestrator.services.cloud import MainCloudRouter, SessionFolderHandle
from orchestrator.services.cloud.errors import FeatureNotAvailable
from orchestrator.services.thread_retirement import delete_main_cloud_session_folder
from tests.cloud.fake import FakeMainCloudBackend

INSTANCE = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
LOG = logging.getLogger("test.thread_retirement")


def _thread(**over):
    handle = SessionFolderHandle(backend="nextcloud", native_id="42").to_db()
    row = {
        "id": "11111111-1111-4111-8111-111111111111",
        "main_cloud_session_handle": handle,
        "main_cloud_backend": "nextcloud",
        "main_cloud_backend_instance_id": INSTANCE,
    }
    row.update(over)
    return row


@pytest.mark.asyncio
async def test_an_uncached_installation_is_a_typed_503_not_a_500():
    active = FakeMainCloudBackend()
    active.backend_id = "nextcloud"
    router = MainCloudRouter(active)
    with pytest.raises(FeatureNotAvailable):
        router.for_thread(_thread())  # the replica never cached the instance

    with pytest.raises(HTTPException) as refused:
        await delete_main_cloud_session_folder(
            _thread(), main_cloud_router=router, logger=LOG
        )

    assert refused.value.status_code == 503
    assert refused.value.detail["code"] == "main_cloud_unavailable"
    assert "retry" in refused.value.detail["message"]


@pytest.mark.asyncio
async def test_the_resolved_backend_deletes_the_folder():
    backend = SimpleNamespace(
        is_initialized=True,
        backend_id="nextcloud",
        delete_session_folder=AsyncMock(),
    )
    router = SimpleNamespace(for_thread=lambda thread: backend)

    await delete_main_cloud_session_folder(
        _thread(), main_cloud_router=router, logger=LOG
    )

    (handle,) = backend.delete_session_folder.await_args.args
    assert handle.native_id == "42"


@pytest.mark.asyncio
async def test_a_failed_remote_delete_only_logs(caplog):
    backend = SimpleNamespace(
        is_initialized=True,
        backend_id="nextcloud",
        delete_session_folder=AsyncMock(side_effect=RuntimeError("502")),
    )
    router = SimpleNamespace(for_thread=lambda thread: backend)

    await delete_main_cloud_session_folder(
        _thread(), main_cloud_router=router, logger=LOG
    )

    assert "Failed to delete main-cloud session folder" in caplog.text


@pytest.mark.asyncio
async def test_a_thread_without_a_folder_never_resolves_a_backend():
    def unreachable(thread):
        raise AssertionError("no folder, no backend lookup")

    await delete_main_cloud_session_folder(
        _thread(main_cloud_session_handle=None),
        main_cloud_router=SimpleNamespace(for_thread=unreachable),
        logger=LOG,
    )
