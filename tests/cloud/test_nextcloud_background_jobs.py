"""``NextcloudBackend.background_jobs_status``: the admin Main cloud page's
cron health (bundled_nextcloud_performance.md, D13).

The adapter reads ``core/backgroundjobs_mode`` and ``core/lastcron`` as admin
through the provisioning API's app config (OCS v2). The bodies below are the
shapes a live Nextcloud 31 returns: ``{"ocs": {"meta": {"statuscode": 200},
"data": {"data": "cron"}}}``, and ``lastcron`` is unix seconds as a string, or
``""`` before the first ``cron.php`` run. Any failure is ``None``: the page
reports, it never raises.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

import httpx
import pytest

from orchestrator.services.cloud import (
    NextcloudBackend,
    OpenCloudBackend,
    SupportsBackgroundJobsStatus,
)
from orchestrator.services.cloud.config import NextcloudSettings

BASE = "https://nc.example.com"
CONFIG = "/ocs/v2.php/apps/provisioning_api/api/v1/config/apps/core/"


def _ocs(value: object, *, statuscode: int = 200) -> dict:
    return {
        "ocs": {
            "meta": {"status": "ok", "statuscode": statuscode, "message": "OK"},
            "data": {"data": value},
        }
    }


def _backend(
    *, mode: object = "cron", lastcron: object = "", handler=None
) -> tuple[NextcloudBackend, list[httpx.Request]]:
    seen: list[httpx.Request] = []

    def answer(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        key = request.url.path.removeprefix(CONFIG)
        value = {"backgroundjobs_mode": mode, "lastcron": lastcron}[key]
        return httpx.Response(200, json=_ocs(value))

    be = NextcloudBackend(
        NextcloudSettings(
            base_url=BASE,
            public_url=BASE,
            admin_user="admin",
            admin_password="admin",
            agent_user="agent-service",
            agent_password="agent-service-dev",
        )
    )
    be._client = httpx.AsyncClient(
        base_url=BASE,
        headers={"OCS-APIRequest": "true"},
        transport=httpx.MockTransport(handler or answer),
    )
    be._initialized = True
    return be, seen


def _ago(seconds: float) -> str:
    return str(int(time.time() - seconds))


def test_nextcloud_offers_it_and_opencloud_does_not():
    assert issubclass(NextcloudBackend, SupportsBackgroundJobsStatus)
    assert not hasattr(OpenCloudBackend, "background_jobs_status")


@pytest.mark.asyncio
async def test_a_healthy_cron_reads_both_keys_as_admin_over_ocs():
    lastcron = _ago(60)
    be, seen = _backend(mode="cron", lastcron=lastcron)

    status = await be.background_jobs_status()

    assert status == {
        "mode": "cron",
        "last_run_at": datetime.fromtimestamp(int(lastcron), timezone.utc).isoformat(),
        "stale": False,
    }
    assert [r.url.path for r in seen] == [
        CONFIG + "backgroundjobs_mode",
        CONFIG + "lastcron",
    ]
    assert all(r.url.params["format"] == "json" for r in seen)
    assert all(r.headers["OCS-APIRequest"] == "true" for r in seen)


@pytest.mark.asyncio
async def test_cron_without_a_run_in_15_minutes_is_stale():
    be, _ = _backend(mode="cron", lastcron=_ago(16 * 60))
    status = await be.background_jobs_status()
    assert status["stale"] is True
    assert status["last_run_at"] is not None


@pytest.mark.asyncio
async def test_a_run_within_15_minutes_is_not_stale():
    be, _ = _backend(mode="cron", lastcron=_ago(14 * 60))
    assert (await be.background_jobs_status())["stale"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("lastcron", ["", "0", None])
async def test_cron_that_never_ran_is_stale(lastcron):
    be, _ = _backend(mode="cron", lastcron=lastcron)
    assert await be.background_jobs_status() == {
        "mode": "cron",
        "last_run_at": None,
        "stale": True,
    }


@pytest.mark.asyncio
async def test_a_numeric_lastcron_is_read_too():
    be, _ = _backend(mode="cron", lastcron=int(time.time()) - 30)
    assert (await be.background_jobs_status())["stale"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["ajax", "webcron", "none"])
async def test_only_cron_can_be_stale(mode):
    be, _ = _backend(mode=mode, lastcron="")
    assert await be.background_jobs_status() == {
        "mode": mode,
        "last_run_at": None,
        "stale": False,
    }


@pytest.mark.asyncio
async def test_an_unset_mode_is_nextclouds_default_ajax():
    be, _ = _backend(mode="", lastcron=_ago(60))
    assert (await be.background_jobs_status())["mode"] == "ajax"


@pytest.mark.asyncio
async def test_an_unknown_mode_is_none_and_never_stale():
    be, _ = _backend(mode="hourly", lastcron="")
    status = await be.background_jobs_status()
    assert status["mode"] is None
    assert status["stale"] is False


@pytest.mark.asyncio
async def test_a_garbage_lastcron_is_no_run():
    be, _ = _backend(mode="cron", lastcron="yesterday")
    status = await be.background_jobs_status()
    assert status["last_run_at"] is None
    assert status["stale"] is True


def _refusing(response: httpx.Response):
    return lambda _request: response


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        # Not an admin (OCS v2 mirrors the status in HTTP).
        httpx.Response(403, json=_ocs(None, statuscode=403)),
        # An HTTP 200 whose OCS status is a refusal.
        httpx.Response(200, json=_ocs(None, statuscode=997)),
        # A login page instead of OCS JSON.
        httpx.Response(200, text="<!DOCTYPE html><html></html>"),
        # OCS data that is not an object.
        httpx.Response(200, json={"ocs": {"meta": {"statuscode": 200}, "data": []}}),
    ],
    ids=["http-403", "ocs-refusal", "html", "list-data"],
)
async def test_any_refusal_or_unexpected_shape_is_none(response):
    be, _ = _backend(handler=_refusing(response))
    assert await be.background_jobs_status() is None


@pytest.mark.asyncio
async def test_a_transport_error_is_none():
    def boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    be, _ = _backend(handler=boom)
    assert await be.background_jobs_status() is None


@pytest.mark.asyncio
async def test_an_uninitialized_backend_sends_nothing():
    be, seen = _backend()
    be._initialized = False
    assert await be.background_jobs_status() is None
    assert seen == []
