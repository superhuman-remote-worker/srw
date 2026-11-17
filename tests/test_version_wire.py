"""``GET /api/version``: what installation a client is talking to.

A router of its own, so that it does not sit beside ``/api/auth/me`` and
inherit that route's classification by proximity. It requires an identity
(``get_current_user``) and, like the auth bootstrap, no approval: it says
nothing about the caller.
"""

import httpx
import pytest
from fastapi import HTTPException

from orchestrator.routers import version as version_routes
from orchestrator.security.token_scopes import ANY, classify_route
from orchestrator.services import installation
from tests._mounted_router import mount_router
from tests.test_identity_wire import Harness, user_row


@pytest.fixture
def harness():
    return Harness()


def test_the_route_is_the_only_one_on_its_router():
    assert {
        (route.path, tuple(sorted(route.methods)))
        for route in version_routes.router.routes
    } == {("/api/version", ("GET",))}


def test_any_scoped_token_may_read_it_and_nothing_else_there():
    assert classify_route("GET", "/api/version") == ANY
    assert classify_route("POST", "/api/version") != ANY


def test_server_version_reads_what_the_image_was_built_with():
    assert installation.server_version(
        {
            "SRW_RELEASE_VERSION": "0.3.0",
            "SRW_SOURCE_REVISION": "abc1234",
            "COCKPIT_EXTERNAL_URL": "https://cockpit.example.com/",
        }
    ) == {
        "release_version": "0.3.0",
        "source_revision": "abc1234",
        "manifest_api_versions": ["srw/v1alpha1"],
        "cockpit_url": "https://cockpit.example.com",
    }


def test_a_process_from_a_checkout_says_so():
    # The same words the MCP server's health report uses; and no Cockpit
    # address is invented when the installation names none.
    assert installation.server_version({}) == {
        "release_version": "development",
        "source_revision": "unavailable",
        "manifest_api_versions": ["srw/v1alpha1"],
        "cockpit_url": None,
    }


@pytest.mark.asyncio
async def test_version_over_http_needs_an_identity_and_no_approval(harness):
    harness.user = user_row(is_approved=False)
    dependencies = version_routes.VersionDependencies(
        store=object(),
        get_current_user=harness.get_current_user,
        server_version=lambda: installation.server_version(
            {"SRW_RELEASE_VERSION": "9.9.9"}
        ),
    )
    app = mount_router(
        version_routes.router,
        factories={"version_dependencies_factory": lambda: dependencies},
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        answered = await client.get("/api/version")
        harness.error = HTTPException(status_code=401, detail="Not authenticated")
        refused = await client.get("/api/version")
    assert answered.status_code == 200, answered.text
    assert answered.json()["release_version"] == "9.9.9"
    assert "user" not in answered.json()
    assert refused.status_code == 401
