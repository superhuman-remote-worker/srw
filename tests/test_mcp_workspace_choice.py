"""Slice A3: the `workspace` argument of the job and session creation tools."""

import importlib
import json
import os

os.environ.setdefault("MCP_TRANSPORT", "stdio")

from unittest.mock import AsyncMock, patch  # noqa: E402

import httpx  # noqa: E402
import pytest  # noqa: E402

from shared.orch_surface.client import AsyncCockpitClient  # noqa: E402
from shared.orch_surface.jobs import CallerCtx, get_descriptor, make_bound_handler  # noqa: E402
from shared.orch_surface.workspace_choice import WorkspaceArgumentError, workspace_field  # noqa: E402

_server = importlib.import_module("mcp_server.server")
USER = "0a1b2c3d-0000-4000-8000-000000000001"


def _item(name, scope):
    return {
        "resource": {
            "metadata": {"name": name, "scope": scope},
            "spec": {"backend": "sandbox"},
        },
        "uid": f"u-{name}",
    }


class _FakeLister:
    def __init__(self, listings):
        self.listings = listings
        self.calls = []

    async def list_manifest_resources(self, *, scope_kind, scope_name, kind=None):
        self.calls.append((scope_kind, scope_name, kind))
        value = self.listings.get((scope_kind, scope_name), {"resources": []})
        if isinstance(value, int):
            request = httpx.Request("GET", "http://t/api/resources")
            raise httpx.HTTPStatusError(
                "refused",
                request=request,
                response=httpx.Response(value, request=request),
            )
        return value


@pytest.mark.asyncio
async def test_omitted_is_not_sent():
    assert await workspace_field(_FakeLister({}), None, project_id=None) == (
        False,
        None,
    )


@pytest.mark.asyncio
async def test_none_means_no_workspace():
    assert await workspace_field(_FakeLister({}), "none", project_id="p-1") == (
        True,
        None,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ["None", "NONE", " none "])
async def test_none_is_case_insensitive(value):
    assert await workspace_field(_FakeLister({}), value, project_id="p-1") == (
        True,
        None,
    )


@pytest.mark.asyncio
async def test_project_template_wins_over_shared():
    lister = _FakeLister(
        {
            ("Project", "p-1"): {
                "resources": [_item("web", {"kind": "Project", "name": "p-1"})]
            },
            ("Catalog", "shared"): {
                "resources": [_item("web", {"kind": "Catalog", "name": "shared"})]
            },
        }
    )
    assert await workspace_field(lister, "web", project_id="p-1") == (
        True,
        {
            "template": {
                "ref": {"name": "web", "scope": {"kind": "Project", "name": "p-1"}}
            }
        },
    )
    assert lister.calls == [("Project", "p-1", "WorkspaceTemplate")]


@pytest.mark.asyncio
async def test_account_template_keeps_its_stored_scope():
    lister = _FakeLister(
        {
            ("Account", "me"): {
                "resources": [_item("lean", {"kind": "Account", "name": USER})]
            }
        }
    )
    _, binding = await workspace_field(lister, "lean", project_id=None)
    assert binding == {
        "template": {
            "ref": {"name": "lean", "scope": {"kind": "Account", "name": USER}}
        }
    }


@pytest.mark.asyncio
async def test_unreadable_scope_is_skipped():
    lister = _FakeLister(
        {
            ("Account", "me"): 403,
            ("Catalog", "shared"): {
                "resources": [
                    _item("container-minimal", {"kind": "Catalog", "name": "shared"})
                ]
            },
        }
    )
    _, binding = await workspace_field(lister, "container-minimal", project_id=None)
    assert binding["template"]["ref"]["scope"] == {"kind": "Catalog", "name": "shared"}


@pytest.mark.asyncio
async def test_unknown_name_points_to_manifest_list():
    with pytest.raises(
        WorkspaceArgumentError, match="manifest_list kind=WorkspaceTemplate"
    ):
        await workspace_field(_FakeLister({}), "nope", project_id=None)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value",
    [
        {
            "template": {
                "inline": {"backend": "sandbox", "environment": {"image": "anything:1"}}
            }
        },
        {"instanceRef": {"uid": "x"}},
        {"template": {"ref": {"name": "a"}}, "extra": 1},
        {
            "template": {
                "ref": {"name": "a", "scope": {"kind": "Catalog", "name": "shared"}},
                "inline": {"backend": "sandbox"},
            }
        },
        {"template": {"ref": "a"}},
        "  ",
        42,
    ],
)
async def test_other_shapes_are_refused(value):
    with pytest.raises(WorkspaceArgumentError, match="manifest_apply"):
        await workspace_field(_FakeLister({}), value, project_id=None)


@pytest.mark.asyncio
async def test_json_string_is_treated_as_an_unknown_template_name():
    # A JSON-encoded dict is not parsed: the whole string is looked up as a name.
    with pytest.raises(
        WorkspaceArgumentError, match="manifest_list kind=WorkspaceTemplate"
    ):
        await workspace_field(
            _FakeLister({}), '{"template": {"ref": {"name": "a"}}}', project_id=None
        )


@pytest.mark.asyncio
async def test_a_failing_lookup_is_not_skipped():
    lister = _FakeLister({("Account", "me"): 500})
    with pytest.raises(httpx.HTTPStatusError):
        await workspace_field(lister, "x", project_id=None)


@pytest.mark.asyncio
async def test_ref_dict_passes_through():
    ref = {
        "template": {
            "ref": {"name": "a", "scope": {"kind": "Catalog", "name": "shared"}}
        }
    }
    assert await workspace_field(_FakeLister({}), ref, project_id=None) == (True, ref)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("supplied", "workspace", "expected"),
    [
        (False, None, "absent"),
        (True, None, None),
        (
            True,
            {"template": {"ref": {"name": "a"}}},
            {"template": {"ref": {"name": "a"}}},
        ),
    ],
)
async def test_clients_send_the_field_only_when_supplied(supplied, workspace, expected):
    bodies = []

    async def handler(request):
        bodies.append(json.loads(request.content))
        return httpx.Response(
            201, json={"id": "job-1", "thread_id": "t-1", "status": "created"}
        )

    client = AsyncCockpitClient(
        "http://orchestrator.test", transport=httpx.MockTransport(handler)
    )
    try:
        await client.create_project_job(
            project_id="p-1",
            description="d",
            workspace=workspace,
            workspace_supplied=supplied,
        )
        await client.create_job(
            description="d", workspace=workspace, workspace_supplied=supplied
        )
        await client.create_persistent_thread(
            workspace=workspace, workspace_supplied=supplied
        )
    finally:
        await client.close()
    assert len(bodies) == 3
    for body in bodies:
        if expected == "absent":
            assert "workspace" not in body
        else:
            assert body["workspace"] == expected


@pytest.mark.asyncio
async def test_create_persistent_thread_resolves_none():
    mock = AsyncMock()
    mock.create_persistent_thread.return_value = {
        "thread_id": "t-1",
        "status": "created",
    }
    with patch.object(_server, "_get_client", return_value=mock):
        await _server.create_persistent_thread(title="T", workspace="none")
    kwargs = mock.create_persistent_thread.await_args.kwargs
    assert kwargs["workspace"] is None
    assert kwargs["workspace_supplied"] is True


@pytest.mark.asyncio
async def test_create_persistent_thread_refuses_inline_recipes():
    mock = AsyncMock()
    with patch.object(_server, "_get_client", return_value=mock):
        result = await _server.create_persistent_thread(
            workspace={"template": {"inline": {"backend": "sandbox"}}}
        )
    assert "manifest_apply" in result
    mock.create_persistent_thread.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_project_job_looks_up_a_name_in_the_project_first():
    mock = AsyncMock()
    mock.list_manifest_resources.return_value = {
        "resources": [_item("web", {"kind": "Project", "name": "p-1"})]
    }
    mock.create_project_job.return_value = {"id": "job-1", "status": "created"}
    with patch.object(_server, "_get_client", return_value=mock):
        await _server.create_project_job(
            project_id="p-1", description="d", workspace="web"
        )
    kwargs = mock.create_project_job.await_args.kwargs
    assert kwargs["workspace"] == {
        "template": {
            "ref": {"name": "web", "scope": {"kind": "Project", "name": "p-1"}}
        }
    }
    assert kwargs["workspace_supplied"] is True


@pytest.mark.asyncio
async def test_create_job_descriptor_sends_the_looked_up_ref():
    posted = []

    async def handler(request):
        if request.method == "GET" and request.url.path == "/api/resources":
            if request.url.params["scope_kind"] == "Catalog":
                return httpx.Response(
                    200,
                    json={
                        "resources": [
                            _item(
                                "container-minimal",
                                {"kind": "Catalog", "name": "shared"},
                            )
                        ]
                    },
                )
            return httpx.Response(200, json={"resources": []})
        assert request.url.path == "/api/jobs"
        posted.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "status": "created",
                "config_name": "worker_base",
            },
        )

    client = AsyncCockpitClient(
        base_url="http://orchestrator.test", transport=httpx.MockTransport(handler)
    )
    invoke = make_bound_handler(
        get_descriptor("create_job"),
        client_provider=lambda: client,
        caller_provider=lambda: CallerCtx(kind="session", user_id="user-1"),
    )
    try:
        await invoke(description="run on the lean image", workspace="container-minimal")
    finally:
        await client.close()
    (body,) = posted
    assert body["workspace"] == {
        "template": {
            "ref": {
                "name": "container-minimal",
                "scope": {"kind": "Catalog", "name": "shared"},
            }
        }
    }
