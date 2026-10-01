"""Bounded diagnostic of same-recipient route publication contention.

The fake API enforces the Pod UID/resourceVersion JSON Patch tests and route
resource UID delete preconditions. The production prepare and router methods
run unchanged against it; synchronization controls only the order of Kube calls.
"""

import asyncio
from copy import deepcopy
import json
from threading import Event, Lock
from unittest.mock import AsyncMock, MagicMock

import pytest
from kubernetes.client.exceptions import ApiException

from orchestrator.routers import sessions
from orchestrator.services.session_router import (
    SessionRouteAuthorityError,
    SessionRouterService,
)
from shared.pinned_session_identity import PinnedSessionBinding


THREAD = "00000000-0000-4000-8000-0000000000a1"
GENERATION = "11111111-2222-4333-8444-555555555555"
POD_NAME = "srw-agent-abc"
POD_UID = "pod-uid-1"
ROUTE_NAME = f"session-{THREAD}"


class KubeBoundary:
    def __init__(self):
        self.pod = {
            "metadata": {
                "name": POD_NAME,
                "namespace": "srw",
                "uid": POD_UID,
                "resourceVersion": "7",
                "labels": {
                    "srw/component": "agent",
                    "srw/managed-by": "agent-provisioner",
                    "srw/purpose": "job",
                },
            },
            "status": {
                "phase": "Running", "podIP": "10.0.0.5",
                "containerStatuses": [{"name": "agent", "ready": True}],
            },
        }
        self.services = {}
        self.ingresses = {}
        self.pod_reads = []
        self.patch_tests = []
        self.history = []
        self.first_patch_entered = Event()
        self.second_patch_entered = Event()
        self.winner_published = Event()
        self.coordinate_patches = False
        self.patch_count = 0
        self.lock = Lock()
        self.force_unrelated_422 = False

    @staticmethod
    def _missing():
        raise ApiException(status=404)

    def read_namespaced_pod(self, *, name, namespace, **_kwargs):
        assert (name, namespace) == (POD_NAME, "srw")
        snapshot = deepcopy(self.pod)
        self.pod_reads.append(snapshot["metadata"]["resourceVersion"])
        return snapshot

    def patch_namespaced_pod(self, *, name, namespace, body, **_kwargs):
        assert (name, namespace) == (POD_NAME, "srw")
        tests = {op["path"]: op["value"] for op in body if op["op"] == "test"}
        self.patch_tests.append(tests)
        if self.coordinate_patches:
            with self.lock:
                self.patch_count += 1
                patch_number = self.patch_count
            if patch_number == 1:
                self.first_patch_entered.set()
                if not self.second_patch_entered.wait(10):
                    raise AssertionError("second patch did not arrive")
            elif patch_number == 2:
                self.second_patch_entered.set()
                if not self.winner_published.wait(10):
                    raise AssertionError("winner was not published")
        if self.force_unrelated_422:
            self.history.append("unrelated-422")
            raise ApiException(status=422, reason="unrelated admission validation")
        if (tests.get("/metadata/uid") != self.pod["metadata"]["uid"]
            or tests.get("/metadata/resourceVersion")
            != self.pod["metadata"]["resourceVersion"]):
            self.history.append("stale-or-uid-422")
            raise ApiException(status=422, reason="JSON Patch test failed")
        for op in body:
            if op["op"] == "add":
                label = op["path"].removeprefix("/metadata/labels/").replace("~1", "/")
                self.pod["metadata"]["labels"][label] = op["value"]
        self.pod["metadata"]["resourceVersion"] = "8"
        self.history.append("pod-patch-rv7-to-rv8")
        return deepcopy(self.pod)

    def _read_route(self, collection, *, name, namespace, **_kwargs):
        assert name == ROUTE_NAME
        if namespace != "srw" or name not in collection:
            self._missing()
        return deepcopy(collection[name])

    def read_namespaced_service(self, **kwargs):
        return self._read_route(self.services, **kwargs)

    def read_namespaced_ingress(self, **kwargs):
        return self._read_route(self.ingresses, **kwargs)

    def _create_route(self, collection, kind, *, namespace, body, **_kwargs):
        assert namespace == "srw"
        name = body["metadata"]["name"]
        if name in collection:
            raise ApiException(status=409)
        item = deepcopy(body)
        item["metadata"]["uid"] = f"{kind}-uid-1"
        collection[name] = item
        self.history.append(f"create-{kind}")
        return deepcopy(item)

    def create_namespaced_service(self, **kwargs):
        return self._create_route(self.services, "service", **kwargs)

    def create_namespaced_ingress(self, **kwargs):
        return self._create_route(self.ingresses, "ingress", **kwargs)

    def _delete_route(self, collection, kind, *, name, namespace, body, **_kwargs):
        assert namespace == "srw"
        item = collection.get(name)
        if item is None:
            self._missing()
        assert body["preconditions"]["uid"] == item["metadata"]["uid"]
        del collection[name]
        self.history.append(f"delete-{kind}")

    def delete_namespaced_service(self, **kwargs):
        return self._delete_route(self.services, "service", **kwargs)

    def delete_namespaced_ingress(self, **kwargs):
        return self._delete_route(self.ingresses, "ingress", **kwargs)


def make_binding(**changes):
    values = dict(
        thread_id=THREAD, runtime_generation=GENERATION,
        agent_id="00000000-0000-4000-8000-0000000000a2",
        runtime_attach_token="00000000-0000-4000-8000-0000000000a3",
        agent_hostname=POD_NAME, pod_namespace="srw", pod_uid=POD_UID,
        pod_ip="10.0.0.5", pod_port=8001, agent_status="session",
    )
    values.update(changes)
    return PinnedSessionBinding(**values)


def make_service(kube, db):
    return SessionRouterService(
        namespace="srw", ingress_host="api.example.com", db=db,
        core_api=kube, networking_api=kube,
    )


@pytest.mark.asyncio
async def test_same_recipient_stale_rv_preserves_winner_and_emits_no_failure(monkeypatch):
    kube = KubeBoundary()
    kube.coordinate_patches = True
    binding = make_binding()
    db = MagicMock()
    db.get_pinned_session_binding = AsyncMock(return_value=binding)
    db.get_thread = AsyncMock(return_value={
        "id": THREAD, "execution_lane": "pinned", "status": "created",
        "user_id": "u1", "agent_id": binding.agent_id,
        "runtime_generation": GENERATION,
        "runtime_attach_token": binding.runtime_attach_token,
        "runtime_retirement_token": None, "metadata": {},
    })
    advisory_lock = asyncio.Lock()
    db.thread_advisory_lock = MagicMock(return_value=advisory_lock)
    service = make_service(kube, db)
    async def ready(*_args, **_kwargs):
        return True
    monkeypatch.setattr(sessions, "wait_for_ready", ready)
    events = []
    monkeypatch.setattr(
        sessions, "lifecycle_emit",
        lambda _uid, _tid, state, **extra: events.append({"state": state, **extra}),
    )
    noop = AsyncMock(return_value=None)
    deps = sessions.SessionsDependencies(
        store=db, agent_provisioner=MagicMock(), container_provisioner=MagicMock(),
        workspace_suspension_service=MagicMock(), session_router=service,
        session_tokens=MagicMock(), ensure_session_workspace=noop,
        await_late_cloud_setup=noop,
        await_protected_cloud_runtime_ready=AsyncMock(return_value=True),
        session_grant_violations=AsyncMock(return_value=[]),
        session_endpoint_violations=AsyncMock(return_value=[]),
        find_idle_persistent_agent=AsyncMock(return_value=None),
        send_session_attach=AsyncMock(return_value=True),
    )
    authority = sessions.ThreadRuntimeAuthority(thread_id=THREAD, generation=GENERATION)
    async def prepare():
        return await sessions._do_prepare(
            THREAD, "u1", "session_base", None, authority, dependencies=deps,
        )

    winner = asyncio.create_task(prepare())
    assert await asyncio.to_thread(kube.first_patch_entered.wait, 10)
    loser = asyncio.create_task(prepare())
    assert await asyncio.wait_for(winner, 10) is True
    before_loser = {
        "service": ROUTE_NAME in kube.services,
        "ingress": ROUTE_NAME in kube.ingresses,
        "events": [event["state"] for event in events],
    }
    assert before_loser["service"] and before_loser["ingress"]
    kube.winner_published.set()
    await asyncio.wait_for(loser, 10)
    actual = {
        "pod_uid": kube.pod["metadata"]["uid"],
        "pod_rv": kube.pod["metadata"]["resourceVersion"],
        "patch_tests": kube.patch_tests,
        "before_loser": before_loser,
        "after_loser": {
            "service": ROUTE_NAME in kube.services,
            "ingress": ROUTE_NAME in kube.ingresses,
            "events": [event["state"] for event in events],
        },
        "history": kube.history,
    }
    print("DIAGNOSTIC " + json.dumps(actual, sort_keys=True))
    assert all(t["/metadata/uid"] == POD_UID for t in kube.patch_tests)
    assert [t["/metadata/resourceVersion"] for t in kube.patch_tests] == ["7", "7"]
    assert kube.history.count("stale-or-uid-422") == 1
    assert (
        actual["after_loser"]["service"],
        actual["after_loser"]["ingress"],
        actual["after_loser"]["events"].count("failed"),
    ) == (True, True, 0), actual


@pytest.mark.asyncio
async def test_unrelated_422_and_changed_recipient_fail_closed():
    for mode in (
        "unrelated-422", "changed-uid", "changed-generation", "foreign-owner",
    ):
        kube = KubeBoundary()
        db = MagicMock()
        db.get_pinned_session_binding = AsyncMock(return_value=make_binding())
        service = make_service(kube, db)
        if mode == "unrelated-422":
            kube.force_unrelated_422 = True
        elif mode == "changed-uid":
            kube.pod["metadata"]["uid"] = "replacement-pod-uid"
        else:
            if mode == "changed-generation":
                db.get_pinned_session_binding.return_value = make_binding(
                    runtime_generation="22222222-2222-4222-8222-222222222222"
                )
            else:
                kube.services[ROUTE_NAME] = service._service_body(
                    THREAD, ROUTE_NAME, POD_NAME, "foreign-pod-uid",
                    GENERATION, namespace="srw",
                )
                kube.services[ROUTE_NAME]["metadata"]["uid"] = "foreign-service-uid"
        prior_services = deepcopy(kube.services)
        with pytest.raises(SessionRouteAuthorityError):
            await service.ensure_route(THREAD, POD_NAME, POD_UID, GENERATION)
        assert kube.services == prior_services and kube.ingresses == {}, mode
        print("CONTROL " + json.dumps({"mode": mode, "history": kube.history}))


@pytest.mark.asyncio
async def test_old_generation_cleanup_preserves_successor_route():
    kube = KubeBoundary()
    db = MagicMock()
    db.get_pinned_session_binding = AsyncMock(return_value=make_binding())
    service = make_service(kube, db)
    kube.services[ROUTE_NAME] = service._service_body(
        THREAD, ROUTE_NAME, POD_NAME, "successor-pod-uid",
        "22222222-2222-4222-8222-222222222222", namespace="srw",
    )
    kube.ingresses[ROUTE_NAME] = service._ingress_body(
        THREAD, ROUTE_NAME, POD_NAME, "successor-pod-uid",
        "22222222-2222-4222-8222-222222222222", namespace="srw",
    )
    kube.services[ROUTE_NAME]["metadata"]["uid"] = "successor-service-uid"
    kube.ingresses[ROUTE_NAME]["metadata"]["uid"] = "successor-ingress-uid"
    assert await service.teardown_route(
        THREAD, expected_namespace="srw", expected_runtime_generation=GENERATION,
        expected_owner_uid=POD_UID,
    )
    assert ROUTE_NAME in kube.services and ROUTE_NAME in kube.ingresses
    assert kube.history == []
