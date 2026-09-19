"""Exact-recipient tests for session Service + Ingress publication."""

from copy import deepcopy
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.services.session_router import (
    SessionRouteAuthorityError,
    SessionRouterService,
)
from shared.pinned_session_identity import PinnedSessionBinding

THREAD_ID = "00000000-0000-4000-8000-0000000000a1"
OTHER_THREAD_ID = "00000000-0000-4000-8000-0000000000b1"
AGENT_ID = "00000000-0000-4000-8000-0000000000a2"
ATTACH_TOKEN = "00000000-0000-4000-8000-0000000000a3"
RUNTIME_GENERATION = "11111111-2222-4333-8444-555555555555"
POD_NAME = "srw-agent-abc"
POD_UID = "pod-uid-1"
POD_IP = "10.0.0.5"


def _not_found():
    from kubernetes.client.exceptions import ApiException

    return ApiException(status=404)


def _binding(**changes) -> PinnedSessionBinding:
    values = {
        "thread_id": THREAD_ID,
        "runtime_generation": RUNTIME_GENERATION,
        "agent_id": AGENT_ID,
        "runtime_attach_token": ATTACH_TOKEN,
        "agent_hostname": POD_NAME,
        "pod_namespace": "srw",
        "pod_uid": POD_UID,
        "pod_ip": POD_IP,
        "pod_port": 8001,
        "agent_status": "session",
    }
    values.update(changes)
    return PinnedSessionBinding(**values)


def _pod(*, uid=POD_UID, ip=POD_IP, route_labels=False, resource_version="7"):
    labels = {
        "srw/component": "agent",
        "srw/managed-by": "agent-provisioner",
        "srw/purpose": "session" if route_labels else "job",
    }
    if route_labels:
        labels.update(
            {
                "srw.io/thread-id": THREAD_ID,
                "srw/thread-id": THREAD_ID[:12],
                "srw.io/runtime-generation": RUNTIME_GENERATION,
            }
        )
    return {
        "metadata": {
            "name": POD_NAME,
            "namespace": "srw",
            "uid": uid,
            "resourceVersion": resource_version,
            "labels": labels,
        },
        "status": {
            "phase": "Running",
            "podIP": ip,
            "containerStatuses": [{"name": "agent", "ready": True}],
        },
    }


@pytest.fixture
def db():
    value = MagicMock()
    value.get_pinned_session_binding = AsyncMock(return_value=_binding())
    return value


@pytest.fixture
def k8s_core_api():
    api = MagicMock()
    pod = _pod()
    services = {}

    def read_pod(**_kwargs):
        return deepcopy(pod)

    def patch_pod(*, body, **_kwargs):
        for operation in body:
            if operation["op"] != "add":
                continue
            path = operation["path"]
            if path == "/metadata/labels/srw.io~1thread-id":
                pod["metadata"]["labels"]["srw.io/thread-id"] = operation["value"]
            elif path == "/metadata/labels/srw~1thread-id":
                pod["metadata"]["labels"]["srw/thread-id"] = operation["value"]
            elif path == "/metadata/labels/srw~1purpose":
                pod["metadata"]["labels"]["srw/purpose"] = operation["value"]
            elif path == "/metadata/labels/srw.io~1runtime-generation":
                pod["metadata"]["labels"]["srw.io/runtime-generation"] = operation[
                    "value"
                ]
        return deepcopy(pod)

    def read_service(*, name, **_kwargs):
        if name not in services:
            raise _not_found()
        return deepcopy(services[name])

    def create_service(*, body, **_kwargs):
        resource = deepcopy(body)
        resource["metadata"]["uid"] = "service-uid"
        services[resource["metadata"]["name"]] = resource
        return deepcopy(resource)

    def patch_service(*, name, body, **_kwargs):
        resource = services[name]
        for operation in body:
            if operation["path"] == "/metadata/labels/srw.io~1runtime-generation":
                resource["metadata"]["labels"]["srw.io/runtime-generation"] = operation[
                    "value"
                ]
            elif operation["path"] == "/metadata/ownerReferences":
                resource["metadata"]["ownerReferences"] = operation["value"]
            elif operation["path"] == "/spec/selector":
                resource["spec"]["selector"] = operation["value"]
        return deepcopy(resource)

    api._pod = pod
    api._services = services
    api.read_namespaced_pod.side_effect = read_pod
    api.patch_namespaced_pod.side_effect = patch_pod
    api.read_namespaced_service.side_effect = read_service
    api.create_namespaced_service.side_effect = create_service
    api.patch_namespaced_service.side_effect = patch_service
    return api


@pytest.fixture
def k8s_networking_api():
    api = MagicMock()
    ingresses = {}

    def read_ingress(*, name, **_kwargs):
        if name not in ingresses:
            raise _not_found()
        return deepcopy(ingresses[name])

    def create_ingress(*, body, **_kwargs):
        resource = deepcopy(body)
        resource["metadata"]["uid"] = "ingress-uid"
        ingresses[resource["metadata"]["name"]] = resource
        return deepcopy(resource)

    def patch_ingress(*, name, body, **_kwargs):
        resource = ingresses[name]
        for operation in body:
            if operation["path"] == "/metadata/labels/srw.io~1runtime-generation":
                resource["metadata"]["labels"]["srw.io/runtime-generation"] = operation[
                    "value"
                ]
            elif operation["path"] == "/metadata/ownerReferences":
                resource["metadata"]["ownerReferences"] = operation["value"]
        return deepcopy(resource)

    api._ingresses = ingresses
    api.read_namespaced_ingress.side_effect = read_ingress
    api.create_namespaced_ingress.side_effect = create_ingress
    api.patch_namespaced_ingress.side_effect = patch_ingress
    return api


@pytest.fixture
def svc(db, k8s_core_api, k8s_networking_api):
    return SessionRouterService(
        namespace="srw",
        ingress_host="api.example.com",
        ingress_class="traefik",
        annotations={"foo": "bar"},
        db=db,
        core_api=k8s_core_api,
        networking_api=k8s_networking_api,
    )


@pytest.fixture
def single_origin_svc(db, k8s_core_api, k8s_networking_api):
    return SessionRouterService(
        namespace="srw",
        ingress_host="",
        ingress_class="srw-evaluation-abc123",
        single_origin=True,
        db=db,
        core_api=k8s_core_api,
        networking_api=k8s_networking_api,
    )


@pytest.mark.asyncio
async def test_single_origin_route_is_hostless_and_reconciles(
    single_origin_svc, k8s_networking_api
):
    for _ in range(2):
        await single_origin_svc.ensure_route(
            THREAD_ID, POD_NAME, POD_UID, RUNTIME_GENERATION
        )
    route = k8s_networking_api._ingresses[f"session-{THREAD_ID}"]
    assert route["metadata"]["annotations"] == {
        "kubernetes.io/ingress.class": "srw-evaluation-abc123",
        "traefik.ingress.kubernetes.io/router.entrypoints": "websecure",
        "traefik.ingress.kubernetes.io/router.tls": "true",
    }
    assert "ingressClassName" not in route["spec"]
    assert "tls" not in route["spec"]
    rule = route["spec"]["rules"][0]
    assert "host" not in rule
    assert rule["http"]["paths"] == [{
        "path": f"/p/{THREAD_ID}",
        "pathType": "Prefix",
        "backend": {"service": {"name": f"session-{THREAD_ID}", "port": {"number": 8001}}},
    }]
    assert k8s_networking_api.create_namespaced_ingress.call_count == 1
    assert k8s_networking_api.patch_namespaced_ingress.call_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["host", "class", "owner", "target", "tls", "annotation"])
async def test_single_origin_reconciliation_refuses_changed_route_authority(
    single_origin_svc, k8s_core_api, k8s_networking_api, mutation
):
    await single_origin_svc.ensure_route(THREAD_ID, POD_NAME, POD_UID, RUNTIME_GENERATION)
    route = k8s_networking_api._ingresses[f"session-{THREAD_ID}"]
    if mutation == "host":
        route["spec"]["rules"][0]["host"] = "old.example.com"
    elif mutation == "class":
        route["spec"]["ingressClassName"] = "traefik"
    elif mutation == "owner":
        route["metadata"]["ownerReferences"][0]["uid"] = "foreign-pod"
    elif mutation == "target":
        route["spec"]["rules"][0]["http"]["paths"][0]["backend"]["service"]["name"] = "foreign-service"
    elif mutation == "tls":
        route["spec"]["tls"] = [{"secretName": "foreign-certificate"}]
    else:
        route["metadata"]["annotations"]["kubernetes.io/ingress.class"] = "traefik"
    k8s_core_api.patch_namespaced_pod.reset_mock()
    with pytest.raises(SessionRouteAuthorityError, match="Ingress is not trusted"):
        await single_origin_svc.ensure_route(THREAD_ID, POD_NAME, POD_UID, RUNTIME_GENERATION)
    k8s_core_api.patch_namespaced_pod.assert_not_called()


@pytest.mark.asyncio
async def test_single_origin_refuses_legacy_namespace_binding(single_origin_svc, db):
    db.get_pinned_session_binding.return_value = _binding(pod_namespace="agents-old")
    with pytest.raises(SessionRouteAuthorityError):
        await single_origin_svc.ensure_route(THREAD_ID, POD_NAME, POD_UID, RUNTIME_GENERATION)


@pytest.mark.asyncio
async def test_route_creation_is_pod_uid_resource_version_and_generation_fenced(
    svc, db, k8s_core_api, k8s_networking_api
):
    assert (
        await svc.ensure_route(THREAD_ID, POD_NAME, POD_UID, RUNTIME_GENERATION)
        == f"/p/{THREAD_ID}"
    )

    patch = k8s_core_api.patch_namespaced_pod.call_args.kwargs
    assert patch["body"][:2] == [
        {"op": "test", "path": "/metadata/uid", "value": POD_UID},
        {
            "op": "test",
            "path": "/metadata/resourceVersion",
            "value": "7",
        },
    ]
    assert {
        "op": "add",
        "path": "/metadata/labels/srw.io~1runtime-generation",
        "value": RUNTIME_GENERATION,
    } in patch["body"]
    service = k8s_core_api._services[f"session-{THREAD_ID}"]
    assert service["spec"]["selector"] == {
        "srw.io/thread-id": THREAD_ID,
        "srw.io/runtime-generation": RUNTIME_GENERATION,
    }
    ingress = k8s_networking_api._ingresses[f"session-{THREAD_ID}"]
    assert ingress["metadata"]["ownerReferences"][0]["uid"] == POD_UID
    assert db.get_pinned_session_binding.await_count >= 3
    assert "_request_timeout" in k8s_core_api.read_namespaced_pod.call_args.kwargs


@pytest.mark.asyncio
async def test_legacy_namespace_route_targets_exact_binding_without_shadow(
    svc, db, k8s_core_api, k8s_networking_api, monkeypatch
):
    monkeypatch.setenv("PINNED_LEGACY_AGENT_NAMESPACES", "agents-old")
    db.get_pinned_session_binding.return_value = _binding(pod_namespace="agents-old")
    k8s_core_api._pod["metadata"]["namespace"] = "agents-old"

    assert (
        await svc.ensure_route(THREAD_ID, POD_NAME, POD_UID, RUNTIME_GENERATION)
        == f"/p/{THREAD_ID}"
    )

    assert k8s_core_api.patch_namespaced_pod.call_args.kwargs["namespace"] == (
        "agents-old"
    )
    assert k8s_core_api.create_namespaced_service.call_args.kwargs["namespace"] == (
        "agents-old"
    )
    assert (
        k8s_networking_api.create_namespaced_ingress.call_args.kwargs["namespace"]
        == "agents-old"
    )
    assert (
        k8s_core_api._services[f"session-{THREAD_ID}"]["metadata"]["namespace"]
        == "agents-old"
    )
    assert (
        k8s_networking_api._ingresses[f"session-{THREAD_ID}"]["metadata"]["namespace"]
        == "agents-old"
    )


@pytest.mark.asyncio
async def test_legacy_namespace_refuses_current_namespace_route_shadow(
    svc, db, k8s_core_api, k8s_networking_api, monkeypatch
):
    monkeypatch.setenv("PINNED_LEGACY_AGENT_NAMESPACES", "agents-old")
    db.get_pinned_session_binding.return_value = _binding(pod_namespace="agents-old")
    k8s_core_api._pod["metadata"]["namespace"] = "agents-old"
    name = f"session-{THREAD_ID}"
    shadow = svc._service_body(
        THREAD_ID,
        name,
        POD_NAME,
        POD_UID,
        RUNTIME_GENERATION,
        namespace="srw",
    )
    shadow["metadata"]["uid"] = "shadow-service-uid"
    k8s_core_api._services[name] = shadow

    with pytest.raises(
        SessionRouteAuthorityError,
        match="shadow exists outside authoritative namespace",
    ):
        await svc.ensure_route(THREAD_ID, POD_NAME, POD_UID, RUNTIME_GENERATION)

    k8s_core_api.patch_namespaced_pod.assert_not_called()
    k8s_core_api.create_namespaced_service.assert_not_called()
    k8s_networking_api.create_namespaced_ingress.assert_not_called()


@pytest.mark.asyncio
async def test_legacy_namespace_refuses_other_allowlisted_route_shadow(
    svc, db, k8s_core_api, k8s_networking_api, monkeypatch
):
    monkeypatch.setenv(
        "PINNED_LEGACY_AGENT_NAMESPACES",
        "agents-old,agents-older",
    )
    db.get_pinned_session_binding.return_value = _binding(pod_namespace="agents-old")
    k8s_core_api._pod["metadata"]["namespace"] = "agents-old"
    name = f"session-{THREAD_ID}"
    shadow = svc._service_body(
        THREAD_ID,
        name,
        POD_NAME,
        POD_UID,
        RUNTIME_GENERATION,
        namespace="agents-older",
    )
    shadow["metadata"]["uid"] = "shadow-service-uid"

    def read_service(*, name, namespace, **_kwargs):
        if namespace == "agents-older":
            return deepcopy(shadow)
        raise _not_found()

    k8s_core_api.read_namespaced_service.side_effect = read_service

    with pytest.raises(
        SessionRouteAuthorityError,
        match="shadow exists outside authoritative namespace",
    ):
        await svc.ensure_route(THREAD_ID, POD_NAME, POD_UID, RUNTIME_GENERATION)

    k8s_core_api.patch_namespaced_pod.assert_not_called()
    k8s_core_api.create_namespaced_service.assert_not_called()
    k8s_networking_api.create_namespaced_ingress.assert_not_called()


@pytest.mark.asyncio
async def test_teardown_rejects_missing_namespace_authority(
    svc, k8s_core_api, k8s_networking_api
):
    with pytest.raises(SessionRouteAuthorityError, match="namespace authority"):
        await svc.teardown_route(
            THREAD_ID,
            expected_namespace="",
            expected_runtime_generation=RUNTIME_GENERATION,
            expected_owner_uid=POD_UID,
        )

    k8s_core_api.delete_namespaced_service.assert_not_called()
    k8s_networking_api.delete_namespaced_ingress.assert_not_called()


@pytest.mark.asyncio
async def test_recycled_pod_uid_fails_before_any_mutation(
    svc, k8s_core_api, k8s_networking_api
):
    k8s_core_api._pod["metadata"]["uid"] = "pod-uid-successor"

    with pytest.raises(SessionRouteAuthorityError):
        await svc.ensure_route(THREAD_ID, POD_NAME, POD_UID, RUNTIME_GENERATION)

    k8s_core_api.patch_namespaced_pod.assert_not_called()
    k8s_core_api.create_namespaced_service.assert_not_called()
    k8s_networking_api.create_namespaced_ingress.assert_not_called()


@pytest.mark.asyncio
async def test_binding_change_after_pod_read_fails_before_patch(
    svc, db, k8s_core_api, k8s_networking_api
):
    db.get_pinned_session_binding.side_effect = [_binding(), None]

    with pytest.raises(SessionRouteAuthorityError):
        await svc.ensure_route(THREAD_ID, POD_NAME, POD_UID, RUNTIME_GENERATION)

    k8s_core_api.patch_namespaced_pod.assert_not_called()
    k8s_core_api.create_namespaced_service.assert_not_called()
    k8s_networking_api.create_namespaced_ingress.assert_not_called()


@pytest.mark.asyncio
async def test_foreign_existing_service_fails_before_pod_mutation(
    svc, k8s_core_api, k8s_networking_api
):
    name = f"session-{THREAD_ID}"
    foreign = svc._service_body(THREAD_ID, name, POD_NAME, POD_UID, RUNTIME_GENERATION)
    foreign["metadata"]["uid"] = "service-uid"
    foreign["spec"]["selector"]["srw.io/thread-id"] = "foreign-thread"
    k8s_core_api._services[name] = foreign

    with pytest.raises(SessionRouteAuthorityError):
        await svc.ensure_route(THREAD_ID, POD_NAME, POD_UID, RUNTIME_GENERATION)

    k8s_core_api.patch_namespaced_pod.assert_not_called()
    k8s_core_api.create_namespaced_service.assert_not_called()
    k8s_networking_api.create_namespaced_ingress.assert_not_called()


@pytest.mark.asyncio
async def test_legacy_exact_route_is_generation_adopted(
    svc, k8s_core_api, k8s_networking_api
):
    name = f"session-{THREAD_ID}"
    service = svc._service_body(THREAD_ID, name, POD_NAME, POD_UID, None)
    service["metadata"]["uid"] = "service-uid"
    service["spec"]["selector"].pop("srw.io/runtime-generation")
    ingress = svc._ingress_body(THREAD_ID, name, POD_NAME, POD_UID, None)
    ingress["metadata"]["uid"] = "ingress-uid"
    k8s_core_api._services[name] = service
    k8s_networking_api._ingresses[name] = ingress

    assert (
        await svc.ensure_route(THREAD_ID, POD_NAME, POD_UID, RUNTIME_GENERATION)
        == f"/p/{THREAD_ID}"
    )

    assert (
        k8s_core_api._services[name]["metadata"]["labels"]["srw.io/runtime-generation"]
        == RUNTIME_GENERATION
    )
    assert (
        k8s_networking_api._ingresses[name]["metadata"]["labels"][
            "srw.io/runtime-generation"
        ]
        == RUNTIME_GENERATION
    )


@pytest.mark.asyncio
async def test_exact_teardown_preserves_successor_generation(
    svc, k8s_core_api, k8s_networking_api
):
    name = f"session-{THREAD_ID}"
    successor_generation = "99999999-2222-4333-8444-555555555555"
    service = svc._service_body(
        THREAD_ID, name, POD_NAME, "successor-uid", successor_generation
    )
    service["metadata"]["uid"] = "service-successor"
    ingress = svc._ingress_body(
        THREAD_ID, name, POD_NAME, "successor-uid", successor_generation
    )
    ingress["metadata"]["uid"] = "ingress-successor"
    k8s_core_api._services[name] = service
    k8s_networking_api._ingresses[name] = ingress

    assert (
        await svc.teardown_route(
            THREAD_ID,
            expected_namespace="srw",
            expected_runtime_generation=RUNTIME_GENERATION,
            expected_owner_uid=POD_UID,
        )
        is True
    )
    k8s_core_api.delete_namespaced_service.assert_not_called()
    k8s_networking_api.delete_namespaced_ingress.assert_not_called()


DEDICATED_POD_NAME = f"persistent-{THREAD_ID[:12]}"


def _make_dedicated(db, k8s_core_api, *, labels=None):
    """Reshape the shared harness into one historical dedicated session Pod.

    A dedicated ``persistent-agent`` Pod predates the canonical route labels.
    It carries the full thread UUID in the compatibility label and no
    ``srw.io/*`` label at all until ``ensure_route`` adopts it.
    """

    db.get_pinned_session_binding.return_value = _binding(
        agent_hostname=DEDICATED_POD_NAME
    )
    pod = k8s_core_api._pod
    pod["metadata"]["name"] = DEDICATED_POD_NAME
    pod["metadata"]["labels"] = {
        "srw/component": "persistent-agent",
        "srw/thread-id": THREAD_ID,
    }
    if labels is not None:
        pod["metadata"]["labels"].update(labels)
    return pod


@pytest.mark.asyncio
async def test_dedicated_pod_route_publication_adopts_the_historical_full_label(
    svc, db, k8s_core_api, k8s_networking_api
):
    pod = _make_dedicated(db, k8s_core_api)

    assert (
        await svc.ensure_route(
            THREAD_ID, DEDICATED_POD_NAME, POD_UID, RUNTIME_GENERATION
        )
        == f"/p/{THREAD_ID}"
    )

    # The post-patch validation runs against the published shape: canonical
    # full label plus the short compatibility label.
    assert pod["metadata"]["labels"]["srw.io/thread-id"] == THREAD_ID
    assert pod["metadata"]["labels"]["srw/thread-id"] == THREAD_ID[:12]
    assert pod["metadata"]["labels"]["srw.io/runtime-generation"] == RUNTIME_GENERATION
    assert pod["metadata"]["labels"]["srw/purpose"] == "session"
    assert k8s_core_api.read_namespaced_pod.call_count >= 2
    service = k8s_core_api._services[f"session-{THREAD_ID}"]
    assert service["spec"]["selector"] == {
        "srw.io/thread-id": THREAD_ID,
        "srw.io/runtime-generation": RUNTIME_GENERATION,
    }
    assert (
        k8s_networking_api._ingresses[f"session-{THREAD_ID}"]["metadata"][
            "ownerReferences"
        ][0]["uid"]
        == POD_UID
    )


@pytest.mark.asyncio
async def test_dedicated_pod_already_carrying_route_labels_revalidates(
    svc, db, k8s_core_api, k8s_networking_api
):
    _make_dedicated(
        db,
        k8s_core_api,
        labels={
            "srw.io/thread-id": THREAD_ID,
            "srw/thread-id": THREAD_ID[:12],
            "srw/purpose": "session",
            "srw.io/runtime-generation": RUNTIME_GENERATION,
        },
    )

    assert (
        await svc.ensure_route(
            THREAD_ID, DEDICATED_POD_NAME, POD_UID, RUNTIME_GENERATION
        )
        == f"/p/{THREAD_ID}"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "labels",
    [
        pytest.param({"srw/thread-id": OTHER_THREAD_ID}, id="foreign-full-label"),
        pytest.param({"srw/thread-id": THREAD_ID[:12]}, id="short-without-canonical"),
        pytest.param(
            {"srw/thread-id": THREAD_ID[:12], "srw.io/thread-id": OTHER_THREAD_ID},
            id="short-with-foreign-canonical",
        ),
        pytest.param({"srw/thread-id": None}, id="no-identity-label"),
        pytest.param({"srw/component": "agent"}, id="foreign-component"),
    ],
)
async def test_dedicated_pod_without_exact_identity_is_refused(
    svc, db, k8s_core_api, k8s_networking_api, labels
):
    pod = _make_dedicated(db, k8s_core_api)
    for key, value in labels.items():
        if value is None:
            pod["metadata"]["labels"].pop(key, None)
        else:
            pod["metadata"]["labels"][key] = value

    with pytest.raises(SessionRouteAuthorityError):
        await svc.ensure_route(
            THREAD_ID, DEDICATED_POD_NAME, POD_UID, RUNTIME_GENERATION
        )

    assert k8s_core_api.create_namespaced_service.call_count == 0
    assert k8s_networking_api.create_namespaced_ingress.call_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "published",
    [
        pytest.param({"srw.io/thread-id": OTHER_THREAD_ID}, id="wrong-canonical"),
        pytest.param({"srw/thread-id": THREAD_ID}, id="compatibility-stayed-full"),
        pytest.param(
            {"srw.io/runtime-generation": OTHER_THREAD_ID}, id="wrong-generation"
        ),
        pytest.param({"srw/purpose": "job"}, id="wrong-purpose"),
    ],
)
async def test_dedicated_route_refuses_a_post_patch_label_that_is_not_exact(
    svc, db, k8s_core_api, k8s_networking_api, published
):
    pod = _make_dedicated(db, k8s_core_api)
    original_patch = k8s_core_api.patch_namespaced_pod.side_effect

    def patch_then_corrupt(**kwargs):
        result = original_patch(**kwargs)
        pod["metadata"]["labels"].update(published)
        return result

    k8s_core_api.patch_namespaced_pod.side_effect = patch_then_corrupt

    with pytest.raises(SessionRouteAuthorityError):
        await svc.ensure_route(
            THREAD_ID, DEDICATED_POD_NAME, POD_UID, RUNTIME_GENERATION
        )

    assert k8s_core_api.create_namespaced_service.call_count == 0
    assert k8s_networking_api.create_namespaced_ingress.call_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        pytest.param({"uid": "pod-uid-other"}, id="foreign-uid"),
        pytest.param({"ip": "10.0.0.9"}, id="foreign-ip"),
        pytest.param({"namespace": "agents-old"}, id="foreign-namespace"),
    ],
)
async def test_dedicated_route_keeps_the_reciprocal_identity_fences(
    svc, db, k8s_core_api, k8s_networking_api, changes
):
    pod = _make_dedicated(db, k8s_core_api)
    if "uid" in changes:
        pod["metadata"]["uid"] = changes["uid"]
    if "ip" in changes:
        pod["status"]["podIP"] = changes["ip"]
    if "namespace" in changes:
        pod["metadata"]["namespace"] = changes["namespace"]

    with pytest.raises(SessionRouteAuthorityError):
        await svc.ensure_route(
            THREAD_ID, DEDICATED_POD_NAME, POD_UID, RUNTIME_GENERATION
        )

    assert k8s_core_api.create_namespaced_service.call_count == 0


@pytest.mark.asyncio
async def test_dedicated_route_still_refuses_a_shadow_in_a_legacy_namespace(
    svc, db, k8s_core_api, k8s_networking_api, monkeypatch
):
    monkeypatch.setenv("PINNED_LEGACY_AGENT_NAMESPACES", "agents-old")
    _make_dedicated(db, k8s_core_api)
    name = f"session-{THREAD_ID}"
    k8s_core_api._services[name] = {
        "metadata": {"name": name, "namespace": "agents-old"}
    }

    with pytest.raises(SessionRouteAuthorityError, match="shadow"):
        await svc.ensure_route(
            THREAD_ID, DEDICATED_POD_NAME, POD_UID, RUNTIME_GENERATION
        )

    assert k8s_core_api.patch_namespaced_pod.call_count == 0


@pytest.mark.asyncio
async def test_managed_pool_pod_identity_is_unchanged_by_the_dedicated_repair(
    svc, db, k8s_core_api
):
    # A warm-pool Pod never carries a thread-id label before publication and
    # must still not be admitted through the dedicated identity branch.
    k8s_core_api._pod["metadata"]["labels"]["srw/managed-by"] = "someone-else"

    with pytest.raises(SessionRouteAuthorityError):
        await svc.ensure_route(THREAD_ID, POD_NAME, POD_UID, RUNTIME_GENERATION)

    assert k8s_core_api.create_namespaced_service.call_count == 0
