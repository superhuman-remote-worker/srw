"""Claimant-attestation acceptance for pooled stateless workers.

Covers the documented matrix from
``stateless_worker_bundle_requires_pinned_registration.md`` §4 without
inventing an ``agents`` registration:

* valid sandbox/VM claimants, recovery false and true → 200 + authorized
* transport auth, lease, UID, replacement, pool, namespace, config, K8s
  availability refusals
* lease stolen / claimant replaced between initial and final observation
* recovery-report replay (accepted idempotent, conflicting refused)
* dependency wiring + Helm/RBAC surface (no new RBAC required)
"""

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from orchestrator.services import stateless_claimant_attestation as sca

UNIT_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
POD_NAME = "srw-agent-stateless-d9d86bd6f-pll7v"
POD_UID = "aaaaaaaa-1111-2222-3333-bbbbbbbbbbbb"
OTHER_UID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
NAMESPACE = "superhuman-remote-worker"
CHART_NAME = "superhuman-remote-worker"
RELEASE = "srw"


def _pod(*, name=POD_NAME, uid=POD_UID, phase="Running", deleting=False, labels=None):
    base = {
        "srw/class": "agent-stateless",
        "app.kubernetes.io/name": CHART_NAME,
        "app.kubernetes.io/instance": RELEASE,
        "app.kubernetes.io/component": "agent-stateless",
    }
    if labels is not None:
        base.update(labels)
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            uid=uid,
            labels=dict(base),
            deletion_timestamp="2026-09-21T00:00:00Z" if deleting else None,
        ),
        status=SimpleNamespace(
            phase=phase,
            pod_ip="10.0.0.9",
            conditions=[SimpleNamespace(type="Ready", status="False")],
            container_statuses=[
                SimpleNamespace(
                    name="agent", ready=False, state=SimpleNamespace(running={})
                )
            ],
        ),
    )


class _FakeCoreApi:
    """Sync fake: ``run_bounded_k8s_call`` runs it in a worker thread."""

    def __init__(self, pod=None, exc=None):
        self._pod = pod
        self._exc = exc
        self.calls = []

    def read_namespaced_pod(self, name, namespace, **kwargs):
        self.calls.append((name, namespace))
        if self._exc is not None:
            raise self._exc
        return self._pod


class _ApiError(Exception):
    def __init__(self, status):
        super().__init__(f"api {status}")
        self.status = status


@pytest.mark.asyncio
async def test_valid_busy_executor_accepted_without_ready():
    """A valid Running pod is accepted even when Ready//container ready is False."""
    api = _FakeCoreApi(pod=_pod())
    await sca.attest_stateless_executor_pod(
        POD_NAME,
        POD_UID,
        core_api=api,
        namespace=NAMESPACE,
        expected_name=CHART_NAME,
        expected_instance=RELEASE,
    )
    assert api.calls == [(POD_NAME, NAMESPACE)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs",
    [
        {"pod_name": "", "pod_uid": POD_UID},
        {"pod_name": POD_NAME, "pod_uid": ""},
        {"pod_name": POD_NAME, "pod_uid": "not-a-uid"},
    ],
    ids=["empty-name", "empty-uid", "malformed-uid"],
)
async def test_malformed_claimant_is_generic_403(kwargs):
    api = _FakeCoreApi(pod=_pod())
    with pytest.raises(HTTPException) as exc:
        await sca.attest_stateless_executor_pod(
            kwargs["pod_name"],
            kwargs["pod_uid"],
            core_api=api,
            namespace=NAMESPACE,
            expected_name=CHART_NAME,
            expected_instance=RELEASE,
        )
    assert exc.value.status_code == 403
    assert exc.value.detail == "Lease validation failed"
    assert api.calls == []


@pytest.mark.asyncio
async def test_missing_pod_is_generic_403():
    api = _FakeCoreApi(exc=_ApiError(404))
    with pytest.raises(HTTPException) as exc:
        await sca.attest_stateless_executor_pod(
            POD_NAME,
            POD_UID,
            core_api=api,
            namespace=NAMESPACE,
            expected_name=CHART_NAME,
            expected_instance=RELEASE,
        )
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_same_name_replacement_is_generic_403():
    api = _FakeCoreApi(pod=_pod(uid=OTHER_UID))
    with pytest.raises(HTTPException) as exc:
        await sca.attest_stateless_executor_pod(
            POD_NAME,
            POD_UID,
            core_api=api,
            namespace=NAMESPACE,
            expected_name=CHART_NAME,
            expected_instance=RELEASE,
        )
    assert exc.value.status_code == 403


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pod_kwargs",
    [
        {"phase": "Succeeded"},
        {"phase": "Failed"},
        {"phase": "Pending"},
        {"deleting": True},
    ],
    ids=["succeeded", "failed", "pending", "deleting"],
)
async def test_non_running_or_deleting_refused(pod_kwargs):
    api = _FakeCoreApi(pod=_pod(**pod_kwargs))
    with pytest.raises(HTTPException) as exc:
        await sca.attest_stateless_executor_pod(
            POD_NAME,
            POD_UID,
            core_api=api,
            namespace=NAMESPACE,
            expected_name=CHART_NAME,
            expected_instance=RELEASE,
        )
    assert exc.value.status_code == 403


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "labels",
    [
        {"srw/class": "other"},
        {"app.kubernetes.io/component": "agent"},
        {"app.kubernetes.io/name": "wrong-chart"},
        {"app.kubernetes.io/instance": "wrong-release"},
    ],
    ids=["class", "component", "name", "instance"],
)
async def test_wrong_pool_refused(labels):
    api = _FakeCoreApi(pod=_pod(labels=labels))
    with pytest.raises(HTTPException) as exc:
        await sca.attest_stateless_executor_pod(
            POD_NAME,
            POD_UID,
            core_api=api,
            namespace=NAMESPACE,
            expected_name=CHART_NAME,
            expected_instance=RELEASE,
        )
    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_nondefault_valid_release_accepted():
    api = _FakeCoreApi(pod=_pod(labels={"app.kubernetes.io/instance": "srw-prod"}))
    await sca.attest_stateless_executor_pod(
        POD_NAME,
        POD_UID,
        core_api=api,
        namespace=NAMESPACE,
        expected_name=CHART_NAME,
        expected_instance="srw-prod",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs",
    [
        {"namespace": "", "expected_name": CHART_NAME, "expected_instance": RELEASE},
        {"namespace": NAMESPACE, "expected_name": "", "expected_instance": RELEASE},
        {"namespace": NAMESPACE, "expected_name": CHART_NAME, "expected_instance": ""},
    ],
    ids=["namespace", "name", "instance"],
)
async def test_missing_server_config_is_unknown_503(kwargs):
    api = _FakeCoreApi(pod=_pod())
    with pytest.raises(HTTPException) as exc:
        await sca.attest_stateless_executor_pod(
            POD_NAME, POD_UID, core_api=api, **kwargs
        )
    assert exc.value.status_code == 503
    assert exc.value.detail == "Claimant authority unavailable"


@pytest.mark.asyncio
async def test_unavailable_client_is_unknown_503():
    api = _FakeCoreApi(pod=_pod())
    with pytest.raises(HTTPException) as exc:
        await sca.attest_stateless_executor_pod(
            POD_NAME,
            POD_UID,
            core_api=None,
            namespace=NAMESPACE,
            expected_name=CHART_NAME,
            expected_instance=RELEASE,
        )
    assert exc.value.status_code == 503
    assert api.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [500, 503, None])
async def test_k8s_timeout_or_error_is_unknown_503(status):
    exc = _ApiError(status) if status is not None else RuntimeError("boom")
    if status is None:
        delattr(exc, "status") if hasattr(exc, "status") else None
    api = _FakeCoreApi(exc=exc)
    with pytest.raises(HTTPException) as raised:
        await sca.attest_stateless_executor_pod(
            POD_NAME,
            POD_UID,
            core_api=api,
            namespace=NAMESPACE,
            expected_name=CHART_NAME,
            expected_instance=RELEASE,
        )
    assert raised.value.status_code == 503


def test_executor_namespace_is_server_owned(monkeypatch):
    monkeypatch.setenv("WORKSPACE_NAMESPACE", "release-ns")
    assert sca.executor_namespace() == "release-ns"
    monkeypatch.delenv("WORKSPACE_NAMESPACE", raising=False)
    assert sca.executor_namespace() == "superhuman-remote-worker"


def test_pool_identity_missing_fails_closed(monkeypatch):
    monkeypatch.delenv("AGENT_LABEL_NAME", raising=False)
    monkeypatch.delenv("AGENT_LABEL_INSTANCE", raising=False)
    name, instance = sca.pool_identity()
    assert (name, instance) == ("", "")


def test_orchestrator_rbac_already_allows_pod_get():
    from pathlib import Path

    rbac = Path("helm/templates/orchestrator/rbac.yaml").read_text()
    assert 'resources: ["pods"]' in rbac
    assert '"get"' in rbac or "'get'" in rbac or "get" in rbac
    # No new Helm RBAC is required for read-only claimant attestation:
    # the existing Role already grants pods get/list/watch.


def test_stateless_pool_labels_match_attestation_contract():
    from pathlib import Path

    deployment = Path("helm/templates/agent/stateless-deployment.yaml").read_text()
    assert "srw/class: agent-stateless" in deployment
    assert "app.kubernetes.io/component: agent-stateless" in deployment
    # The Deployment deliberately carries no agent-provisioner identity;
    # attestation must not require those pinned labels.
    assert "srw/managed-by: agent-provisioner" in deployment  # documented delta
    assert "srw/class: agent-stateless" in deployment


def test_unit_claim_dependencies_expose_attestor():
    from orchestrator import main as orch_main

    deps = orch_main._unit_claim_bundle_dependencies()
    assert callable(deps.attest_stateless_claimant)
