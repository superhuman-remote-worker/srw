"""Pin the pod-authority classification that makes an in-place container
restart safe: the same pod UID with a running container is still the claimant,
and a different UID is a replacement."""

from types import SimpleNamespace

from orchestrator.services.agent_provisioner import classify_agent_pod_authority


def _pod(uid, phase, *, running=None, terminated=None):
    status = SimpleNamespace(
        state=SimpleNamespace(running=running, terminated=terminated)
    )
    return SimpleNamespace(
        metadata=SimpleNamespace(uid=uid, deletion_timestamp=None),
        status=SimpleNamespace(phase=phase, container_statuses=[status]),
    )


def test_restarted_same_uid_running_pod_is_exact_live():
    pod = _pod("u1", "Running", running=object())
    assert classify_agent_pod_authority(pod, "u1") == "exact_live"


def test_same_uid_all_containers_terminated_is_exact_terminal():
    pod = _pod("u1", "Failed", terminated=object())
    assert classify_agent_pod_authority(pod, "u1") == "exact_terminal"


def test_other_uid_is_replacement():
    pod = _pod("u2", "Running", running=object())
    assert classify_agent_pod_authority(pod, "u1") == "replacement"
