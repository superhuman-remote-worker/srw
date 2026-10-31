"""Generic image hosting preserves configuration and fences exact Kubernetes IDs."""

import base64
from copy import deepcopy
import json
import subprocess
import sys
from types import SimpleNamespace
from uuid import uuid4

from kubernetes.client import ApiClient
from kubernetes.client.exceptions import ApiException
import pytest

from orchestrator.services.generic_harness_runtime import (
    GENERIC_FINALIZER,
    GenericAttemptIdentity,
    GenericBindings,
    GenericBoundFile,
    GenericHarnessRuntime,
    GenericRuntimeError,
    GenericRuntimePolicy,
    build_generic_launch,
)


IDENTITY = GenericAttemptIdentity("11111111-1111-4111-8111-111111111111", 1)


def job(runtime=None, **overrides):
    return {
        "execution": {
            "expert": {
                "inline": {"runtime": {"image": "busybox:1.36.1", **(runtime or {})}}
            },
            "workspace": None,
            "connectors": {},
        },
        "completion": {"mode": "ProcessExit"},
        **overrides,
    }


def plan(runtime=None, **kwargs):
    return build_generic_launch(IDENTITY, job(runtime), namespace="srw", **kwargs)


def files_from_plan(launch):
    container = launch.pod["spec"]["containers"][0]
    return {
        mount["mountPath"]: base64.b64decode(
            launch.delivery_secret["data"][mount["subPath"]]
        )
        for mount in container.get("volumeMounts", [])
    }


class MemoryKubernetes:
    """Use generated API response models, including camelCase/ID field mapping."""

    def __init__(self):
        self.objects = {}
        self.calls = []
        self.client = ApiClient()
        self.core = SimpleNamespace()
        self.networking = SimpleNamespace()
        self.failure = None
        for suffix, kind, model, target in (
            ("pod", "Pod", "V1Pod", self.core),
            ("secret", "Secret", "V1Secret", self.core),
            ("network_policy", "NetworkPolicy", "V1NetworkPolicy", self.networking),
        ):
            for action in ("create", "read", "delete", "patch"):
                setattr(
                    target,
                    f"{action}_namespaced_{suffix}",
                    self.operation(action, kind, model),
                )

    def operation(self, action, kind, model):
        def invoke(*, namespace, name=None, body=None, _request_timeout=None):
            assert _request_timeout is not None
            self.calls.append((action, kind, name, deepcopy(body)))
            if self.failure == (action, kind):
                raise ApiException(
                    status=503, reason="response-must-not-expose-fixture-secret"
                )
            if name is None:
                name = body["metadata"]["name"]
            key = (kind, namespace, name)
            if action == "create":
                if key in self.objects:
                    raise ApiException(status=409)
                obj = deepcopy(body)
                obj["metadata"].update(uid=str(uuid4()), resourceVersion="1")
                if kind == "Pod":
                    obj["status"] = {"phase": "Pending"}
                self.objects[key] = obj
            elif key not in self.objects:
                raise ApiException(status=404)
            elif action == "patch":
                metadata = self.objects[key]["metadata"]
                for operation in body:
                    field = operation["path"].removeprefix("/metadata/")
                    if (
                        operation["op"] == "test"
                        and metadata[field] != operation["value"]
                    ):
                        raise ApiException(status=409)
                    if operation["op"] == "replace":
                        metadata[field] = operation["value"]
            elif action == "delete":
                obj = self.objects[key]
                if body["preconditions"]["uid"] != obj["metadata"]["uid"]:
                    raise ApiException(status=409)
                if obj["metadata"].get("finalizers"):
                    obj["metadata"]["deletionTimestamp"] = "2026-09-09T00:00:00Z"
                else:
                    del self.objects[key]
                    return None
            return self.client.deserialize(
                SimpleNamespace(data=json.dumps(self.objects[key])), model
            )

        return invoke

    @property
    def pod(self):
        return self.objects[("Pod", "srw", IDENTITY.pod_name)]

    def running(self, *, terminated=False, exit_code=0, ready=False):
        state = (
            {"terminated": {"exitCode": exit_code, "reason": "Completed"}}
            if terminated
            else {"running": {}}
        )
        self.pod["status"] = {
            "phase": ("Succeeded" if exit_code == 0 else "Failed")
            if terminated
            else "Running",
            "containerStatuses": [
                {
                    "name": "harness",
                    "image": "busybox:1.36.1",
                    "imageID": "containerd://sha256:fixture-digest",
                    "ready": ready,
                    "restartCount": 0,
                    "state": state,
                }
            ],
        }

    def runtime(self):
        return GenericHarnessRuntime(self.core, self.networking, namespace="srw")


def test_ordinary_image_keeps_its_own_entrypoint_and_has_no_platform_credentials(
    monkeypatch,
):
    monkeypatch.setenv("MCP_INTERNAL_KEY", "ambient-credential-must-not-leak")
    monkeypatch.setenv("AGENT_CONFIGMAP", "shared-config-must-not-leak")
    launch = plan()
    pod_spec = launch.pod["spec"]
    container = pod_spec["containers"][0]
    assert container["image"] == "busybox:1.36.1"
    assert not {
        "command",
        "args",
        "env",
        "envFrom",
        "readinessProbe",
        "livenessProbe",
        "volumeMounts",
    }.intersection(container)
    assert not {"volumes", "initContainers", "serviceAccountName"}.intersection(
        pod_spec
    )
    assert pod_spec["automountServiceAccountToken"] is False
    assert pod_spec["enableServiceLinks"] is False
    assert pod_spec["restartPolicy"] == "Never"
    assert pod_spec["securityContext"] == {"seccompProfile": {"type": "RuntimeDefault"}}
    assert container["securityContext"]["allowPrivilegeEscalation"] is False
    assert container["securityContext"]["capabilities"] == {"drop": ["ALL"]}
    assert "ambient-credential" not in json.dumps(launch.pod)
    assert "app.kubernetes.io/component" not in launch.pod["metadata"]["labels"]
    assert launch.delivery_secret is None
    assert launch.network_policy["spec"]["egress"] == []
    assert launch.network_policy["spec"]["ingress"] == []


def test_config_task_and_binding_files_preserve_opaque_json_and_ssh_workspace_separation():
    config = {
        "tools": ["unknown-custom-tool"],
        "settings": {"keep": None, "false": False},
        "password": "literal-private-setting",
    }
    task = {"text": "Inspect the remote environment", "data": [None, 3, "0123"]}
    descriptor = {
        "workspace": {"transport": "ssh", "host": "workspace.example", "port": 22}
    }
    launch = build_generic_launch(
        IDENTITY,
        job({"config": config}, task=task),
        namespace="srw",
        bindings=GenericBindings(descriptor=descriptor),
    )
    delivered = files_from_plan(launch)
    assert json.loads(delivered["/run/srw/config.json"]) == config
    assert json.loads(delivered["/run/srw/task.json"]) == task
    assert json.loads(delivered["/run/srw/bindings.json"]) == descriptor
    assert launch.delivery_secret["immutable"] is True
    assert "literal-private-setting" not in repr(launch)
    assert "literal-private-setting" not in json.dumps(launch.pod)
    assert "persistentVolumeClaim" not in json.dumps(launch.pod)
    assert len(launch.pod["spec"]["containers"]) == 1
    assert all(
        mount["readOnly"]
        for mount in launch.pod["spec"]["containers"][0]["volumeMounts"]
    )


def test_explicit_secret_and_connector_env_use_only_an_isolated_secret():
    launch = plan(
        {
            "env": {
                "VISIBLE": "plain",
                "TOKEN": {"secretRef": {"name": "private", "key": "token"}},
            }
        },
        bindings=GenericBindings(
            secret_env={"TOKEN": "authorized-token"},
            environment={"SITE_PASSWORD": "authorized-password"},
        ),
    )
    env = {item["name"]: item for item in launch.pod["spec"]["containers"][0]["env"]}
    assert env["VISIBLE"]["value"] == "plain"
    for key, wanted in (
        ("TOKEN", "authorized-token"),
        ("SITE_PASSWORD", "authorized-password"),
    ):
        ref = env[key]["valueFrom"]["secretKeyRef"]
        assert ref["name"] == IDENTITY.pod_name
        assert (
            base64.b64decode(launch.delivery_secret["data"][ref["key"]]).decode()
            == wanted
        )
        assert wanted not in json.dumps(launch.pod)


@pytest.mark.parametrize(
    "bindings",
    [
        GenericBindings(),
        GenericBindings(secret_env={"TOKEN": "value", "UNREQUESTED": "value"}),
        GenericBindings(
            secret_env={"TOKEN": "value"}, environment={"TOKEN": "collision"}
        ),
        GenericBindings(
            secret_env={"TOKEN": "value"}, environment={"SRW_BINDINGS_FILE": "override"}
        ),
    ],
)
def test_missing_extra_or_colliding_credential_delivery_is_rejected(bindings):
    with pytest.raises(ValueError):
        plan(
            {"env": {"TOKEN": {"secretRef": {"name": "s", "key": "k"}}}},
            bindings=bindings,
        )


@pytest.mark.parametrize(
    "path",
    [
        "relative",
        "/tmp/../etc/passwd",
        "/tmp/a/",
        "/run/srw/task.json",
        "/run/srw/config.json",
        "/run/srw/bindings.json",
        "/run/srw/bindings",
        "/run/srw/bindings/",
        "/run/srw/bindings/../config.json",
        "/run/srw/bindings/../../srw-workspace/key",
        "/run/srw/bindings/./token",
        "/run/srw/bindings//token",
        "/run/srw/bindings-other/token",
        "/run",
        "/dev/null",
        "/proc/self/mem",
        "//tmp/key",
        "/tmp/a\x00b",
    ],
)
def test_file_delivery_rejects_ambiguous_or_reserved_paths(path):
    with pytest.raises(ValueError):
        plan(bindings=GenericBindings(files=(GenericBoundFile(path, "bound-content"),)))


@pytest.mark.parametrize("second", ["fixture", "fixture/child"])
def test_connector_file_mounts_cannot_overlap_each_other(second):
    with pytest.raises(ValueError, match="distinct"):
        plan(
            bindings=GenericBindings(
                files=(
                    GenericBoundFile("/run/srw/bindings/fixture", "first"),
                    GenericBoundFile("/run/srw/bindings/" + second, "second"),
                ),
            )
        )


def test_connector_files_coexist_with_platform_delivery_and_workspace_bindings():
    source = job({"config": {"keep": None}}, task={"text": "fixture task"})
    launch = build_generic_launch(
        IDENTITY,
        source,
        namespace="srw",
        bindings=GenericBindings(
            descriptor={"connectors": {"fixture": {"driver": "srw.files/v1"}}},
            files=(
                GenericBoundFile("/run/srw/bindings/fixture", "file content"),
                GenericBoundFile("/run/srw-workspace/key", "workspace key", 0o400),
            ),
        ),
    )
    files = files_from_plan(launch)
    assert files["/run/srw/bindings/fixture"] == b"file content"
    assert files["/run/srw-workspace/key"] == b"workspace key"
    assert json.loads(files["/run/srw/config.json"]) == {"keep": None}
    assert json.loads(files["/run/srw/task.json"]) == {"text": "fixture task"}
    assert json.loads(files["/run/srw/bindings.json"])["connectors"]["fixture"] == {
        "driver": "srw.files/v1"
    }


def test_explicit_file_permissions_and_network_grants_are_preserved():
    rule = {
        "to": [{"ipBlock": {"cidr": "192.0.2.7/32"}}],
        "ports": [{"port": 22, "protocol": "TCP"}],
    }
    launch = plan(
        bindings=GenericBindings(
            files=(GenericBoundFile("/credentials/token", b"value", 0o400),),
            egress=(rule,),
        )
    )
    assert files_from_plan(launch)["/credentials/token"] == b"value"
    assert launch.pod["spec"]["volumes"][0]["secret"]["items"][0]["mode"] == 0o400
    assert launch.network_policy["spec"]["egress"] == [rule]


def test_runtime_envelope_controls_pull_policy_and_resources_only():
    runtime = {
        "pullPolicy": "Never",
        "resources": {
            "requests": {"cpu": 0.125, "memory": "512Mi"},
            "limits": {"cpu": 4, "memory": "3Gi"},
        },
    }
    launch = plan(runtime)
    container = launch.pod["spec"]["containers"][0]
    assert container["imagePullPolicy"] == "Never"
    assert container["resources"]["requests"]["cpu"] == "0.125"
    assert container["resources"]["limits"]["memory"] == "3Gi"
    assert not {"command", "args", "readinessProbe", "livenessProbe"} & set(container)


@pytest.mark.parametrize(
    "override",
    [
        {"command": ["custom-entrypoint", "literal; argument"]},
        {"args": ["-c", "id"]},
        {"args": []},
        {"probes": {"readiness": {"httpGet": {"path": "/ready", "port": 8080}}}},
    ],
)
def test_launch_overrides_never_reach_the_container(override):
    """The image's own ENTRYPOINT and CMD run. The schema has no launch fields;
    a spec that carries one anyway skipped validation, so the builder fails
    closed instead of ignoring it."""
    with pytest.raises(ValueError, match="own entrypoint"):
        plan(override)


def test_remaining_deadline_does_not_reset_whole_job_timeout():
    source = job(timeoutSeconds=3600)
    with pytest.raises(ValueError, match="remaining whole-job"):
        build_generic_launch(IDENTITY, source, namespace="srw")
    launch = build_generic_launch(
        IDENTITY, source, namespace="srw", remaining_timeout_seconds=17
    )
    assert launch.pod["spec"]["activeDeadlineSeconds"] == 17
    with pytest.raises(ValueError, match="expired"):
        build_generic_launch(
            IDENTITY, source, namespace="srw", remaining_timeout_seconds=0
        )


def test_reference_adapter_fails_before_effects():
    with pytest.raises(ValueError, match="dedicated runtime"):
        plan({"adapter": "srw/v1"})


def test_delivery_and_resource_limits_are_checked_before_effects():
    with pytest.raises(ValueError, match="512 KiB"):
        plan({"config": {"huge": "x" * (512 * 1024)}})
    with pytest.raises(ValueError, match="exceed"):
        plan({"resources": {"requests": {"cpu": 3}, "limits": {"cpu": 1}}})
    launch = plan({"resources": {"limits": {"cpu": 0.1, "memory": "64Mi"}}})
    assert launch.pod["spec"]["containers"][0]["resources"]["requests"]["cpu"] == "0.1"
    assert (
        launch.pod["spec"]["containers"][0]["resources"]["requests"]["memory"] == "64Mi"
    )


def test_operator_security_policy_is_not_an_opaque_harness_setting():
    launch = plan(
        {"config": {"privileged": True, "hostNetwork": True}},
        policy=GenericRuntimePolicy(
            runtime_class_name="gvisor",
            run_as_user=1000,
            run_as_group=1000,
            read_only_root_filesystem=True,
        ),
    )
    assert launch.pod["spec"]["runtimeClassName"] == "gvisor"
    assert launch.pod["spec"]["hostNetwork"] is False
    assert launch.pod["spec"]["securityContext"]["runAsNonRoot"] is True
    assert launch.pod["spec"]["containers"][0]["securityContext"]["privileged"] is False


@pytest.mark.asyncio
async def test_launch_isolation_precedes_delivery_and_pod_and_reconciliation_is_idempotent():
    kube = MemoryKubernetes()
    runtime = kube.runtime()
    launch = plan({"config": {"keep": None}})
    first = await runtime.launch(launch)
    assert first.phase == "Pending"
    assert first.pod_uid
    assert [(action, kind) for action, kind, *_ in kube.calls] == [
        ("create", "NetworkPolicy"),
        ("create", "Secret"),
        ("create", "Pod"),
    ]
    # Real Kubernetes canonicalizes quantity spelling and omits empty lists and
    # non-pointer false booleans. Those changes do not create a second attempt.
    kube.pod["spec"]["containers"][0]["resources"]["requests"]["cpu"] = "250m"
    del kube.pod["spec"]["hostNetwork"]
    del kube.objects[("NetworkPolicy", "srw", IDENTITY.pod_name)]["spec"]["ingress"]
    second = await runtime.launch(launch)
    assert second.pod_uid == first.pod_uid
    assert len(kube.objects) == 3


@pytest.mark.asyncio
async def test_existing_pod_without_explicit_token_isolation_is_not_adopted():
    kube = MemoryKubernetes()
    runtime = kube.runtime()
    launch = plan()
    await runtime.launch(launch)
    del kube.pod["spec"]["automountServiceAccountToken"]
    with pytest.raises(GenericRuntimeError, match="does not match"):
        await runtime.launch(launch)


@pytest.mark.asyncio
async def test_network_failure_cannot_start_an_unisolated_pod_or_expose_response_secrets():
    kube = MemoryKubernetes()
    kube.failure = ("create", "NetworkPolicy")
    with pytest.raises(GenericRuntimeError) as raised:
        await kube.runtime().launch(plan({"config": {"password": "private"}}))
    assert "fixture-secret" not in str(raised.value)
    assert kube.objects == {}
    assert len(kube.calls) == 1


@pytest.mark.asyncio
async def test_same_name_different_uid_is_never_cancelled_or_cleaned():
    kube = MemoryKubernetes()
    runtime = kube.runtime()
    launched = await runtime.launch(plan())
    kube.pod["metadata"]["uid"] = "successor-uid"
    before = len(kube.calls)
    observed = await runtime.cancel(IDENTITY, expected_pod_uid=launched.pod_uid)
    assert observed.phase == "Replaced"
    assert await runtime.cleanup(IDENTITY, expected_pod_uid=launched.pod_uid) is False
    assert all(action == "read" for action, *_ in kube.calls[before:])


@pytest.mark.asyncio
async def test_cancel_requests_uid_fenced_deletion_but_does_not_claim_processes_stopped():
    kube = MemoryKubernetes()
    runtime = kube.runtime()
    launched = await runtime.launch(plan())
    kube.running()
    observed = await runtime.cancel(IDENTITY, expected_pod_uid=launched.pod_uid)
    assert observed.phase == "Running"
    assert observed.deletion_requested is True
    assert observed.containers_terminal is False
    assert observed.pod_absent is False
    assert await runtime.cleanup(IDENTITY, expected_pod_uid=launched.pod_uid) is False
    assert kube.pod["metadata"]["finalizers"] == [GENERIC_FINALIZER]
    deletes = [
        body
        for action, kind, _, body in kube.calls
        if action == "delete" and kind == "Pod"
    ]
    assert deletes == [{"preconditions": {"uid": launched.pod_uid}}]


@pytest.mark.asyncio
async def test_exit_readiness_and_resolved_image_digest_are_separate_observations():
    kube = MemoryKubernetes()
    runtime = kube.runtime()
    launched = await runtime.launch(plan())
    kube.running(ready=True)
    observed = await runtime.observe(IDENTITY, expected_pod_uid=launched.pod_uid)
    assert observed.readiness is None  # No application probe was requested.
    assert observed.image_id == "containerd://sha256:fixture-digest"
    assert observed.process_exit_code is None
    kube.running(terminated=True, exit_code=7)
    observed = await runtime.observe(IDENTITY, expected_pod_uid=launched.pod_uid)
    assert observed.phase == "Failed"
    assert observed.process_exit_code == 7
    assert observed.containers_terminal is True


@pytest.mark.asyncio
async def test_declared_readiness_probe_can_report_false_without_changing_process_state():
    """A Job cannot declare probes. The workspace runtime declares its own on
    the pods it builds, and observation reports that probe's state."""
    kube = MemoryKubernetes()
    runtime = kube.runtime()
    launch = plan()
    launch.pod["spec"]["containers"][0]["readinessProbe"] = {
        "exec": {"command": ["check"]}
    }
    launched = await runtime.launch(launch)
    kube.running(ready=False)
    observed = await runtime.observe(IDENTITY, expected_pod_uid=launched.pod_uid)
    assert observed.phase == "Running"
    assert observed.readiness is False
    assert observed.process_exit_code is None


@pytest.mark.asyncio
async def test_terminal_phase_without_container_evidence_does_not_authorize_cleanup():
    kube = MemoryKubernetes()
    runtime = kube.runtime()
    launched = await runtime.launch(plan())
    kube.pod["status"] = {"phase": "Failed", "reason": "NodeLost"}
    observed = await runtime.observe(IDENTITY, expected_pod_uid=launched.pod_uid)
    assert observed.containers_terminal is False
    assert await runtime.cleanup(IDENTITY, expected_pod_uid=launched.pod_uid) is False


@pytest.mark.asyncio
async def test_cleanup_removes_terminal_exact_pod_and_scoped_delivery_only_after_observation():
    kube = MemoryKubernetes()
    runtime = kube.runtime()
    launched = await runtime.launch(plan({"config": {"option": None}}))
    kube.running(terminated=True)
    assert await runtime.cleanup(IDENTITY, expected_pod_uid=launched.pod_uid) is True
    assert kube.objects == {}
    patches = [
        body
        for action, kind, _, body in kube.calls
        if action == "patch" and kind == "Pod"
    ]
    assert patches[0][0] == {
        "op": "test",
        "path": "/metadata/uid",
        "value": launched.pod_uid,
    }
    assert patches[0][1]["path"] == "/metadata/resourceVersion"
    assert patches[0][-1]["value"] == []


@pytest.mark.asyncio
async def test_cleanup_does_not_remove_another_controllers_finalizer():
    kube = MemoryKubernetes()
    runtime = kube.runtime()
    launched = await runtime.launch(plan())
    kube.running(terminated=True)
    kube.pod["metadata"]["finalizers"].append("example.test/owned")
    assert await runtime.cleanup(IDENTITY, expected_pod_uid=launched.pod_uid) is False
    assert kube.pod["metadata"]["finalizers"] == ["example.test/owned"]
    assert ("NetworkPolicy", "srw", IDENTITY.pod_name) in kube.objects


def test_importing_generic_runtime_never_loads_reference_harness_configuration():
    checked = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import orchestrator.services.generic_harness_runtime; assert not any(name == 'shared.runtime' or name.startswith('shared.runtime.') for name in sys.modules)",
        ],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert checked.returncode == 0, checked.stderr
