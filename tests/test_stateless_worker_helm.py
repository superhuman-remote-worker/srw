from __future__ import annotations

import os
from pathlib import Path
import signal
import shutil
import subprocess
import time

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "helm"


def _render(*settings: str, show_only: str | None = None) -> list[dict]:
    command = [
        "helm",
        "template",
        "stateless-worker-config-test",
        str(CHART),
        "-f",
        str(CHART / "ci/test-values.yaml"),
        # This fixture also enables autoscaling for CI coverage. Restore the
        # chart defaults for unit rendering; each case sets its own gates.
        "--set",
        "agent.stateless.enabled=false",
        "--set",
        "agent.stateless.autoscaling.enabled=false",
    ]
    if show_only:
        command.extend(["--show-only", show_only])
    for setting in settings:
        command.extend(["--set", setting])
    rendered = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return [document for document in yaml.safe_load_all(rendered) if document]


def _only_kind(documents: list[dict], kind: str) -> dict:
    matches = [document for document in documents if document.get("kind") == kind]
    assert len(matches) == 1
    return matches[0]


def _stateless_agent_container(deployment: dict) -> dict:
    containers = deployment["spec"]["template"]["spec"]["containers"]
    matches = [container for container in containers if container["name"] == "agent"]
    assert len(matches) == 1
    return matches[0]


@pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is not installed")
def test_stateless_worker_gate_is_independent_and_default_off() -> None:
    # The default-off claim is read from the chart itself. Renders here carry
    # helm/ci/test-values.yaml, which enables the stateless pool so CI can
    # validate the KEDA ScaledObject against its CRD schema -- a rendering
    # convenience, not the product default -- so that overlay can no longer
    # answer "what does a stock install do?".
    defaults = yaml.safe_load((CHART / "values.yaml").read_text())
    assert defaults["agent"]["stateless"]["enabled"] is False
    assert defaults["agent"]["stateless"]["autoscaling"]["enabled"] is False

    config_map = _only_kind(
        _render("agent.stateless.enabled=false", show_only="templates/configmap.yaml"),
        "ConfigMap",
    )

    assert config_map["data"]["STATELESS_SESSION_ENABLED"] == "false"
    assert config_map["data"]["STATELESS_CLOUD_PUSH_RECOVERY_ENABLED"] == "false"
    assert config_map["data"]["SESSION_REWIND_IDLE_CONVERSATION_ENABLED"] == "false"
    assert config_map["data"]["STATELESS_WORKER_ENABLED"] == "false"
    assert config_map["data"]["STATELESS_WORKER_DEFAULT_ENABLED"] == "false"
    assert config_map["data"]["COMPLETION_COMMANDS_ENABLED"] == "false"
    assert config_map["data"]["COMPLETION_STATUS_REORDER_ENABLED"] == "false"
    assert config_map["data"]["COMPLETION_FINALIZER_INLINE_DELAY_SECONDS"] == "0"
    assert config_map["data"]["WORKER_BATCH_MIN_WALL_SECONDS"] == "300"
    assert config_map["data"]["LANGGRAPH_STRICT_MSGPACK"] == "true"


@pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is not installed")
@pytest.mark.parametrize("enabled", [False, True])
def test_cloud_push_recovery_gate_reaches_orchestrator_and_rolls_executors(enabled):
    value = str(enabled).lower()
    settings = (
        "agent.stateless.enabled=true",
        f"agent.stateless.cloudPushRecoveryEnabled={value}",
    )
    config = _only_kind(
        _render(*settings, show_only="templates/configmap.yaml"), "ConfigMap"
    )
    deployment = _only_kind(
        _render(*settings, show_only="templates/agent/stateless-deployment.yaml"),
        "Deployment",
    )
    env = {
        entry["name"]: entry.get("value")
        for entry in _stateless_agent_container(deployment)["env"]
    }
    assert config["data"]["STATELESS_CLOUD_PUSH_RECOVERY_ENABLED"] == value
    assert env["STATELESS_CLOUD_PUSH_RECOVERY_ENABLED"] == value
    orchestrator = _only_kind(
        _render(*settings, show_only="templates/orchestrator/deployment.yaml"),
        "Deployment",
    )
    env = {
        entry["name"]: entry
        for entry in orchestrator["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert (
        env["STATELESS_CLOUD_PUSH_RECOVERY_ENABLED"]["valueFrom"]["configMapKeyRef"][
            "key"
        ]
        == "STATELESS_CLOUD_PUSH_RECOVERY_ENABLED"
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is not installed")
@pytest.mark.parametrize("enabled", [False, True])
def test_idle_conversation_rewind_gate_reaches_only_the_orchestrator(enabled):
    value = str(enabled).lower()
    settings = (f"agent.stateless.rewind.idleConversationEnabled={value}",)
    config = _only_kind(
        _render(*settings, show_only="templates/configmap.yaml"), "ConfigMap"
    )
    assert config["data"]["SESSION_REWIND_IDLE_CONVERSATION_ENABLED"] == value
    deployment = _only_kind(
        _render(*settings, show_only="templates/orchestrator/deployment.yaml"),
        "Deployment",
    )
    env = {
        entry["name"]: entry
        for entry in deployment["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert (
        env["SESSION_REWIND_IDLE_CONVERSATION_ENABLED"]["valueFrom"]["configMapKeyRef"][
            "key"
        ]
        == "SESSION_REWIND_IDLE_CONVERSATION_ENABLED"
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is not installed")
def test_worker_gate_requires_a_stateless_executor_pool() -> None:
    command = [
        "helm",
        "template",
        "stateless-worker-config-test",
        str(CHART),
        "-f",
        str(CHART / "ci/test-values.yaml"),
        "--set",
        "agent.stateless.enabled=false",
        "--set",
        "agent.stateless.worker.enabled=true",
    ]

    rendered = subprocess.run(command, capture_output=True, text=True)

    assert rendered.returncode != 0
    assert (
        "agent.stateless.worker.enabled requires agent.stateless.enabled"
        in rendered.stderr
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is not installed")
def test_completion_status_reorder_requires_completion_commands() -> None:
    command = [
        "helm",
        "template",
        "stateless-worker-config-test",
        str(CHART),
        "-f",
        str(CHART / "ci/test-values.yaml"),
        "--set",
        "orchestrator.completionCommandsEnabled=false",
        "--set",
        "orchestrator.completionStatusReorderEnabled=true",
    ]

    rendered = subprocess.run(command, capture_output=True, text=True)

    assert rendered.returncode != 0
    assert (
        "orchestrator.completionStatusReorderEnabled requires "
        "orchestrator.completionCommandsEnabled" in rendered.stderr
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is not installed")
def test_completion_control_settings_change_orchestrator_rollout_checksum() -> None:
    def checksum(*settings: str) -> str:
        deployment = _only_kind(
            _render(*settings, show_only="templates/orchestrator/deployment.yaml"),
            "Deployment",
        )
        return deployment["spec"]["template"]["metadata"]["annotations"][
            "checksum/completion-control-settings"
        ]

    disabled = checksum()
    commands = checksum("orchestrator.completionCommandsEnabled=true")
    reordered = checksum(
        "orchestrator.completionCommandsEnabled=true",
        "orchestrator.completionStatusReorderEnabled=true",
    )
    delayed = checksum(
        "orchestrator.completionCommandsEnabled=true",
        "orchestrator.completionFinalizerInlineDelaySeconds=15",
    )

    assert len({disabled, commands, reordered, delayed}) == 4


@pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is not installed")
def test_stateless_admission_settings_change_orchestrator_rollout_checksum() -> None:
    def checksum(*settings: str) -> str:
        deployment = _only_kind(
            _render(*settings, show_only="templates/orchestrator/deployment.yaml"),
            "Deployment",
        )
        return deployment["spec"]["template"]["metadata"]["annotations"][
            "checksum/stateless-admission-settings"
        ]

    disabled = checksum()
    sessions = checksum("agent.stateless.enabled=true")
    recovery = checksum(
        "agent.stateless.enabled=true",
        "agent.stateless.cloudPushRecoveryEnabled=true",
    )
    worker = checksum(
        "agent.stateless.enabled=true",
        "agent.stateless.worker.enabled=true",
    )
    default_worker = checksum(
        "agent.stateless.enabled=true",
        "agent.stateless.worker.enabled=true",
        "agent.stateless.worker.defaultEnabled=true",
    )

    assert len({disabled, sessions, recovery, worker, default_worker}) == 5


@pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is not installed")
def test_generic_pool_opens_sessions_without_opening_worker_admission() -> None:
    documents = _render(
        "agent.stateless.enabled=true",
        "agent.stateless.worker.enabled=false",
    )
    config_map = _only_kind(
        [
            document
            for document in documents
            if document.get("kind") == "ConfigMap"
            and str(document.get("metadata", {}).get("name", "")).endswith(
                "-remote-worker-config"
            )
        ],
        "ConfigMap",
    )
    stateless_deployments = [
        document
        for document in documents
        if document.get("kind") == "Deployment"
        and str(document.get("metadata", {}).get("name", "")).endswith(
            "-agent-stateless"
        )
    ]

    assert config_map["data"]["STATELESS_SESSION_ENABLED"] == "true"
    assert config_map["data"]["STATELESS_WORKER_ENABLED"] == "false"
    assert len(stateless_deployments) == 1
    assert _stateless_agent_container(stateless_deployments[0])["command"][
        -1
    ].startswith("exec python -m agent --mode stateless")


@pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is not installed")
def test_stateless_executor_grace_exceeds_shutdown_and_abort_budget() -> None:
    deployment = _only_kind(
        _render(
            "agent.stateless.enabled=true",
            show_only="templates/agent/stateless-deployment.yaml",
        ),
        "Deployment",
    )

    # Chart defaults are 300s graceful shutdown + 15s abort. The remaining
    # margin lets local resource drains publish the exact claimant ACK before
    # kubelet may send SIGKILL.
    assert (
        deployment["spec"]["template"]["spec"]["terminationGracePeriodSeconds"] == 360
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is not installed")
def test_stateless_executor_pods_are_born_with_the_process_zero_finalizer() -> None:
    """Without it the claimant-loss hold is a race the reconciler loses.

    The kubelet removes a finalizer-free Pod object ~0.7 s after its
    containers terminate; the reconciler ticks every 15 s and (by design)
    never accepts a 404 as process-zero proof, so a stolen claim's debt
    became unsettleable. The finalizer keeps the exact terminal UID
    readable until the orchestrator has recorded proof and releases it.
    """
    from orchestrator.services.agent_provisioner import (
        STATELESS_EXECUTOR_PROCESS_ZERO_FINALIZER,
    )

    def finalizers(*settings: str) -> list[str] | None:
        deployment = _only_kind(
            _render(
                "agent.stateless.enabled=true",
                *settings,
                show_only="templates/agent/stateless-deployment.yaml",
            ),
            "Deployment",
        )
        return deployment["spec"]["template"]["metadata"].get("finalizers")

    assert finalizers() == [STATELESS_EXECUTOR_PROCESS_ZERO_FINALIZER]
    assert finalizers("agent.stateless.processZeroFinalizer=false") is None


@pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is not installed")
def test_stateless_executor_execs_python_as_pid1() -> None:
    deployment = _only_kind(
        _render(
            "agent.stateless.enabled=true",
            show_only="templates/agent/stateless-deployment.yaml",
        ),
        "Deployment",
    )

    assert _stateless_agent_container(deployment)["command"] == [
        "sh",
        "-c",
        (
            "exec python -m agent --mode stateless --config worker_base "
            "--port 8001 --host 0.0.0.0 --loop"
        ),
    ]


@pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is not installed")
def test_rendered_stateless_shell_delivers_sigterm_to_agent(
    tmp_path: Path,
) -> None:
    deployment = _only_kind(
        _render(
            "agent.stateless.enabled=true",
            show_only="templates/agent/stateless-deployment.yaml",
        ),
        "Deployment",
    )
    command = _stateless_agent_container(deployment)["command"]
    ready_path = tmp_path / "ready"
    term_path = tmp_path / "term"
    # Model the editable source root used by the image, including safe-path
    # behavior; an adjacent agent.py is not the installed module entrypoint.
    source_root = tmp_path / "application-source"
    package = source_root / "agent"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (package / "__main__.py").write_text(
        """\
import os
from pathlib import Path
import signal


def stop(signum, _frame):
    Path(os.environ["TERM_PATH"]).write_text(str(signum))
    raise SystemExit(0)


signal.signal(signal.SIGTERM, stop)
Path(os.environ["READY_PATH"]).write_text(str(os.getpid()))
signal.pause()
"""
    )
    environment = {
        **os.environ,
        "READY_PATH": str(ready_path),
        "TERM_PATH": str(term_path),
        "PYTHONPATH": str(source_root),
        "PYTHONSAFEPATH": "1",
    }
    process = subprocess.Popen(
        command,
        cwd=tmp_path,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        deadline = time.monotonic() + 5
        while not ready_path.exists() and process.poll() is None:
            if time.monotonic() >= deadline:
                break
            time.sleep(0.01)

        assert ready_path.exists(), (
            f"probe did not become ready (returncode={process.poll()})"
        )
        # `exec` replaces the shell rather than leaving Python as a child.
        assert int(ready_path.read_text()) == process.pid

        process.terminate()
        assert process.wait(timeout=5) == 0
        assert term_path.read_text() == str(signal.SIGTERM.value)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)


@pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is not installed")
def test_stateless_worker_local_budget_override_reaches_both_planes() -> None:
    settings = (
        "agent.stateless.enabled=true",
        "agent.stateless.worker.enabled=true",
        "agent.stateless.worker.defaultEnabled=true",
        "agent.stateless.worker.batchMinWallSeconds=60",
    )
    documents = _render(*settings)
    config_map = next(
        document
        for document in documents
        if document.get("kind") == "ConfigMap"
        and "STATELESS_WORKER_ENABLED" in document.get("data", {})
    )
    config_map_name = config_map["metadata"]["name"]

    assert config_map["data"]["STATELESS_WORKER_ENABLED"] == "true"
    assert config_map["data"]["STATELESS_WORKER_DEFAULT_ENABLED"] == "true"
    assert config_map["data"]["STATELESS_SESSION_ENABLED"] == "true"
    assert config_map["data"]["WORKER_BATCH_MIN_WALL_SECONDS"] == "60"

    deployments = {
        document["metadata"]["labels"]["app.kubernetes.io/component"]: document
        for document in documents
        if document.get("kind") == "Deployment"
        and "app.kubernetes.io/component"
        in document.get("metadata", {}).get("labels", {})
    }
    orchestrator = next(
        container
        for container in deployments["orchestrator"]["spec"]["template"]["spec"][
            "containers"
        ]
        if container["name"] == "orchestrator"
    )
    env_by_name = {entry["name"]: entry for entry in orchestrator["env"]}
    for key in (
        "STATELESS_SESSION_ENABLED",
        "STATELESS_WORKER_ENABLED",
        "STATELESS_WORKER_DEFAULT_ENABLED",
        "WORKER_BATCH_MIN_WALL_SECONDS",
    ):
        assert env_by_name[key]["valueFrom"]["configMapKeyRef"] == {
            "name": config_map_name,
            "key": key,
        }

    executor = next(
        container
        for container in deployments["agent-stateless"]["spec"]["template"]["spec"][
            "containers"
        ]
        if container["name"] == "agent"
    )
    assert {
        entry["configMapRef"]["name"]
        for entry in executor["envFrom"]
        if "configMapRef" in entry
    } == {config_map_name}


@pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is not installed")
def test_completion_command_gate_true_reaches_control_and_execution_planes() -> None:
    documents = _render(
        "agent.stateless.enabled=true",
        "orchestrator.completionCommandsEnabled=true",
        "orchestrator.completionStatusReorderEnabled=true",
        "orchestrator.completionFinalizerInlineDelaySeconds=15",
    )
    config_map = next(
        document
        for document in documents
        if document.get("kind") == "ConfigMap"
        and "COMPLETION_COMMANDS_ENABLED" in document.get("data", {})
    )
    config_map_name = config_map["metadata"]["name"]
    assert config_map["data"]["COMPLETION_COMMANDS_ENABLED"] == "true"
    assert config_map["data"]["COMPLETION_STATUS_REORDER_ENABLED"] == "true"
    assert config_map["data"]["COMPLETION_FINALIZER_INLINE_DELAY_SECONDS"] == "15"

    deployments = {
        document["metadata"]["labels"]["app.kubernetes.io/component"]: document
        for document in documents
        if document.get("kind") == "Deployment"
        and "app.kubernetes.io/component"
        in document.get("metadata", {}).get("labels", {})
    }
    orchestrator = next(
        container
        for container in deployments["orchestrator"]["spec"]["template"]["spec"][
            "containers"
        ]
        if container["name"] == "orchestrator"
    )
    orchestrator_env = {entry["name"]: entry for entry in orchestrator["env"]}
    assert orchestrator_env["COMPLETION_COMMANDS_ENABLED"]["valueFrom"][
        "configMapKeyRef"
    ] == {
        "name": config_map_name,
        "key": "COMPLETION_COMMANDS_ENABLED",
    }
    assert orchestrator_env["COMPLETION_STATUS_REORDER_ENABLED"]["valueFrom"][
        "configMapKeyRef"
    ] == {
        "name": config_map_name,
        "key": "COMPLETION_STATUS_REORDER_ENABLED",
    }
    assert orchestrator_env["COMPLETION_FINALIZER_INLINE_DELAY_SECONDS"]["valueFrom"][
        "configMapKeyRef"
    ] == {
        "name": config_map_name,
        "key": "COMPLETION_FINALIZER_INLINE_DELAY_SECONDS",
    }

    executor = _stateless_agent_container(deployments["agent-stateless"])
    assert {
        entry["configMapRef"]["name"]
        for entry in executor["envFrom"]
        if "configMapRef" in entry
    } == {config_map_name}
    # The execution plane receives the exact shared key through envFrom; an
    # explicit container value would override it and split the flag state.
    assert "COMPLETION_COMMANDS_ENABLED" not in {
        entry["name"] for entry in executor.get("env", [])
    }


def test_tilt_overlay_explicitly_enables_short_worker_batches() -> None:
    values = yaml.safe_load((ROOT / "deployment/values-tilt.yaml").read_text())

    assert values["agent"]["stateless"]["enabled"] is True
    worker = values["agent"]["stateless"]["worker"]
    assert worker == {
        "enabled": True,
        "defaultEnabled": False,
        "batchMinWallSeconds": 60,
    }
