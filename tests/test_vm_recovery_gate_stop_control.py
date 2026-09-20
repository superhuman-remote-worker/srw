"""Gate-only retained stop evidence; fake Kubernetes exercises actual state changes."""

from copy import deepcopy
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

import pytest


def objects():
    job, user, operation, generation, vm, vmi, pod, pvc = [
        str(uuid4()) for _ in range(8)
    ]
    doc = dict(
        version=1,
        run_id="stop-test",
        job_id=job,
        user_id=user,
        operation_id=operation,
        generation=generation,
        namespace="gate-vms",
        vm_name="agent-vm-" + job,
        vm_uid=vm,
        vmi_name="agent-vm-" + job,
        vmi_uid=vmi,
        pod_name="virt-launcher-fixture",
        pod_uid=pod,
        pvc_uid=pvc,
        original_strategy="RerunOnFailure",
        stage="planned",
    )

    def obj(name, uid, owner=None):
        return {
            "metadata": {
                "name": name,
                "uid": uid,
                "namespace": "gate-vms",
                "resourceVersion": "1",
                "generation": 1,
                "finalizers": ["foreign/keep"],
                "labels": {"srw.io/owner-kind": "job", "srw.io/owner-id": job},
                "annotations": {"srw.io/provision-generation": generation},
                "ownerReferences": [owner] if owner else [],
            },
            "spec": {},
            "status": {},
        }

    vm_obj = obj(doc["vm_name"], vm)
    vm_obj["spec"]["runStrategy"] = "RerunOnFailure"
    vm_obj["status"]["desiredGeneration"] = 1
    vmi_obj = obj(
        doc["vmi_name"],
        vmi,
        dict(
            apiVersion="kubevirt.io/v1",
            kind="VirtualMachine",
            name=doc["vm_name"],
            uid=vm,
            controller=True,
        ),
    )
    pod_obj = obj(
        doc["pod_name"],
        pod,
        dict(
            apiVersion="kubevirt.io/v1",
            kind="VirtualMachineInstance",
            name=doc["vmi_name"],
            uid=vmi,
            controller=True,
        ),
    )
    return doc, {"vm": vm_obj, "vmi": vmi_obj, "pod": pod_obj}


class Kube:
    def __init__(self, state):
        self.state = deepcopy(state)
        self.events = []
        self.conflict = False
        self.lost = False

    async def assert_quiet(self, doc):
        from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
            GateStopError,
        )

        if getattr(self, "competitors", False):
            raise GateStopError("stop_runtime_ambiguous")

    async def read(self, doc, kind):
        return deepcopy(self.state.get(kind))

    async def patch(self, doc, kind, body):
        from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
            GateStopError,
        )

        old = self.state[kind]
        if self.conflict:
            self.conflict = False
            old["metadata"]["resourceVersion"] = str(
                int(old["metadata"]["resourceVersion"]) + 1
            )
            old["metadata"]["finalizers"].append("concurrent/keep")
            raise GateStopError("patch_uncertain")
        assert body["metadata"]["uid"] == old["metadata"]["uid"]
        assert body["metadata"]["resourceVersion"] == old["metadata"]["resourceVersion"]
        for key, value in deepcopy(body["metadata"]).items():
            if key == "annotations":
                for annotation, text in value.items():
                    if text is None:
                        old["metadata"].setdefault("annotations", {}).pop(
                            annotation, None
                        )
                    else:
                        old["metadata"].setdefault("annotations", {})[annotation] = text
            else:
                old["metadata"][key] = value
        if "spec" in body:
            old["spec"].update(body["spec"])
            old["metadata"]["generation"] += 1
            old["status"]["desiredGeneration"] = old["metadata"]["generation"]
            self.events.append(body["spec"]["runStrategy"])
            if body["spec"]["runStrategy"] == "Halted":
                from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
                    FINALIZER,
                )

                for child in ("pod", "vmi"):
                    if (
                        child in self.state
                        and FINALIZER not in self.state[child]["metadata"]["finalizers"]
                    ):
                        self.state.pop(child)
                        self.events.append("release-" + child)
        old["metadata"]["resourceVersion"] = str(
            int(old["metadata"]["resourceVersion"]) + 1
        )
        from orchestrator.operator_cli.vm_recovery_gate_stop_control import FINALIZER

        if kind != "vm" and FINALIZER not in old["metadata"]["finalizers"]:
            self.events.append("release-" + kind)
            self.state.pop(kind)
        if self.lost:
            self.lost = False
            raise GateStopError("patch_uncertain")


class Store:
    def __init__(self):
        self.doc = None
        self.receipt = False
        self.active = True
        self.events = []

    async def save(self, doc):
        self.doc = deepcopy(doc)
        self.events.append(doc["stage"])

    @asynccontextmanager
    async def authorized(self, doc, *, receipt=False):
        from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
            GateStopError,
        )

        async def check():
            if not self.active or (receipt and not self.receipt):
                raise GateStopError("authority_refused")

        await check()
        yield check

    async def cancel(self, doc):
        self.active = False
        self.events.append("cancel")


@pytest.mark.asyncio
async def test_retention_receipt_release_order_and_foreign_finalizers():
    from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
        GateStopControl,
        FINALIZER,
        GateStopError,
    )

    doc, state = objects()
    kube = Kube(state)
    store = Store()
    control = GateStopControl(kube, store, timeout=0.05, interval=0)
    await control.prepare(doc)
    assert store.doc["stage"] == "held"
    assert kube.events == ["Manual"]
    assert all(
        kube.state[k]["metadata"]["finalizers"] == ["foreign/keep", FINALIZER]
        for k in ["pod", "vmi"]
    )
    with pytest.raises(GateStopError):
        await control.release(doc)
    assert "pod" in kube.state
    store.receipt = True
    await control.release(doc)
    assert kube.events == ["Manual", "Halted", "release-pod", "release-vmi"]
    store.active = False
    with pytest.raises(GateStopError):
        await control.restore(doc)
    assert kube.events[-1] == "release-vmi"
    store.active = True
    await control.restore(doc)
    assert kube.events[-1] == "RerunOnFailure"
    assert store.doc["stage"] == "restored"


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["conflict", "lost"])
async def test_retry_revalidates_and_preserves_concurrent_finalizer(fault):
    from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
        GateStopControl,
        FINALIZER,
    )

    doc, state = objects()
    kube = Kube(state)
    store = Store()
    setattr(kube, fault, True)
    await GateStopControl(kube, store, timeout=0.1, interval=0).prepare(doc)
    assert FINALIZER in kube.state["vmi"]["metadata"]["finalizers"]
    if fault == "conflict":
        assert "concurrent/keep" in kube.state["vmi"]["metadata"]["finalizers"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["uid", "namespace", "owner", "deleting", "foreign_gate"]
)
async def test_wrong_object_never_receives_fault_retention(change):
    from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
        GateStopControl,
        GateStopError,
        MARKER,
    )

    doc, state = objects()
    meta = state["vmi"]["metadata"]
    if change == "uid":
        meta["uid"] = str(uuid4())
    elif change == "namespace":
        meta["namespace"] = "other"
    elif change == "owner":
        meta["ownerReferences"][0]["uid"] = str(uuid4())
    elif change == "deleting":
        meta["deletionTimestamp"] = "2026-09-20T00:00:00Z"
    else:
        meta["annotations"][MARKER] = "other-run"
    kube = Kube(state)
    store = Store()
    with pytest.raises(GateStopError):
        await GateStopControl(kube, store, timeout=0.01, interval=0).prepare(doc)
    assert not kube.events


@pytest.mark.asyncio
async def test_abort_reconstructs_after_partial_install_and_never_restarts():
    from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
        GateStopControl,
        FINALIZER,
        MARKER,
    )

    doc, state = objects()
    state["vmi"]["metadata"]["finalizers"].append(FINALIZER)
    state["vmi"]["metadata"]["annotations"][MARKER] = doc["operation_id"]
    kube = Kube(state)
    store = Store()
    await store.save(doc)
    await GateStopControl(kube, store, timeout=0.05, interval=0).abort(
        deepcopy(store.doc)
    )
    assert store.events.index("cancel") < store.events.index("aborted")
    assert kube.events == ["Halted", "release-pod", "release-vmi"]


@pytest.mark.asyncio
async def test_manual_ack_is_required_before_prepare_completes():
    from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
        GateStopControl,
        GateStopError,
    )

    doc, state = objects()
    kube = Kube(state)
    store = Store()
    original = kube.patch

    async def no_ack(doc, kind, body):
        await original(doc, kind, body)
        if kind == "vm":
            kube.state[kind]["status"]["desiredGeneration"] = 1

    kube.patch = no_ack
    with pytest.raises(GateStopError):
        await GateStopControl(kube, store, timeout=0.01, interval=0).prepare(doc)
    assert store.doc["stage"] == "planned"


@pytest.mark.asyncio
async def test_transport_cancellation_joins_inflight_patch_before_abort():
    import asyncio
    import threading
    from orchestrator.operator_cli.vm_recovery_gate_stop_control import joined_call

    entered, finish = threading.Event(), threading.Event()
    events = []

    def patch():
        entered.set()
        finish.wait(2)
        events.append("late-running-patch")

    task = asyncio.create_task(joined_call(patch))
    while not entered.is_set():
        await asyncio.sleep(0.001)
    task.cancel()
    await asyncio.sleep(0.01)
    assert not task.done()
    finish.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    events.append("abort-Halted")
    assert events == ["late-running-patch", "abort-Halted"]


@pytest.mark.asyncio
async def test_patch_transport_uses_merge_patch_uid_rv_and_timeout():
    from types import SimpleNamespace
    from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
        KubernetesStopObjects,
    )

    calls = []

    def call_api(*args, **kwargs):
        calls.append((args, kwargs))
        return {}

    port = KubernetesStopObjects(
        SimpleNamespace(api_client=SimpleNamespace(call_api=call_api))
    )
    doc, _ = objects()
    body = {
        "metadata": {
            "uid": doc["pod_uid"],
            "resourceVersion": "9",
            "finalizers": ["foreign/keep"],
        }
    }
    await port.patch(doc, "pod", body)
    args, kw = calls[0]
    assert args == ("/api/v1/namespaces/gate-vms/pods/virt-launcher-fixture", "PATCH")
    assert kw["header_params"]["Content-Type"] == "application/merge-patch+json"
    assert kw["body"] == body
    assert kw["_request_timeout"] == (3, 5)


@pytest.mark.asyncio
@pytest.mark.parametrize("lost_process", [False, True])
async def test_cli_scope_failure_and_cleanup_only_reconstruct_abort(
    monkeypatch, lost_process
):
    from types import SimpleNamespace
    from orchestrator.operator_cli import vm_recovery_gate_stop_control as gate
    from orchestrator.operator_cli.vm_workspace_recovery_acceptance import LiveScenario

    doc, state = objects()
    kube = Kube(state)
    store = Store()

    async def load():
        return [deepcopy(store.doc)] if store.doc else []

    store.load = load
    monkeypatch.setattr(gate, "KubernetesStopObjects", lambda _core: kube)
    monkeypatch.setattr(gate, "GateStopStore", lambda _db, _run: store)
    scenario = object.__new__(LiveScenario)
    scenario.run_id = doc["run_id"]
    scenario.gate_user_id = UUID(doc["user_id"])
    scenario.namespace = doc["namespace"]
    scenario.db = object()
    scenario._core = SimpleNamespace(
        list_namespaced_pod=lambda **kw: SimpleNamespace(
            items=[
                SimpleNamespace(
                    metadata=SimpleNamespace(uid=doc["pod_uid"], name=doc["pod_name"])
                )
            ]
        )
    )
    identity = dict(
        owner_kind="job",
        owner_id=doc["job_id"],
        provision_generation=doc["generation"],
        namespace=doc["namespace"],
        vm_uid=doc["vm_uid"],
        prior_vmi_uid=doc["vmi_uid"],
        prior_launcher_uid=doc["pod_uid"],
        root_pvc_uid=doc["pvc_uid"],
    )
    if lost_process:
        await gate.GateStopControl(kube, store, timeout=0.1, interval=0).prepare(doc)
        # A new scenario owns no in-memory document, and no principal is minted.
        scenario.gate_user_id = None
        await scenario._cleanup_stop_retention()
    else:
        with pytest.raises(RuntimeError, match="original failure"):
            async with scenario._positive_stop_retention(
                identity, UUID(doc["operation_id"])
            ):
                raise RuntimeError("original failure")
    assert store.doc["stage"] == "aborted"
    assert "RerunOnFailure" not in kube.events
    assert kube.events[-3:] == ["Halted", "release-pod", "release-vmi"]


def test_gate_role_grants_only_pod_metadata_patch():
    from pathlib import Path
    import yaml

    source = Path(
        "helm/templates/orchestrator/vm-workspace-recovery-gate.yaml"
    ).read_text()
    # Parse the static rule body without templated resource metadata.
    rules = yaml.safe_load(source.split("rules:\n", 1)[1].split("\n---", 1)[0])
    pod = next(rule for rule in rules if "pods" in rule["resources"])
    assert "patch" in pod["verbs"]
    assert pod["resources"] == ["pods"]
    pvc = next(rule for rule in rules if "persistentvolumeclaims" in rule["resources"])
    assert pvc["verbs"] == ["get", "list", "watch"]
    assert all("pods/status" not in rule["resources"] for rule in rules)


@pytest.mark.asyncio
async def test_fault_requires_current_retained_identity_before_exec(monkeypatch):
    from types import SimpleNamespace
    from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
        GateStopControl,
        GateStopError,
    )
    from orchestrator.operator_cli.vm_workspace_recovery_acceptance import LiveScenario
    import kubernetes.stream

    doc, state = objects()
    kube = Kube(state)
    store = Store()
    control = GateStopControl(kube, store, timeout=0.1, interval=0)
    await control.prepare(doc)
    scenario = object.__new__(LiveScenario)
    scenario.namespace = doc["namespace"]
    scenario._gate_stop = (control, doc)
    scenario._core = SimpleNamespace(connect_get_namespaced_pod_exec=object())
    calls = []
    monkeypatch.setattr(
        kubernetes.stream, "stream", lambda *a, **kw: calls.append((a, kw))
    )
    identity = {"prior_launcher_uid": doc["pod_uid"]}
    await scenario._crash_launcher(identity)
    assert len(calls) == 1 and calls[0][0][1] == doc["pod_name"]
    kube.state["pod"]["metadata"]["uid"] = str(uuid4())
    with pytest.raises(GateStopError):
        await scenario._crash_launcher(identity)
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_abort_releases_retained_children_when_vm_already_deleting():
    from orchestrator.operator_cli.vm_recovery_gate_stop_control import GateStopControl

    doc, state = objects()
    kube = Kube(state)
    store = Store()
    control = GateStopControl(kube, store, timeout=0.05, interval=0)
    await control.prepare(doc)
    kube.state["vm"]["metadata"]["deletionTimestamp"] = "2026-09-20T00:00:00Z"
    await control.abort(doc)
    assert kube.events == ["Manual", "release-pod", "release-vmi"]
    assert store.doc["stage"] == "aborted"


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_task", [True, False])
async def test_cli_cancel_during_late_restore_joins_then_halts(
    monkeypatch, cancel_task
):
    import asyncio
    import threading
    from types import SimpleNamespace
    from orchestrator.operator_cli import vm_recovery_gate_stop_control as gate
    from orchestrator.operator_cli.vm_workspace_recovery_acceptance import LiveScenario

    doc, state = objects()
    kube = Kube(state)
    store = Store()
    store.receipt = True

    async def load():
        return [deepcopy(store.doc)] if store.doc else []

    store.load = load
    monkeypatch.setattr(gate, "KubernetesStopObjects", lambda _core: kube)
    monkeypatch.setattr(gate, "GateStopStore", lambda _db, _run: store)
    scenario = object.__new__(LiveScenario)
    scenario.run_id = doc["run_id"]
    scenario.gate_user_id = UUID(doc["user_id"])
    scenario.namespace = doc["namespace"]
    scenario.db = object()
    scenario._core = SimpleNamespace(
        list_namespaced_pod=lambda **kw: SimpleNamespace(
            items=[
                SimpleNamespace(
                    metadata=SimpleNamespace(uid=doc["pod_uid"], name=doc["pod_name"])
                )
            ]
        )
    )
    identity = dict(
        owner_kind="job",
        owner_id=doc["job_id"],
        provision_generation=doc["generation"],
        namespace=doc["namespace"],
        vm_uid=doc["vm_uid"],
        prior_vmi_uid=doc["vmi_uid"],
        prior_launcher_uid=doc["pod_uid"],
        root_pvc_uid=doc["pvc_uid"],
    )
    entered, finish = threading.Event(), threading.Event()
    original = kube.patch

    async def patch(doc, kind, body):
        if body.get("spec", {}).get("runStrategy") == "RerunOnFailure":

            def late():
                entered.set()
                finish.wait(2)
                kube.state["vm"]["spec"]["runStrategy"] = "RerunOnFailure"
                kube.events.append("RerunOnFailure")

            await gate.joined_call(late)
        else:
            await original(doc, kind, body)

    kube.patch = patch

    async def execute():
        async with scenario._positive_stop_retention(
            identity, UUID(doc["operation_id"])
        ) as (control, current):
            await control.release(current)
            await control.restore(current)

    task = asyncio.create_task(execute())
    while not entered.is_set():
        await asyncio.sleep(0.001)
    if cancel_task:
        task.cancel()
    else:
        store.active = False
    await asyncio.sleep(0.01)
    assert not task.done()
    finish.set()
    with pytest.raises(asyncio.CancelledError if cancel_task else gate.GateStopError):
        await task
    assert kube.events[-2:] == ["RerunOnFailure", "Halted"]
    assert store.doc["stage"] == "aborted"


@pytest.mark.asyncio
@pytest.mark.parametrize("during_prepare", [True, False])
async def test_strategy_control_does_not_overwrite_concurrent_strategy(during_prepare):
    from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
        GateStopControl,
        GateStopError,
    )

    doc, state = objects()
    kube = Kube(state)
    store = Store()
    store.receipt = True
    control = GateStopControl(kube, store, timeout=0.05, interval=0)
    if during_prepare:
        original = kube.patch

        async def concurrent(doc, kind, body):
            await original(doc, kind, body)
            if kind == "pod":
                kube.state["vm"]["spec"]["runStrategy"] = "Halted"

        kube.patch = concurrent
        with pytest.raises(GateStopError):
            await control.prepare(doc)
    else:
        await control.prepare(doc)
        await control.release(doc)
        kube.state["vm"]["spec"]["runStrategy"] = "Manual"
        with pytest.raises(GateStopError):
            await control.restore(doc)
    assert "RerunOnFailure" not in kube.events


@pytest.mark.asyncio
async def test_fault_rechecks_manual_ack_and_pending_stop_request(monkeypatch):
    from types import SimpleNamespace
    import kubernetes.stream
    from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
        GateStopControl,
        GateStopError,
    )
    from orchestrator.operator_cli.vm_workspace_recovery_acceptance import LiveScenario

    doc, state = objects()
    kube = Kube(state)
    store = Store()
    control = GateStopControl(kube, store, timeout=0.1, interval=0)
    await control.prepare(doc)
    scenario = object.__new__(LiveScenario)
    scenario.namespace = doc["namespace"]
    scenario._gate_stop = (control, doc)
    scenario._core = SimpleNamespace(connect_get_namespaced_pod_exec=object())
    calls = []
    monkeypatch.setattr(kubernetes.stream, "stream", lambda *a, **kw: calls.append(1))
    kube.state["vm"]["status"]["stateChangeRequests"] = [{"action": "Stop"}]
    with pytest.raises(GateStopError):
        await scenario._crash_launcher({"prior_launcher_uid": doc["pod_uid"]})
    kube.state["vm"]["status"].pop("stateChangeRequests")
    kube.state["vm"]["status"]["desiredGeneration"] = 1
    with pytest.raises(GateStopError):
        await scenario._crash_launcher({"prior_launcher_uid": doc["pod_uid"]})
    assert not calls


@pytest.mark.asyncio
async def test_repeated_cancellation_cannot_interrupt_durable_cleanup_read(monkeypatch):
    import asyncio
    from types import SimpleNamespace
    from orchestrator.operator_cli import vm_recovery_gate_stop_control as gate
    from orchestrator.operator_cli.vm_workspace_recovery_acceptance import LiveScenario

    doc, state = objects()
    kube = Kube(state)
    store = Store()
    loading, finish, held = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def load():
        loading.set()
        await finish.wait()
        return [deepcopy(store.doc)]

    store.load = load
    monkeypatch.setattr(gate, "KubernetesStopObjects", lambda _core: kube)
    monkeypatch.setattr(gate, "GateStopStore", lambda _db, _run: store)
    scenario = object.__new__(LiveScenario)
    scenario.run_id = doc["run_id"]
    scenario.gate_user_id = UUID(doc["user_id"])
    scenario.namespace = doc["namespace"]
    scenario.db = object()
    scenario._core = SimpleNamespace(
        list_namespaced_pod=lambda **kw: SimpleNamespace(
            items=[
                SimpleNamespace(
                    metadata=SimpleNamespace(uid=doc["pod_uid"], name=doc["pod_name"])
                )
            ]
        )
    )
    identity = dict(
        owner_kind="job",
        owner_id=doc["job_id"],
        provision_generation=doc["generation"],
        namespace=doc["namespace"],
        vm_uid=doc["vm_uid"],
        prior_vmi_uid=doc["vmi_uid"],
        prior_launcher_uid=doc["pod_uid"],
        root_pvc_uid=doc["pvc_uid"],
    )

    async def execute():
        async with scenario._positive_stop_retention(
            identity, UUID(doc["operation_id"])
        ):
            held.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(execute())
    await held.wait()
    task.cancel()
    await loading.wait()
    task.cancel()
    await asyncio.sleep(0.01)
    assert not task.done()
    finish.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert store.doc["stage"] == "aborted"


@pytest.mark.asyncio
async def test_abort_already_halted_invalidates_unknown_remote_restore_cas():
    from orchestrator.operator_cli.vm_recovery_gate_stop_control import GateStopControl

    doc, state = objects()
    kube = Kube(state)
    store = Store()
    store.receipt = True
    control = GateStopControl(kube, store, timeout=0.05, interval=0)
    await control.prepare(doc)
    await control.release(doc)
    old_rv = kube.state["vm"]["metadata"]["resourceVersion"]
    delayed = {
        "metadata": {"uid": doc["vm_uid"], "resourceVersion": old_rv},
        "spec": {"runStrategy": "RerunOnFailure"},
    }
    await control.abort(doc)
    assert kube.state["vm"]["metadata"]["resourceVersion"] != old_rv
    with pytest.raises(AssertionError):
        await kube.patch(doc, "vm", delayed)
    assert kube.state["vm"]["spec"]["runStrategy"] == "Halted"
    assert (
        kube.state["vm"]["metadata"]["annotations"]["srw.io/provision-generation"]
        == doc["generation"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("migration", [{"migrationUid": str(uuid4())}, [], True])
async def test_prepare_refuses_migration_or_malformed_migration_state(migration):
    from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
        GateStopControl,
        GateStopError,
    )

    doc, state = objects()
    state["vmi"]["status"]["migrationState"] = migration
    kube = Kube(state)
    store = Store()
    with pytest.raises(GateStopError):
        await GateStopControl(kube, store, timeout=0.05, interval=0).prepare(doc)
    assert not kube.events


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "extra", ["pending_migration", "second_pod", "incomplete_list"]
)
async def test_actual_kube_port_refuses_pending_migrations_and_second_launchers(extra):
    from types import SimpleNamespace
    from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
        KubernetesStopObjects,
        GateStopError,
    )

    doc, state = objects()

    def call_api(path, method, **kwargs):
        assert method == "GET"
        if path.endswith("/pods"):
            items = [state["pod"]]
            if extra == "second_pod":
                items.append({"metadata": {"uid": str(uuid4())}})
            return {
                "items": items,
                "metadata": {"continue": "more" if extra == "incomplete_list" else ""},
            }
        assert path.endswith("/virtualmachineinstancemigrations")
        return {
            "metadata": {},
            "items": [
                {
                    "metadata": {"namespace": doc["namespace"]},
                    "spec": {"vmiName": doc["vmi_name"]},
                    "status": {"phase": "Pending"},
                }
            ]
            if extra == "pending_migration"
            else [],
        }

    port = KubernetesStopObjects(
        SimpleNamespace(api_client=SimpleNamespace(call_api=call_api))
    )
    with pytest.raises(GateStopError):
        await port.assert_quiet(doc)


@pytest.mark.asyncio
async def test_fault_rechecks_migration_and_second_launcher(monkeypatch):
    from types import SimpleNamespace
    import kubernetes.stream
    from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
        GateStopControl,
        GateStopError,
    )
    from orchestrator.operator_cli.vm_workspace_recovery_acceptance import LiveScenario

    doc, state = objects()
    kube = Kube(state)
    store = Store()
    control = GateStopControl(kube, store, timeout=0.1, interval=0)
    await control.prepare(doc)
    scenario = object.__new__(LiveScenario)
    scenario.namespace = doc["namespace"]
    scenario._gate_stop = (control, doc)
    scenario._core = SimpleNamespace(connect_get_namespaced_pod_exec=object())
    calls = []
    monkeypatch.setattr(kubernetes.stream, "stream", lambda *a, **kw: calls.append(1))
    kube.state["vmi"]["status"]["migrationState"] = {"migrationUid": str(uuid4())}
    with pytest.raises(GateStopError):
        await scenario._crash_launcher({"prior_launcher_uid": doc["pod_uid"]})
    kube.state["vmi"]["status"].pop("migrationState")
    kube.competitors = True
    with pytest.raises(GateStopError):
        await scenario._crash_launcher({"prior_launcher_uid": doc["pod_uid"]})
    assert not calls


@pytest.mark.asyncio
async def test_kube_port_allows_completed_or_unrelated_migrations():
    from types import SimpleNamespace
    from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
        KubernetesStopObjects,
    )

    doc, state = objects()

    def call_api(path, method, **kwargs):
        if path.endswith("/pods"):
            return {"metadata": {}, "items": [state["pod"]]}
        return {
            "metadata": {},
            "items": [
                {
                    "metadata": {"namespace": doc["namespace"]},
                    "spec": {"vmiName": doc["vmi_name"]},
                    "status": {"phase": "Succeeded"},
                },
                {
                    "metadata": {"namespace": doc["namespace"]},
                    "spec": {"vmiName": "another-vmi"},
                    "status": {"phase": "Pending"},
                },
            ],
        }

    await KubernetesStopObjects(
        SimpleNamespace(api_client=SimpleNamespace(call_api=call_api))
    ).assert_quiet(doc)


@pytest.mark.asyncio
async def test_gate_client_disables_implicit_http_retries(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from kubernetes import config
    from orchestrator.operator_cli.vm_workspace_recovery_acceptance import LiveScenario

    monkeypatch.setattr(config, "load_incluster_config", lambda: None)
    scenario = object.__new__(LiveScenario)
    scenario.db = SimpleNamespace(connect=AsyncMock(), close=AsyncMock())
    scenario.provisioner = SimpleNamespace(
        connect=lambda _db: None, disconnect=AsyncMock()
    )
    await scenario.connect()
    try:
        assert (
            scenario._gate_api_client.rest_client.pool_manager.connection_pool_kw[
                "retries"
            ].total
            == 0
        )
        assert scenario._core.api_client is scenario._custom.api_client
    finally:
        await scenario.close()


@pytest.mark.asyncio
async def test_positive_release_waits_halted_generation_before_child_removal():
    from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
        GateStopControl,
        GateStopError,
        FINALIZER,
    )

    doc, state = objects()
    kube = Kube(state)
    store = Store()
    store.receipt = True
    control = GateStopControl(kube, store, timeout=0.01, interval=0)
    await control.prepare(doc)
    original = kube.patch

    async def old_ack(doc, kind, body):
        await original(doc, kind, body)
        if body.get("spec", {}).get("runStrategy") == "Halted":
            kube.state["vm"]["status"]["desiredGeneration"] = 1

    kube.patch = old_ack
    with pytest.raises(GateStopError):
        await control.release(doc)
    assert FINALIZER in kube.state["pod"]["metadata"]["finalizers"]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [404, 500])
async def test_fixture_inventory_distinguishes_unknown_from_absence(status):
    from types import SimpleNamespace
    from kubernetes.client.exceptions import ApiException
    from orchestrator.operator_cli.vm_recovery_gate_stop_control import (
        KubernetesStopObjects,
        GateStopError,
    )

    doc, _ = objects()
    paths = []

    def call_api(path, method, **kwargs):
        paths.append(path)
        raise ApiException(status=status, reason="private response")

    port = KubernetesStopObjects(
        SimpleNamespace(api_client=SimpleNamespace(call_api=call_api))
    )
    if status == 404:
        assert await port.fixture_objects(doc["namespace"], doc["job_id"]) == dict(
            vm=None, vmi=None, dv=None, pvc=None
        )
        assert len(paths) == 4
    else:
        with pytest.raises(GateStopError, match="purge_observation_unknown"):
            await port.fixture_objects(doc["namespace"], doc["job_id"])


@pytest.mark.asyncio
async def test_cli_purge_freezes_authenticated_teardown_before_resource_reads(
    monkeypatch,
):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from orchestrator.operator_cli import vm_recovery_gate_stop_control as gate
    from orchestrator.operator_cli.vm_workspace_recovery_acceptance import LiveScenario
    from orchestrator.services.vm_provisioner import (
        VMTeardownIdentity,
        VMTeardownResult,
    )

    doc, _ = objects()
    identity = VMTeardownIdentity(doc["generation"], doc["vm_uid"], doc["pvc_uid"])
    capture = AsyncMock(return_value=identity)
    release = AsyncMock(return_value=VMTeardownResult("completed", True))

    class Purge:
        def __init__(self, db, kube, delete, run, namespace, **kw):
            assert capture.await_count == 1
            assert kw["anchor"] == {
                "generation": doc["generation"],
                "vm": doc["vm_uid"],
                "pvc": doc["pvc_uid"],
            }
            self.delete = delete

        async def run(self, job):
            assert await self.delete(str(job), purge_disk=True)

    monkeypatch.setattr(gate, "GateFixturePurge", Purge)
    scenario = object.__new__(LiveScenario)
    scenario.db = object()
    scenario.run_id = doc["run_id"]
    scenario.namespace = doc["namespace"]
    scenario._core = SimpleNamespace(api_client=object())
    scenario.provisioner = SimpleNamespace(
        capture_vm_teardown_identity=capture,
        release_vm_captured=release,
        delete_vm=AsyncMock(side_effect=AssertionError("must not recapture")),
    )
    await scenario._purge_fixture(UUID(doc["job_id"]))
    release.assert_awaited_once_with(
        doc["job_id"], identity, purge_disk=True, capture_snapshot=False
    )
