"""Inventory-only executor ownership and selected-effect independence."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
from copy import deepcopy
import threading

import pytest

from tests.test_vm_launcher_profile import installed_cr
from tests.test_vm_resource_effect_targeted import proof_case
from tests.test_vm_resource_inventory_collector import fixture, page
from tests.test_vm_resource_inventory_settings import load
from vm_controller import resource_inventory, resource_inventory_runtime as runtime


@pytest.mark.asyncio
async def test_full_inventory_sdk_and_normalization_use_one_dedicated_worker(
    monkeypatch,
):
    """A full LIST/GET/normalization must not rotate through default workers."""

    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="vm-inventory") as worker:
        collector, pages, _ = fixture()
        collector.collection_executor = worker
        node = pages["nodes"][0]["items"][0]
        node["metadata"]["labels"]["kubernetes.io/arch"] = "amd64"
        node["status"]["allocatable"].update(
            {
                "ephemeral-storage": "100G",
                "devices.kubevirt.io/tun": "8",
                "devices.kubevirt.io/vhost-net": "8",
            }
        )
        collector.protocol = 2
        collector.kubevirt_namespace = "kubevirt"
        collector.kubevirt_name = "kubevirt"
        collector.label_keys.append("kubernetes.io/arch")
        cr = installed_cr()
        cr["metadata"]["resourceVersion"] = "11"
        observed = []
        marker = ContextVar("inventory-marker")
        marker.set("owner-context")

        def record(method):
            def call(**kwargs):
                observed.append((threading.current_thread().name, marker.get(None)))
                return method(**kwargs)

            return call

        for api, name in (
            (collector.core, "list_node"),
            (collector.core, "list_pod_for_all_namespaces"),
            (collector.core, "list_namespaced_persistent_volume_claim"),
            (collector.core, "list_persistent_volume"),
            (collector.storage, "list_storage_class"),
            (collector.custom, "list_namespaced_custom_object"),
        ):
            setattr(api, name, record(getattr(api, name)))
        collector.core.list_namespaced_limit_range = record(lambda **kwargs: page([]))
        collector.custom.get_namespaced_custom_object = record(
            lambda **kwargs: deepcopy(cr)
        )
        original = resource_inventory.normalize_inventory

        def normalize(*args, **kwargs):
            observed.append((threading.current_thread().name, marker.get(None)))
            return original(*args, **kwargs)

        monkeypatch.setattr(resource_inventory, "normalize_inventory", normalize)
        for sequence in (1, 2):
            result = await collector.collect(sequence)
            assert result["complete"] is True
            await asyncio.gather(
                *(asyncio.to_thread(threading.get_ident) for _ in range(16))
            )
        assert observed and set(observed) == {("vm-inventory_0", "owner-context")}


@pytest.mark.asyncio
async def test_selected_effect_proof_can_finish_while_full_inventory_worker_is_busy():
    """The live selected GET path must never queue behind full normalization."""

    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="vm-inventory") as worker:
        case = proof_case()
        collector = case.controller.resource_inventory_collector
        collector.collection_executor = worker
        entered, release = threading.Event(), threading.Event()

        def blocked():
            entered.set()
            if not release.wait(2):
                raise TimeoutError("test worker not released")

        busy = worker.submit(blocked)
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            proof = await asyncio.wait_for(
                collector.collect_effect_proof(
                    node_name="node-a", storage_class_name="local"
                ),
                timeout=1,
            )
            assert proof["complete"] is True
        finally:
            release.set()
            busy.result(timeout=2)


@pytest.mark.asyncio
async def test_repeated_cancel_drains_dedicated_sdk_call_before_worker_exit():
    """A cancelled full collection cannot close the client under its worker."""

    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="vm-inventory") as worker:
        collector, _, _ = fixture()
        collector.collection_executor = worker
        entered, release, exited = (threading.Event() for _ in range(3))
        worker_thread = None

        def blocked(**kwargs):
            nonlocal worker_thread
            worker_thread = threading.current_thread()
            entered.set()
            try:
                if not release.wait(2):
                    raise TimeoutError("test worker not released")
                return page([])
            finally:
                exited.set()

        collector.core.list_node = blocked
        task = asyncio.create_task(collector.collect(1))
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done() and not exited.is_set()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert exited.is_set()
        assert worker_thread is not None and worker_thread.name == "vm-inventory_0"
    assert not worker_thread.is_alive()


@pytest.mark.asyncio
async def test_runtime_drains_worker_without_blocking_loop_before_api_close(
    monkeypatch,
):
    """An in-flight SDK request completes before API close and worker termination."""

    closed = []

    class Api:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            closed.append("api")

    monkeypatch.setattr(runtime, "_inventory_api_client", lambda: Api())
    worker_thread = None
    entered, release = threading.Event(), threading.Event()

    def blocked():
        nonlocal worker_thread
        worker_thread = threading.current_thread()
        entered.set()
        if not release.wait(2):
            raise TimeoutError("event loop blocked during shutdown")

    async def context():
        async with runtime._observer_resources(
            load(), base_url="http://orchestrator", secret=b"s" * 32
        ) as observer:
            worker = observer.collector.collection_executor
            task = asyncio.get_running_loop().run_in_executor(worker, blocked)
            assert await asyncio.to_thread(entered.wait, 2)
            asyncio.get_running_loop().call_later(0.02, release.set)
        await task

    await asyncio.wait_for(context(), timeout=1)
    assert closed == ["api"]
    assert worker_thread is not None and not worker_thread.is_alive()


@pytest.mark.asyncio
async def test_repeated_context_cancellation_keeps_api_open_until_shutdown(monkeypatch):
    """A second cancellation cannot detach a still-running SDK worker."""

    closed = []

    class Api:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            closed.append("api")

    monkeypatch.setattr(runtime, "_inventory_api_client", lambda: Api())
    entered, release, exited = (threading.Event() for _ in range(3))
    worker_thread = None

    def blocked():
        nonlocal worker_thread
        worker_thread = threading.current_thread()
        entered.set()
        try:
            if not release.wait(2):
                raise TimeoutError("test worker not released")
        finally:
            exited.set()

    async def context():
        async with runtime._observer_resources(
            load(), base_url="http://orchestrator", secret=b"s" * 32
        ) as observer:
            worker = observer.collector.collection_executor
            asyncio.get_running_loop().run_in_executor(worker, blocked)
            assert await asyncio.to_thread(entered.wait, 2)
            await asyncio.Event().wait()

    task = asyncio.create_task(context())
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        await asyncio.sleep(0.02)
        assert not task.done() and not closed
        task.cancel()
        await asyncio.sleep(0.02)
        assert not closed and not exited.is_set()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed == ["api"] and exited.is_set()
    assert worker_thread is not None and not worker_thread.is_alive()
