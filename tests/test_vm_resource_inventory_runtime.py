import asyncio
from contextlib import asynccontextmanager
import json
from types import SimpleNamespace

import httpx
import pytest

from shared.vm_inventory_transport import OPERATION
from shared.vm_lifecycle_auth import AUTH_FIELD, sign_payload
from shared.vm_resource_inventory import InventoryError
from vm_controller import resource_inventory_runtime as runtime
from tests.test_vm_resource_inventory_contract import snapshot
from tests.test_vm_resource_inventory_settings import load

SECRET = b"inventory-runtime-test-at-least-32-byte-secret"


def response_for(request, *, change=None):
    value = json.loads(request.content)
    snap = value["snapshot"]
    receipt = {
        "accepted": True,
        "snapshot_id": snap["snapshot_id"],
        "digest": value["digest"],
        "complete": snap["complete"],
        "current": True,
        "received_at": snap["finished_at"],
    }
    if change == "identity":
        receipt["snapshot_id"] = snap["controller_id"]
    if change == "unknown":
        receipt["raw"] = "private"
    if change == "bool":
        receipt["current"] = 1
    if change == "time":
        receipt["received_at"] = "2020-01-01"
    body = sign_payload(
        receipt,
        direction="response",
        operation=OPERATION,
        secret=SECRET,
        correlation_id=snap["controller_id"]
        if change == "correlation"
        else value[AUTH_FIELD]["request_id"],
    )
    if change == "mac":
        body[AUTH_FIELD]["signature"] = "a" * 64

    class Body(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield json.dumps(body).encode()

    return httpx.Response(200, stream=Body())


@pytest.mark.asyncio
async def test_publication_retries_same_snapshot_with_fresh_transport_identity():
    requests = []

    def handle(request):
        requests.append(json.loads(request.content))
        return response_for(request)

    async with httpx.AsyncClient(
        base_url="http://orchestrator", transport=httpx.MockTransport(handle)
    ) as client:
        publisher = runtime.InventoryPublisher(client, secret=SECRET, timeout_seconds=2)
        value = snapshot()
        await publisher.publish(value)
        await publisher.publish(value)
    assert requests[0]["snapshot"] == requests[1]["snapshot"]
    assert (
        requests[0][AUTH_FIELD]["request_id"] != requests[1][AUTH_FIELD]["request_id"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["identity", "unknown", "bool", "time", "correlation", "mac"]
)
async def test_publication_requires_exact_signed_bounded_receipt(change):
    async with httpx.AsyncClient(
        base_url="http://orchestrator",
        transport=httpx.MockTransport(lambda req: response_for(req, change=change)),
    ) as client:
        with pytest.raises(InventoryError):
            await runtime.InventoryPublisher(
                client, secret=SECRET, timeout_seconds=2
            ).publish(snapshot())


@pytest.mark.asyncio
async def test_response_stream_is_bounded_before_json_decode():
    class Oversized(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b" " * 3000
            yield b" " * 3000
            pytest.fail("read beyond receipt cap")

    async with httpx.AsyncClient(
        base_url="http://orchestrator",
        transport=httpx.MockTransport(
            lambda req: httpx.Response(200, stream=Oversized())
        ),
    ) as client:
        with pytest.raises(InventoryError, match="receipt"):
            await runtime.InventoryPublisher(
                client, secret=SECRET, timeout_seconds=2
            ).publish(snapshot())


@pytest.mark.asyncio
async def test_loop_serializes_rounds_and_survives_failure_without_raw_logging(caplog):
    stop = asyncio.Event()
    value = snapshot()
    seen = []

    async def collect(sequence):
        seen.append(sequence)
        return value

    async def publish(value):
        if len(seen) == 1:
            raise RuntimeError("private-credential")
        stop.set()

    observer = runtime.ResourceInventoryObserver(
        collector=SimpleNamespace(collect=collect),
        publisher=SimpleNamespace(publish=publish),
        interval_seconds=0.001,
    )
    await observer.run(stop)
    assert seen == [1, 2]
    assert "private-credential" not in caplog.text


@pytest.mark.asyncio
async def test_disabled_context_initializes_no_clients_or_tasks(monkeypatch):
    monkeypatch.setattr(
        runtime,
        "_observer_resources",
        lambda *a, **kw: pytest.fail("initialized while off"),
    )
    async with runtime.inventory_observer_context(
        None, base_url="", secret=None, stop=asyncio.Event()
    ) as observer:
        assert observer is None


@pytest.mark.asyncio
async def test_enabled_context_yields_active_observer_until_resources_close(monkeypatch):
    entered, joined = asyncio.Event(), asyncio.Event()
    collector = object()

    class Observer:
        def __init__(self):
            self.collector = collector

        async def run(self, stop):
            entered.set()
            try:
                await stop.wait()
            finally:
                joined.set()

    observer = Observer()

    @asynccontextmanager
    async def resources(*a, **kw):
        try:
            yield observer
        finally:
            assert joined.is_set()

    monkeypatch.setattr(runtime, "_observer_resources", resources)
    async with runtime.inventory_observer_context(
        load(), base_url="http://orchestrator", secret=SECRET, stop=asyncio.Event()
    ) as active:
        await entered.wait()
        assert active is observer
        assert active.collector is collector
        assert not joined.is_set()
    assert joined.is_set()


@pytest.mark.asyncio
async def test_shutdown_cancels_and_awaits_observer_before_closing_clients(monkeypatch):
    entered, joined = asyncio.Event(), asyncio.Event()

    class Observer:
        async def run(self, stop):
            try:
                entered.set()
                await asyncio.Event().wait()
            finally:
                await asyncio.sleep(0)
                joined.set()

    @asynccontextmanager
    async def resources(*a, **kw):
        try:
            yield Observer()
        finally:
            assert joined.is_set()

    monkeypatch.setattr(runtime, "_observer_resources", resources)
    async with runtime.inventory_observer_context(
        load(), base_url="http://orchestrator", secret=SECRET, stop=asyncio.Event()
    ):
        await entered.wait()
    assert joined.is_set()


def test_observer_kubernetes_client_disables_retries_without_mutating_defaults():
    from kubernetes import client

    before = client.Configuration.get_default_copy().retries
    with runtime._inventory_api_client() as api:
        assert api.configuration.retries == 0
    assert client.Configuration.get_default_copy().retries == before


@pytest.mark.asyncio
async def test_total_publication_timeout_closes_slow_response_stream():
    closed = asyncio.Event()

    class Slow(httpx.AsyncByteStream):
        async def __aiter__(self):
            await asyncio.Event().wait()
            yield b"unreachable"

        async def aclose(self):
            closed.set()

    async with httpx.AsyncClient(
        base_url="http://orchestrator",
        transport=httpx.MockTransport(lambda req: httpx.Response(200, stream=Slow())),
    ) as client:
        with pytest.raises(TimeoutError):
            await runtime.InventoryPublisher(
                client, secret=SECRET, timeout_seconds=0.02
            ).publish(snapshot())
    assert closed.is_set()


@pytest.mark.asyncio
async def test_controller_transport_failure_still_closes_observer_context(monkeypatch):
    from vm_controller import controller
    from shared.vm_resource_inventory_settings import InventorySettings
    from unittest.mock import AsyncMock

    monkeypatch.setattr(controller, "TRANSPORT", "http")
    monkeypatch.setattr(InventorySettings, "from_environment", lambda: None)
    entered, closed = [], []

    @asynccontextmanager
    async def context(*a, **kw):
        entered.append(True)
        try:
            yield
        finally:
            closed.append(True)

    monkeypatch.setattr(runtime, "inventory_observer_context", context)
    service = controller.VMController.__new__(controller.VMController)
    service.load_template = lambda: None
    service.init_k8s = lambda: None
    service.headscale = SimpleNamespace(init=AsyncMock())
    service._shutdown = asyncio.Event()
    service._run_transports = AsyncMock(side_effect=RuntimeError("transport failed"))
    with pytest.raises(RuntimeError, match="transport failed"):
        await service.run()
    assert entered == closed == [True]


@pytest.mark.asyncio
async def test_controller_binds_live_collector_for_transport_lifetime(monkeypatch):
    from vm_controller import controller
    from shared.vm_resource_inventory_settings import InventorySettings
    from unittest.mock import AsyncMock

    collector = object()
    entered, joined = asyncio.Event(), asyncio.Event()

    class Observer:
        def __init__(self):
            self.collector = collector

        async def run(self, stop):
            entered.set()
            try:
                await stop.wait()
            finally:
                joined.set()

    @asynccontextmanager
    async def resources(*a, **kw):
        try:
            yield Observer()
        finally:
            assert joined.is_set()

    monkeypatch.setattr(runtime, "_observer_resources", resources)
    monkeypatch.setattr(controller, "TRANSPORT", "http")
    settings = load()
    monkeypatch.setattr(InventorySettings, "from_environment", lambda: settings)
    service = controller.VMController.__new__(controller.VMController)
    service.load_template = lambda: None
    service.init_k8s = lambda: None
    service.headscale = SimpleNamespace(init=AsyncMock())
    service._shutdown = asyncio.Event()

    async def transports():
        await entered.wait()
        assert service.resource_inventory_collector is collector
        assert not joined.is_set()

    service._run_transports = transports
    await service.run()
    assert service.resource_inventory_collector is None
    assert joined.is_set()
