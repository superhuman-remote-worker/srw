"""Non-overlapping inventory publication, isolated from VM lifecycle handling."""

import asyncio
from contextlib import asynccontextmanager
import hmac
import json
import logging
from uuid import uuid4

import httpx

from shared.vm_inventory_transport import (
    OPERATION,
    RECEIPT_MAX_BYTES,
    decode_document,
    validate_envelope,
)
from shared.vm_lifecycle_auth import sign_payload, verify_payload, unsigned_payload
from shared.vm_resource_inventory import InventoryError, inventory_time, snapshot_digest


log = logging.getLogger(__name__)


class InventoryPublisher:
    def __init__(self, client, *, secret, timeout_seconds):
        if not secret:
            raise InventoryError("invalid_inventory_configuration")
        self.client, self.secret, self.timeout_seconds = client, secret, timeout_seconds

    async def publish(self, snapshot):
        digest = snapshot_digest(snapshot)
        request_id = str(uuid4())
        value = sign_payload(
            {"snapshot": snapshot, "digest": digest},
            direction="request",
            operation=OPERATION,
            secret=self.secret,
            request_id=request_id,
        )
        body = json.dumps(
            value, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        ).encode()
        async with asyncio.timeout(self.timeout_seconds):
            async with self.client.stream(
                "POST",
                "/api/internal/vm-resource-inventory/publish",
                content=body,
                headers={
                    "Content-Type": "application/json",
                    "Accept-Encoding": "identity",
                },
            ) as response:
                if (
                    response.status_code != 200
                    or response.headers.get("content-encoding", "identity").lower()
                    != "identity"
                ):
                    raise InventoryError("inventory_receipt_refused")
                raw = bytearray()
                async for chunk in response.aiter_raw():
                    if len(chunk) > RECEIPT_MAX_BYTES - len(raw):
                        raise InventoryError("inventory_receipt_limit")
                    raw.extend(chunk)
        result = decode_document(bytes(raw), max_bytes=RECEIPT_MAX_BYTES)
        validate_envelope(
            result,
            payload_fields={
                "accepted",
                "snapshot_id",
                "digest",
                "complete",
                "current",
                "received_at",
            },
            direction="response",
        )
        if not verify_payload(
            result,
            direction="response",
            operation=OPERATION,
            secret=self.secret,
            expected_correlation_id=request_id,
        ):
            raise InventoryError("inventory_receipt_untrusted")
        receipt = unsigned_payload(result)
        if (
            receipt["accepted"] is not True
            or type(receipt["current"]) is not bool
            or type(receipt["complete"]) is not bool
            or receipt["complete"] != snapshot["complete"]
            or receipt["snapshot_id"] != snapshot["snapshot_id"]
            or not isinstance(receipt["digest"], str)
            or not hmac.compare_digest(receipt["digest"], digest)
            or inventory_time(receipt["received_at"])
            < inventory_time(snapshot["finished_at"])
        ):
            raise InventoryError("inventory_receipt_mismatch")
        return receipt


class ResourceInventoryObserver:
    def __init__(self, *, collector, publisher, interval_seconds):
        self.collector, self.publisher, self.interval_seconds = (
            collector,
            publisher,
            interval_seconds,
        )

    async def run(self, stop):
        sequence = 0
        while not stop.is_set():
            sequence += 1
            try:
                snapshot = await self.collector.collect(sequence)
                await self.publisher.publish(snapshot)
            except asyncio.CancelledError:
                raise
            except Exception:
                # API exceptions may contain raw workload fields or credentials.
                log.warning("VM resource inventory collection or publication failed")
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.interval_seconds)
            except TimeoutError:
                pass


def _inventory_api_client():
    from kubernetes import client

    config = client.Configuration.get_default_copy()
    config.retries = 0
    return client.ApiClient(configuration=config)


@asynccontextmanager
async def _observer_resources(settings, *, base_url, secret):
    from kubernetes import client
    from vm_controller.resource_inventory import ResourceInventoryCollector

    if not base_url or not secret:
        raise InventoryError("invalid_inventory_configuration")
    with _inventory_api_client() as api:
        async with httpx.AsyncClient(
            base_url=base_url,
            timeout=settings.publication_timeout_seconds,
            follow_redirects=False,
            trust_env=False,
        ) as http:
            collector = ResourceInventoryCollector(
                core=client.CoreV1Api(api),
                custom=client.CustomObjectsApi(api),
                storage=client.StorageV1Api(api),
                cluster_id=settings.cluster_id,
                namespace=settings.namespace,
                controller_id=str(uuid4()),
                policy_digest=settings.policy_digest,
                label_keys=settings.label_keys,
                max_items=settings.max_items,
                max_bytes=settings.max_bytes,
                request_timeout_seconds=settings.request_timeout_seconds,
                collection_timeout_seconds=settings.collection_timeout_seconds,
            )
            yield ResourceInventoryObserver(
                collector=collector,
                publisher=InventoryPublisher(
                    http,
                    secret=secret,
                    timeout_seconds=settings.publication_timeout_seconds,
                ),
                interval_seconds=settings.publish_interval_seconds,
            )


@asynccontextmanager
async def inventory_observer_context(settings, *, base_url, secret, stop):
    if settings is None:
        yield
        return
    async with _observer_resources(
        settings, base_url=base_url, secret=secret
    ) as observer:
        task = asyncio.create_task(observer.run(stop), name="vm-resource-inventory")
        try:
            yield
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
