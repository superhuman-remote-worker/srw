"""The phone-line driver ``srw.gpt-live-sip/v1`` (live_session_jobs §5.1–5.3).

Its control-plane class (validate, bind, no upstream lease credential), its
spec and the contract's live/standing rules, its install gating in the
registry, the control token in its pod's request file, and the reconciler's
standing binding that keeps its pod up with no lease.
"""

from __future__ import annotations

import contextlib
import dataclasses
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from orchestrator.security.crypto import live_control_token
from orchestrator.services import connector_service_hosting as hosting
from orchestrator.services.connector_drivers import builtin_connector_drivers
from orchestrator.services.connector_drivers.base import (
    BindContext,
    ConnectorDraft,
    ValidationContext,
)
from orchestrator.services.connector_drivers.gpt_live_sip import GptLiveSipDriver
from orchestrator.services.connector_egress import EgressPins
from orchestrator.services.connector_service_hosting import (
    ServiceHostingReconciler,
    _Binding,
)
from orchestrator.services.connector_service_images import (
    ServiceImageRefused,
    ServiceImageUnavailable,
)
from orchestrator.services.connector_service_launch import (
    ServicePodIdentity,
    service_request,
)
from shared.connectors.builtin import (
    DATASOURCE_SPECS,
    ECHO_SERVICE_SPEC,
    GIT_SWAP_SPEC,
    GPT_LIVE_SIP_SPEC,
    LIVE_SPECS,
    OFFICIAL_SERVICE_SPECS,
    spec_for_type,
)
from shared.connectors.contract import validate_spec

IMAGE = "ghcr.io/srw/driver-gpt-live-sip:1"
SECRET = "whsec_" + "A" * 32
DIGEST = "sha256:" + "a" * 64


def _row(**over: Any) -> dict[str, Any]:
    """A catalog row for a live_voice model on an endpoint."""
    row = {
        "api_key": "sk-live",
        "provider_kind": "endpoint",
        "endpoint_base_url": "https://llm.example.test/v1",
        "model_id": "gpt-live-1",
    }
    row.update(over)
    return row


def _ctx(model_row: Any = None, *, store: Any = ...) -> ValidationContext:
    if store is ...:
        store = SimpleNamespace(
            resolve_catalog_model=AsyncMock(
                return_value=_row() if model_row is None else model_row
            )
        )
    return ValidationContext(
        environment=MagicMock(),
        can_autonomous_send=AsyncMock(return_value=False),
        store=store,
    )


def _draft(
    config: Any = None,
    credentials: Any = None,
    connection_url: str | None = None,
) -> ConnectorDraft:
    return ConnectorDraft(
        name="Reception",
        connection_url=connection_url,
        credentials=credentials,
        config=config,
        read_only=None,
        is_global=None,
        default_branch=None,
    )


async def _validate(config=None, credentials=None, *, existing=None, ctx=None, **kw):
    driver = GptLiveSipDriver(IMAGE)
    return await driver.validate(
        _draft(config, credentials, **kw), existing=existing, ctx=ctx or _ctx()
    )


async def _refused(*args, **kwargs) -> HTTPException:
    with pytest.raises(HTTPException) as error:
        await _validate(*args, **kwargs)
    return error.value


# =============================================================================
# validate
# =============================================================================


class TestValidateConfig:
    @pytest.mark.asyncio
    async def test_a_full_config_is_kept_with_the_derived_egress(self):
        normalized = await _validate(
            {
                "model": " gpt-live-1 ",
                "max_duration_s": 600,
                "no_job_fallback_s": 10,
                "transfer_target": "sip:desk@pbx.example.test",
            },
            {"webhook_secret": SECRET},
        )
        assert normalized.connection_url is None
        assert normalized.config == {
            "model": "gpt-live-1",
            "max_duration_s": 600,
            "no_job_fallback_s": 10,
            "transfer_target": "sip:desk@pbx.example.test",
            "api_host": "llm.example.test",
            "api_port": 443,
        }
        assert normalized.credentials == {"webhook_secret": SECRET}

    @pytest.mark.asyncio
    async def test_the_model_is_looked_up_as_live_voice(self):
        ctx = _ctx()
        await _validate({"model": "gpt-live-1"}, ctx=ctx)
        ctx.store.resolve_catalog_model.assert_awaited_once_with(
            "gpt-live-1", capability="live_voice"
        )

    @pytest.mark.asyncio
    async def test_unknown_keys_are_refused(self):
        error = await _refused({"model": "gpt-live-1", "voice": "marin", "zz": 1})
        assert error.status_code == 400
        assert "voice" in error.detail and "zz" in error.detail

    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", [59, 14401, 0, -1, True, 600.0, "600", None])
    async def test_max_duration_is_60_to_14400_whole_seconds(self, value):
        error = await _refused({"model": "gpt-live-1", "max_duration_s": value})
        assert error.status_code == 400 and "max_duration_s" in error.detail

    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", [60, 14400])
    async def test_max_duration_bounds_are_inclusive(self, value):
        normalized = await _validate({"model": "gpt-live-1", "max_duration_s": value})
        assert normalized.config["max_duration_s"] == value

    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", [0, 121, False, 8.5, "8"])
    async def test_no_job_fallback_is_1_to_120_seconds(self, value):
        error = await _refused({"model": "gpt-live-1", "no_job_fallback_s": value})
        assert error.status_code == 400 and "no_job_fallback_s" in error.detail

    @pytest.mark.asyncio
    @pytest.mark.parametrize("value", [1, 120])
    async def test_no_job_fallback_bounds_are_inclusive(self, value):
        normalized = await _validate(
            {"model": "gpt-live-1", "no_job_fallback_s": value}
        )
        assert normalized.config["no_job_fallback_s"] == value

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "key, high", [("greeting_delay_ms", 10000), ("greeting_retry_ms", 30000)]
    )
    async def test_the_greeting_timing_the_pod_reads_is_offered(self, key, high):
        """drivers/gpt-live-sip reads both (config.go); an engine that speaks
        an exact greeting at once (SRW's voice gateway) takes 0."""
        for value in (0, high):
            normalized = await _validate({"model": "gpt-live-1", key: value})
            assert normalized.config[key] == value
        for value in (-1, high + 1, True, 1.5, "0", None):
            error = await _refused({"model": "gpt-live-1", key: value})
            assert error.status_code == 400 and key in error.detail
        schema = GPT_LIVE_SIP_SPEC.config_schema["properties"][key]
        assert (schema["minimum"], schema["maximum"]) == (0, high)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("target", ["sip:+4969123@pbx.test", "tel:+4969123"])
    async def test_a_sip_or_tel_transfer_target(self, target):
        normalized = await _validate({"model": "gpt-live-1", "transfer_target": target})
        assert normalized.config["transfer_target"] == target

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "target",
        ["http://pbx.test", "sip:", "sip:a b", "+4969123", 42, "tel:" + "1" * 501],
    )
    async def test_any_other_transfer_target_is_refused(self, target):
        error = await _refused({"model": "gpt-live-1", "transfer_target": target})
        assert error.status_code == 400 and "transfer_target" in error.detail

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "config", [{}, {"model": ""}, {"model": "  "}, {"model": 3}]
    )
    async def test_a_line_needs_a_model(self, config):
        error = await _refused(config)
        assert error.status_code == 400 and "live voice model" in error.detail

    @pytest.mark.asyncio
    async def test_a_create_without_config_is_refused(self):
        error = await _refused(None)
        assert error.status_code == 400 and "live voice model" in error.detail

    @pytest.mark.asyncio
    async def test_an_update_without_config_keeps_the_stored_one(self):
        ctx = _ctx()
        normalized = await _validate(None, existing={"id": "x"}, ctx=ctx)
        assert normalized.config is None
        assert normalized.credentials is None
        ctx.store.resolve_catalog_model.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_connection_url_is_refused(self):
        error = await _refused(
            {"model": "gpt-live-1"}, connection_url="sip:line@example.test"
        )
        assert error.status_code == 400 and "no URL" in error.detail


class TestValidateTheEngineHost:
    @pytest.mark.asyncio
    async def test_an_unknown_model_is_refused(self):
        ctx = _ctx(
            store=SimpleNamespace(resolve_catalog_model=AsyncMock(return_value=None))
        )
        error = await _refused({"model": "gpt-nope"}, ctx=ctx)
        assert error.status_code == 400 and "gpt-nope" in error.detail

    @pytest.mark.asyncio
    async def test_a_model_without_a_key_is_refused(self):
        error = await _refused({"model": "gpt-live-1"}, ctx=_ctx(_row(api_key=None)))
        assert error.status_code == 400

    @pytest.mark.asyncio
    async def test_without_a_store_the_catalog_is_unavailable(self):
        error = await _refused({"model": "gpt-live-1"}, ctx=_ctx(store=None))
        assert error.status_code == 503

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("row", "host", "port"),
        [
            (
                _row(endpoint_base_url="https://llm.example.test/v1"),
                "llm.example.test",
                443,
            ),
            (
                _row(endpoint_base_url="https://llm.example.test:8443/v1"),
                "llm.example.test",
                8443,
            ),
            (_row(endpoint_base_url="http://gateway.local/v1"), "gateway.local", 80),
            (
                _row(
                    provider_kind="system",
                    provider_ref="openai",
                    endpoint_base_url=None,
                ),
                "api.openai.com",
                443,
            ),
        ],
    )
    async def test_the_egress_is_the_models_endpoint(self, row, host, port):
        normalized = await _validate({"model": "gpt-live-1"}, ctx=_ctx(row))
        assert normalized.config["api_host"] == host
        assert normalized.config["api_port"] == port

    @pytest.mark.asyncio
    async def test_a_client_supplied_egress_is_dropped_and_derived(self):
        normalized = await _validate(
            {"model": "gpt-live-1", "api_host": "evil.test", "api_port": 25}
        )
        assert normalized.config["api_host"] == "llm.example.test"
        assert normalized.config["api_port"] == 443

    @pytest.mark.asyncio
    async def test_an_endpoint_without_a_host_is_refused(self):
        error = await _refused(
            {"model": "gpt-live-1"}, ctx=_ctx(_row(endpoint_base_url="file:///x"))
        )
        assert error.status_code == 400 and "no host" in error.detail


class TestValidateCredentials:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "secret",
        [
            "whsec_" + "A" * 16,
            "whsec_++//++//++//++//++//++//YWI=",
            "whsec_" + "Z" * 512,
            SECRET,
        ],
    )
    async def test_a_webhook_secret(self, secret):
        normalized = await _validate(
            {"model": "gpt-live-1"}, {"webhook_secret": secret}
        )
        assert normalized.credentials == {"webhook_secret": secret}

    @pytest.mark.asyncio
    async def test_the_secret_is_optional(self):
        normalized = await _validate({"model": "gpt-live-1"}, {})
        assert not normalized.credentials

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "credentials",
        [
            {"webhook_secret": "A" * 32},
            {"webhook_secret": "whsec_short"},
            {"webhook_secret": "whsec_" + "A" * 16 + "!"},
            {"webhook_secret": "whsec_" + "A" * 513},
            # The right characters, but no base64 the webhook check decodes:
            # every delivery to the line would be refused.
            {"webhook_secret": "whsec_" + "A" * 17},
            {"webhook_secret": "whsec_abc+/=" + "x" * 20},
            {"webhook_secret": 12345},
            {"webhook_secret": SECRET, "api_key": "sk-x"},
            {"api_key": "sk-x"},
        ],
    )
    async def test_anything_else_is_refused(self, credentials):
        error = await _refused({"model": "gpt-live-1"}, credentials)
        assert error.status_code == 400 and "webhook_secret" in error.detail

    @pytest.mark.asyncio
    async def test_an_update_with_blank_credentials_keeps_the_secret(self):
        normalized = await _validate({"model": "gpt-live-1"}, {}, existing={"id": "x"})
        assert normalized.credentials is None


# =============================================================================
# bind and the lease
# =============================================================================


class TestBindAndLease:
    def test_the_payload_entry_names_the_line_and_no_secret(self):
        driver = GptLiveSipDriver(IMAGE)
        connector_id = str(uuid4())
        row = {
            "id": connector_id,
            "type": "gpt_live_sip",
            "name": "Reception",
            "description": "The front desk",
            "connection_url": "should-not-leak",
            "config": {"model": "gpt-live-1"},
        }
        entry = driver.bind(
            row,
            {"webhook_secret": SECRET},
            ctx=BindContext(
                gates=MagicMock(), logger=MagicMock(), default_known_hosts=""
            ),
        )
        assert entry["credentials"] == {}
        assert entry["datasource_id"] == connector_id
        assert entry["connection_url"] is None
        assert entry["type"] == "gpt_live_sip" and entry["name"] == "Reception"
        assert SECRET not in repr(entry)

    def test_no_upstream_credential_is_exchanged_for_a_lease(self):
        with pytest.raises(ValueError, match="no upstream credential"):
            GptLiveSipDriver(IMAGE).lease_upstream({"id": "x"})

    def test_the_driver_runs_the_charts_image(self):
        driver = GptLiveSipDriver(IMAGE)
        assert driver.spec is GPT_LIVE_SIP_SPEC
        assert driver.image_reference == IMAGE


# =============================================================================
# The spec and the contract's live rules
# =============================================================================


def _service(**over):
    return dataclasses.replace(GPT_LIVE_SIP_SPEC.service, **over)


class TestSpec:
    def test_the_shipped_spec_is_valid(self):
        assert validate_spec(GPT_LIVE_SIP_SPEC) == []

    def test_its_shape(self):
        spec = GPT_LIVE_SIP_SPEC
        assert spec.name == "srw.gpt-live-sip/v1"
        assert spec.live is True and spec.plane == "service"
        assert spec.service.standing is True and spec.service.idle_seconds is None
        assert spec.service.callers == ("harness",)
        assert spec.credential_delivery == "lease"
        assert spec.delivery_forms == ("lease_token",)
        assert spec.config_schema["required"] == ["model"]
        assert [slot.name for slot in spec.credential_slots] == ["webhook_secret"]

    def test_lookup_and_lists(self):
        assert spec_for_type("gpt_live_sip") is GPT_LIVE_SIP_SPEC
        assert GPT_LIVE_SIP_SPEC in LIVE_SPECS
        assert GPT_LIVE_SIP_SPEC in OFFICIAL_SERVICE_SPECS
        # Not demanded on every installation.
        assert GPT_LIVE_SIP_SPEC not in DATASOURCE_SPECS

    def test_a_live_driver_runs_a_standing_pod(self):
        spec = dataclasses.replace(GPT_LIVE_SIP_SPEC, service=_service(standing=False))
        assert "a live driver runs a standing service pod" in validate_spec(spec)

    def test_a_live_driver_without_a_service_is_refused(self):
        spec = dataclasses.replace(GPT_LIVE_SIP_SPEC, plane="control", service=None)
        assert "a live driver runs a standing service pod" in validate_spec(spec)

    @pytest.mark.parametrize("callers", [("workspace",), ("harness", "workspace")])
    def test_a_live_pod_is_called_by_the_agent_process_only(self, callers):
        spec = dataclasses.replace(GPT_LIVE_SIP_SPEC, service=_service(callers=callers))
        assert (
            "a live driver's pod is called by the agent process only"
            in validate_spec(spec)
        )

    def test_a_standing_pod_never_idles(self):
        spec = dataclasses.replace(GPT_LIVE_SIP_SPEC, service=_service(idle_seconds=60))
        assert "a standing service pod never idles: no idle_seconds" in validate_spec(
            spec
        )

    def test_standing_is_a_boolean(self):
        spec = dataclasses.replace(GPT_LIVE_SIP_SPEC, service=_service(standing=1))
        assert "service standing is a boolean" in validate_spec(spec)

    def test_live_is_a_boolean(self):
        spec = dataclasses.replace(GPT_LIVE_SIP_SPEC, live="yes")
        assert "live is a boolean" in validate_spec(spec)

    def test_a_standing_pod_need_not_be_live(self):
        spec = dataclasses.replace(
            ECHO_SERVICE_SPEC,
            service=dataclasses.replace(
                ECHO_SERVICE_SPEC.service, standing=True, idle_seconds=None
            ),
        )
        assert validate_spec(spec) == []


# =============================================================================
# Install gating
# =============================================================================


class TestRegistry:
    def test_the_driver_is_installed_only_with_an_image(self):
        assert not any(
            isinstance(driver, GptLiveSipDriver)
            for driver in builtin_connector_drivers().drivers()
        )
        assert builtin_connector_drivers().for_type("gpt_live_sip") is None
        registry = builtin_connector_drivers(gpt_live_sip_image=IMAGE)
        driver = registry.for_type("gpt_live_sip")
        assert isinstance(driver, GptLiveSipDriver)
        assert driver.image_reference == IMAGE
        assert registry.get(GPT_LIVE_SIP_SPEC.name) is driver

    def test_an_empty_image_installs_nothing(self):
        registry = builtin_connector_drivers(gpt_live_sip_image="")
        assert registry.for_type("gpt_live_sip") is None


# =============================================================================
# The pod's request file
# =============================================================================


def _request(spec, config):
    connector_id = str(uuid4())
    identity = ServicePodIdentity(
        identity_id=str(uuid4()),
        connector_id=connector_id,
        driver=spec.name,
        digest=DIGEST,
        generation="g1",
    )
    request = service_request(
        identity,
        spec=spec,
        config=config,
        credentials={"webhook_secret": SECRET},
        pins=EgressPins(hosts=(), resolved_at=datetime.now(timezone.utc)),
        policy=SimpleNamespace(exchange_host="srw-exchange.test", exchange_port=8088),
    )
    return connector_id, request


class TestServiceRequest:
    def test_a_live_pod_gets_its_control_token(self):
        connector_id, request = _request(
            GPT_LIVE_SIP_SPEC,
            {"model": "gpt-live-1", "api_host": "api.openai.com", "api_port": 443},
        )
        assert request["control"] == {"token": live_control_token(connector_id)}
        # A lease driver's pod holds no connector credential: the webhook
        # secret stays in the orchestrator.
        assert request["credentials"] == {}
        assert SECRET not in repr(request)
        assert request["connector"]["config"]["model"] == "gpt-live-1"

    def test_the_token_is_per_connector_and_stable(self):
        first, request = _request(GPT_LIVE_SIP_SPEC, {"model": "m"})
        assert request["control"]["token"] == live_control_token(first)
        second, other = _request(GPT_LIVE_SIP_SPEC, {"model": "m"})
        assert other["control"]["token"] != request["control"]["token"]
        assert live_control_token(first) == live_control_token(first)

    @pytest.mark.parametrize("spec", [GIT_SWAP_SPEC, ECHO_SERVICE_SPEC])
    def test_no_other_pod_gets_one(self, spec):
        _, request = _request(spec, {})
        assert "control" not in request


# =============================================================================
# The standing binding (§5.2)
# =============================================================================


class _Store:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.conn = SimpleNamespace(fetch=AsyncMock(return_value=rows))
        self.acquired = 0

    @contextlib.asynccontextmanager
    async def acquire(self):
        self.acquired += 1
        yield self.conn


def _reconciler(store) -> ServiceHostingReconciler:
    return ServiceHostingReconciler(
        store=store, runtime=MagicMock(), drivers=MagicMock(), settings=MagicMock()
    )


SPECS = {GPT_LIVE_SIP_SPEC.name: GPT_LIVE_SIP_SPEC}


@pytest.fixture
def images(monkeypatch):
    calls = SimpleNamespace(
        reference=MagicMock(return_value=IMAGE),
        resolve=AsyncMock(return_value=SimpleNamespace(digest=DIGEST)),
        moved=AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(hosting, "image_reference_for", calls.reference)
    monkeypatch.setattr(hosting, "resolve_driver_image", calls.resolve)
    monkeypatch.setattr(hosting, "check_moved_image", calls.moved)
    return calls


class TestStandingBindings:
    @pytest.mark.asyncio
    async def test_every_line_gets_an_ownerless_binding(self, images):
        lines = [str(uuid4()), str(uuid4())]
        store = _Store([{"id": line, "type": "gpt_live_sip"} for line in lines])
        bindings: dict = {}
        await _reconciler(store)._standing_bindings(bindings, SPECS)
        assert set(bindings) == {(line, DIGEST) for line in lines}
        for line in lines:
            binding = bindings[(line, DIGEST)]
            assert binding == _Binding(line, GPT_LIVE_SIP_SPEC.name, DIGEST)
            assert binding.owners == set()
        sql, types = store.conn.fetch.await_args.args
        assert "FROM datasources" in sql and types == ["gpt_live_sip"]
        images.reference.assert_called_with(GPT_LIVE_SIP_SPEC.name)
        assert images.resolve.await_args.kwargs == {
            "driver": GPT_LIVE_SIP_SPEC.name,
            "reference": IMAGE,
            "timeout": 10.0,
        }
        assert images.moved.await_args.kwargs["spec"] is GPT_LIVE_SIP_SPEC

    @pytest.mark.asyncio
    async def test_an_existing_binding_is_kept_with_its_owners(self, images):
        line = str(uuid4())
        store = _Store([{"id": line, "type": "gpt_live_sip"}])
        held = _Binding(line, GPT_LIVE_SIP_SPEC.name, DIGEST)
        held.bound_by("job", "j-1", datetime.now(timezone.utc))
        other = _Binding("c-2", "srw.echo/v1", DIGEST)
        bindings = {(line, DIGEST): held, ("c-2", DIGEST): other}
        await _reconciler(store)._standing_bindings(bindings, SPECS)
        assert bindings[(line, DIGEST)] is held
        assert held.owners == {("job", "j-1")}
        assert bindings[("c-2", DIGEST)] is other

    @pytest.mark.asyncio
    async def test_without_an_image_reference_nothing_starts(self, images):
        images.reference.return_value = None
        store = _Store([{"id": str(uuid4()), "type": "gpt_live_sip"}])
        bindings: dict = {}
        await _reconciler(store)._standing_bindings(bindings, SPECS)
        assert bindings == {}
        images.resolve.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "error",
        [ServiceImageUnavailable("unresolved"), ServiceImageRefused("moved")],
    )
    async def test_an_unresolvable_image_starts_nothing(self, images, error):
        images.resolve.side_effect = error
        store = _Store([{"id": str(uuid4()), "type": "gpt_live_sip"}])
        bindings: dict = {}
        await _reconciler(store)._standing_bindings(bindings, SPECS)
        assert bindings == {}

    @pytest.mark.asyncio
    async def test_a_moved_contract_starts_nothing(self, images):
        images.moved.return_value = ["the image's spec changed its config schema"]
        store = _Store([{"id": str(uuid4()), "type": "gpt_live_sip"}])
        bindings: dict = {}
        await _reconciler(store)._standing_bindings(bindings, SPECS)
        assert bindings == {}

    @pytest.mark.asyncio
    async def test_one_bad_line_does_not_stop_another(self, images):
        good, bad = str(uuid4()), str(uuid4())
        images.moved.side_effect = lambda conn, *, spec, connector_id, image: (
            ["moved"] if connector_id == bad else []
        )
        store = _Store(
            [{"id": bad, "type": "gpt_live_sip"}, {"id": good, "type": "gpt_live_sip"}]
        )
        bindings: dict = {}
        await _reconciler(store)._standing_bindings(bindings, SPECS)
        assert set(bindings) == {(good, DIGEST)}

    @pytest.mark.asyncio
    async def test_without_a_standing_driver_the_store_is_not_read(self, images):
        store = _Store([])
        bindings: dict = {}
        await _reconciler(store)._standing_bindings(
            bindings,
            {
                ECHO_SERVICE_SPEC.name: ECHO_SERVICE_SPEC,
                GIT_SWAP_SPEC.name: GIT_SWAP_SPEC,
            },
        )
        await _reconciler(store)._standing_bindings(bindings, {})
        assert bindings == {} and store.acquired == 0
