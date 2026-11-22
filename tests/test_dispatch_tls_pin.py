"""An endpoint's pinned certificate travels with its base URL (dispatch).

Vault spec ``endpoint-certificate-pinning-design``, "How the pin travels" and
rows D1-D4 of "Every TLS call site". The leaf certificate travels as
``TlsPin.wire`` (base64 DER on one line) and only the endpoint-row branches of
the dispatch injectors write it; every other path writes ``None`` (a model
section, so the agent's deep-merge clears a previous model's pin) or pops it
(a flat env).

* D1 ``inject_model_credentials``: ``section["tls_pinned_cert"]``.
* D2 ``inject_env_key_credentials``: ``{PREFIX}_TLS_PINNED_CERT``.
* D3 ``inject_thread_dispatch_credentials``: via D1 and D2, re-read from the
  endpoint row on every injection, so a re-trust reaches an open Session.
* D4 ``inject_system_kb_embedding_profile``: via D2 with ``KB_EMBEDDING``.
* D5 ``job_dispatch_credentials.inject_dispatch_credentials``: the job path.
  Its main ``llm`` branch writes the pin itself; nested slots via D1, env
  slots via D2; a slot whose ``{PREFIX}_MODEL`` is already set pops it.

The pin travels in flight only: ``redact_config_override`` strips it like a
key, so no stored override outlives a re-trust, and a caller can never set it
(``transport_key_paths``, the 422 at every caller-authored write boundary).

A pin that can't be read is never treated as no pin: the model slot is
refused (``model_availability.REASON_ENDPOINT_PIN``); a flat-env slot gets no
base URL or key from the row, like a stored key that did not decrypt, and its
pin is the ``UNREADABLE_PIN`` marker, so a base URL that survived in a stored
override never goes out without its pin (``TlsPin.from_env`` refuses it).
"""

# ruff: noqa: F811  (pytest fixtures are requested by parameter name)

from __future__ import annotations

import copy
import logging
import pickle
from unittest.mock import AsyncMock, patch

import pytest

from fastapi import HTTPException

from orchestrator.security.config_redaction import redact_config_override
from orchestrator.services import dispatch_credentials as dc
from orchestrator.services import job_dispatch_credentials as jdc
from orchestrator.services import model_availability as ma
from orchestrator.services import session_resolved_config
from orchestrator.services.config_overrides import refuse_caller_transport_keys
from shared.orch_surface.jobs._utils import transport_key_paths
from shared.runtime.core.model_registry import ModelMeta, UnknownModelError
from shared.runtime.core.transport_resolution import (
    NO_KEY_SENTINEL,
    env_endpoint_names,
)
from shared.tls_pinning import UNREADABLE_PIN, TlsPin
from tests.tls_pin_fixtures import tls_certs  # noqa: F401

LOGGER = logging.getLogger("test.dispatch-tls-pin")

PINNED_ID = "61111111-1111-1111-1111-111111111111"
KEYLESS_ID = "62222222-2222-2222-2222-222222222222"
BROKEN_KEY_ID = "63333333-3333-3333-3333-333333333333"
HTTP_ID = "64444444-4444-4444-4444-444444444444"
CORRUPT_ID = "65555555-5555-5555-5555-555555555555"
UNPINNED_ID = "66666666-6666-6666-6666-666666666666"

PINNED_URL = "https://vllm.university.internal/v1"
KEYLESS_URL = "https://tei.university.internal/v1"
BROKEN_URL = "https://broken.university.internal/v1"
HTTP_URL = "http://vllm.university.internal:8000/v1"
CORRUPT_URL = "https://corrupt.university.internal/v1"
UNPINNED_URL = "https://gateway.internal/v1"

PINNED_KEY = "sk-pinned-endpoint"
SYSTEM_OPENAI_KEY = "sk-system-openai"
STALE_PIN = "c3RhbGUtcGluLWZyb20tYW5vdGhlci1yb3c="
TRUNCATED_PEM = "-----BEGIN CERTIFICATE-----\nMIIBAAAA\n-----END CERTIFICATE-----\n"

#: Every flat-env slot that rides ``inject_env_key_credentials``.
FLAT_PREFIXES = [
    "EMBEDDING",
    "KB_EMBEDDING",
    "RERANK",
    "VISION",
    "WHISPER",
    "TTS",
    "CITATION_LLM",
]


def _pin_env(prefix: str) -> str:
    return f"{prefix}_TLS_PINNED_CERT"


def _endpoint_meta(model_id: str, endpoint_id: str) -> ModelMeta:
    return ModelMeta(
        model_id=model_id,
        provider="openai",
        family="gpt",
        display_name=model_id,
        origin="catalog",
        endpoint_id=endpoint_id,
        api_key_ref="openai",
    )


METAS = {
    "pinned-model": _endpoint_meta("pinned-model", PINNED_ID),
    "keyless-model": _endpoint_meta("keyless-model", KEYLESS_ID),
    "broken-key-model": _endpoint_meta("broken-key-model", BROKEN_KEY_ID),
    "http-model": _endpoint_meta("http-model", HTTP_ID),
    "corrupt-pin-model": _endpoint_meta("corrupt-pin-model", CORRUPT_ID),
    "unpinned-model": _endpoint_meta("unpinned-model", UNPINNED_ID),
    # A provider-key catalog row: no endpoint of its own.
    "provider-model": ModelMeta(
        model_id="provider-model",
        provider="openai",
        family="gpt",
        display_name="provider-model",
        origin="catalog",
        api_key_ref="openai",
    ),
}


class _Store:
    def __init__(self, rows: dict[str, dict], defaults: dict[str, str] | None = None):
        self.rows = rows
        self.defaults = defaults or {}
        self.endpoint_reads: list[str] = []

    async def resolve_api_keys_for_job(self, *, user_id=None, project_id=None):
        return {"openai": SYSTEM_OPENAI_KEY}

    async def get_user_settings(self, user_id):
        return {}

    async def get_user_llm_endpoint(self, endpoint_id):
        self.endpoint_reads.append(endpoint_id)
        row = self.rows.get(endpoint_id)
        return dict(row) if row else None

    async def resolve_default_for_capability(self, capability):
        return self.defaults.get(capability)


async def _resolve(model_id, user_id=None, capability="chat"):
    try:
        return METAS[model_id]
    except KeyError:
        raise UnknownModelError(model_id) from None


def _pin_columns(pin: TlsPin | None) -> dict:
    if pin is None:
        return {"tls_pin_sha256": None, "tls_pin_cert": None}
    return {"tls_pin_sha256": pin.sha256, "tls_pin_cert": pin.pem}


def _row(endpoint_id, label, base_url, api_key, pin, *, stored=None) -> dict:
    return {
        "id": endpoint_id,
        "label": label,
        "base_url": base_url,
        "api_key": api_key,
        "api_key_stored": api_key is not None if stored is None else stored,
        **_pin_columns(pin),
    }


@pytest.fixture
def pins(tls_certs):
    return {
        "leaf": TlsPin.from_pem(tls_certs.leaf.read_text()),
        "swap": TlsPin.from_pem(tls_certs.swap.read_text()),
    }


@pytest.fixture
def rows(pins):
    leaf = pins["leaf"]
    return {
        PINNED_ID: _row(PINNED_ID, "university-vllm", PINNED_URL, PINNED_KEY, leaf),
        # Registered without a key: the column is NULL.
        KEYLESS_ID: _row(KEYLESS_ID, "university-tei", KEYLESS_URL, None, leaf),
        # A key is stored but did not decrypt.
        BROKEN_KEY_ID: _row(
            BROKEN_KEY_ID, "broken", BROKEN_URL, None, leaf, stored=True
        ),
        # Pinned while on https://, then edited to http:// (decision 4 keeps it).
        HTTP_ID: _row(HTTP_ID, "plain-http", HTTP_URL, PINNED_KEY, leaf),
        CORRUPT_ID: {
            **_row(CORRUPT_ID, "corrupt-pin", CORRUPT_URL, PINNED_KEY, None),
            "tls_pin_sha256": leaf.sha256,
            "tls_pin_cert": "-----BEGIN CERTIFICATE-----\nbm90IGEgY2VydA==\n"
            "-----END CERTIFICATE-----\n",
        },
        UNPINNED_ID: _row(UNPINNED_ID, "gateway", UNPINNED_URL, PINNED_KEY, None),
    }


@pytest.fixture
def store(rows):
    return _Store(rows)


def _deps(store: _Store) -> dc.DispatchCredentialDependencies:
    return dc.DispatchCredentialDependencies(
        store=store, logger=LOGGER, resolve_model=AsyncMock(side_effect=_resolve)
    )


@pytest.fixture(autouse=True)
def _no_search_or_image_credentials():
    with patch(
        "orchestrator.services.capability_credentials.resolve_capability_credentials",
        AsyncMock(return_value=None),
    ):
        yield


def _errors(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]


async def _section(section: dict, store: _Store, capability: str = "chat"):
    return await dc.inject_model_credentials(
        section=section,
        model_id=section["model"],
        user_id="u",
        resolved_keys={"openai": SYSTEM_OPENAI_KEY},
        capability=capability,
        dependencies=_deps(store),
    )


async def _env(env: dict, prefix: str, model_id: str, store: _Store) -> dict:
    await dc.inject_env_key_credentials(
        env_keys=env,
        prefix=prefix,
        model_id=model_id,
        user_id="u",
        resolved_keys={"openai": SYSTEM_OPENAI_KEY},
        capability="embedding",
        dependencies=_deps(store),
    )
    return env


# ---------------------------------------------------------------------------
# D1: model sections
# ---------------------------------------------------------------------------


class TestModelSection:
    @pytest.mark.asyncio
    async def test_endpoint_model_gets_base_url_key_and_pin(self, store, pins):
        section = {"model": "pinned-model"}
        assert await _section(section, store) is None
        assert section["base_url"] == PINNED_URL
        assert section["api_key"] == PINNED_KEY
        assert section["tls_pinned_cert"] == pins["leaf"].wire
        assert store.endpoint_reads == [PINNED_ID]

    @pytest.mark.asyncio
    async def test_keyless_endpoint_model_gets_its_pin(self, store, pins):
        section = {"model": "keyless-model"}
        assert await _section(section, store) is None
        assert section["base_url"] == KEYLESS_URL
        assert section["tls_pinned_cert"] == pins["leaf"].wire

    @pytest.mark.asyncio
    async def test_endpoint_row_replaces_a_stale_pin(self, store, pins):
        section = {
            "model": "pinned-model",
            "base_url": PINNED_URL,
            "api_key": "sk-stale",
            "tls_pinned_cert": STALE_PIN,
        }
        assert await _section(section, store) is None
        assert section["tls_pinned_cert"] == pins["leaf"].wire

    @pytest.mark.asyncio
    async def test_endpoint_without_a_pin_writes_none(self, store):
        section = {"model": "unpinned-model", "tls_pinned_cert": STALE_PIN}
        assert await _section(section, store) is None
        assert section["base_url"] == UNPINNED_URL
        assert "tls_pinned_cert" in section
        assert section["tls_pinned_cert"] is None

    @pytest.mark.asyncio
    async def test_provider_key_model_writes_none(self, store):
        section = {"model": "provider-model"}
        assert await _section(section, store) is None
        assert section["api_key"] == SYSTEM_OPENAI_KEY
        # Present and None: the agent's deep-merge reads None as "clear".
        assert "tls_pinned_cert" in section
        assert section["tls_pinned_cert"] is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("model_id", ["provider-model", "self-hosted-unknown"])
    async def test_caller_complete_transport_without_endpoint_gets_none(
        self, store, model_id
    ):
        section = {
            "model": model_id,
            "base_url": "https://mine.example/v1",
            "api_key": "sk-mine",
            "tls_pinned_cert": STALE_PIN,
        }
        assert await _section(section, store) is None
        assert section["base_url"] == "https://mine.example/v1"
        assert section["api_key"] == "sk-mine"
        assert "tls_pinned_cert" in section
        assert section["tls_pinned_cert"] is None
        assert store.endpoint_reads == []

    @pytest.mark.asyncio
    async def test_hot_swap_to_a_provider_key_model_keeps_no_pin(self, store, pins):
        """Review Focus 1: a section that held a pin, re-injected for a
        provider-key model, ends with ``tls_pinned_cert`` None."""
        section = {"model": "pinned-model"}
        assert await _section(section, store) is None
        assert section["tls_pinned_cert"] == pins["leaf"].wire

        section["model"] = "provider-model"
        assert await _section(section, store) is None
        assert "tls_pinned_cert" in section
        assert section["tls_pinned_cert"] is None

        # The swap override the session sends is a fresh section too.
        swap = {"model": "provider-model"}
        assert await _section(swap, store) is None
        assert "tls_pinned_cert" in swap
        assert swap["tls_pinned_cert"] is None

    @pytest.mark.asyncio
    async def test_refused_undecryptable_row_carries_no_pin(self, store, caplog):
        caplog.set_level(logging.DEBUG)
        section = {"model": "broken-key-model", "tls_pinned_cert": STALE_PIN}
        reason = await _section(section, store)
        assert reason == ma.REASON_ENDPOINT_KEY
        assert reason.endpoint == "broken"
        assert "base_url" not in section
        assert "api_key" not in section
        assert section["tls_pinned_cert"] is None

    @pytest.mark.asyncio
    async def test_http_base_url_never_carries_a_pin(self, store):
        section = {"model": "http-model", "tls_pinned_cert": STALE_PIN}
        assert await _section(section, store) is None
        assert section["base_url"] == HTTP_URL
        assert section["tls_pinned_cert"] is None


class TestUnreadablePinIsRefused:
    """A stored pin that can't be read fails closed, never as "no pin"."""

    @pytest.fixture(
        params=[
            "not-a-certificate",
            "fingerprint-without-certificate",
            "mismatch",
            "not-a-trust-anchor",
        ]
    )
    def corrupt(self, request, rows, pins):
        row = rows[CORRUPT_ID]
        if request.param == "fingerprint-without-certificate":
            row["tls_pin_cert"] = None
        elif request.param == "mismatch":
            row["tls_pin_cert"] = pins["swap"].pem
        elif request.param == "not-a-trust-anchor":
            # Valid PEM armour around a truncated DER: it parses, but OpenSSL
            # can't load it as a trust anchor (the Test probe's third check).
            row["tls_pin_cert"] = TRUNCATED_PEM
            row["tls_pin_sha256"] = TlsPin.from_pem(TRUNCATED_PEM).sha256
        return request.param

    @pytest.mark.asyncio
    async def test_model_section_is_refused_naming_the_endpoint(
        self, store, corrupt, caplog
    ):
        caplog.set_level(logging.DEBUG)
        section = {"model": "corrupt-pin-model", "tls_pinned_cert": STALE_PIN}
        reason = await _section(section, store)
        assert reason == ma.REASON_ENDPOINT_PIN
        assert reason.endpoint == "corrupt-pin"
        # No transport at all: neither the base URL without its pin nor the key.
        assert "base_url" not in section
        assert "api_key" not in section
        assert section["tls_pinned_cert"] is None
        errors = _errors(caplog)
        assert len(errors) == 1
        assert "corrupt-pin" in errors[0]
        assert CORRUPT_ID in errors[0]
        assert PINNED_KEY not in errors[0]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("prefix", FLAT_PREFIXES)
    async def test_flat_env_slot_gets_no_transport(
        self, store, corrupt, prefix, caplog
    ):
        caplog.set_level(logging.DEBUG)
        env = await _env(
            {_pin_env(prefix): STALE_PIN}, prefix, "corrupt-pin-model", store
        )
        assert env == {
            f"{prefix}_MODEL": "corrupt-pin-model",
            _pin_env(prefix): UNREADABLE_PIN,
        }
        errors = _errors(caplog)
        assert len(errors) == 1
        assert CORRUPT_ID in errors[0]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("prefix", FLAT_PREFIXES)
    async def test_a_stored_base_url_never_goes_out_without_its_pin(
        self, store, corrupt, prefix
    ):
        # The redacted stored-override shape a Session re-injects: the base
        # URL survives redaction, the key does not.
        url_name = env_endpoint_names(prefix)[0]
        env = await _env(
            {f"{prefix}_MODEL": "corrupt-pin-model", url_name: CORRUPT_URL},
            prefix,
            "corrupt-pin-model",
            store,
        )
        assert env[url_name] == CORRUPT_URL
        assert f"{prefix}_API_KEY" not in env
        assert env[_pin_env(prefix)] == UNREADABLE_PIN
        with pytest.raises(ValueError, match="can't be read"):
            TlsPin.from_env(prefix, env)

    def test_the_refusal_message_names_the_endpoint(self):
        entry = ma.UnavailableModel(
            slot="llm",
            model="corrupt-pin-model",
            reason=ma.EndpointPinUnreadable("corrupt-pin"),
        )
        assert ma.ModelUnavailable([entry]).message() == (
            "The certificate pinned for the model endpoint 'corrupt-pin' can't "
            "be read. Trust its certificate again or remove the pin in "
            "Admin → Models."
        )
        assert entry.as_dict() == {
            "slot": "llm",
            "model": "corrupt-pin-model",
            "reason": "endpoint_pin",
            "endpoint": "corrupt-pin",
        }

    def test_the_reason_survives_a_copy(self):
        reason = ma.EndpointPinUnreadable("corrupt-pin")
        for clone in (copy.deepcopy(reason), pickle.loads(pickle.dumps(reason))):
            assert clone == ma.REASON_ENDPOINT_PIN
            assert clone.endpoint == "corrupt-pin"


# ---------------------------------------------------------------------------
# D2: flat envs
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("prefix", FLAT_PREFIXES)
class TestFlatEnv:
    @pytest.mark.asyncio
    async def test_keyed_endpoint_gets_base_url_key_and_pin(self, store, pins, prefix):
        env = await _env({}, prefix, "pinned-model", store)
        assert env[env_endpoint_names(prefix)[0]] == PINNED_URL
        assert env[f"{prefix}_API_KEY"] == PINNED_KEY
        assert env[_pin_env(prefix)] == pins["leaf"].wire

    @pytest.mark.asyncio
    async def test_keyless_endpoint_gets_sentinel_and_pin(self, store, pins, prefix):
        env = await _env({}, prefix, "keyless-model", store)
        assert env[env_endpoint_names(prefix)[0]] == KEYLESS_URL
        assert env[f"{prefix}_API_KEY"] == NO_KEY_SENTINEL
        assert env[_pin_env(prefix)] == pins["leaf"].wire

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "pin_state", ["pinned", "unpinned", "unreadable", "http", "fresh-block"]
    )
    async def test_decrypt_failure_keeps_a_pinned_base_url_pinned(
        self, store, rows, pins, prefix, pin_state
    ):
        """A key that won't decrypt gets no base URL or key from the row; a
        base URL that survived in the stored override keeps the row's pin."""
        row = rows[BROKEN_KEY_ID]
        expected = pins["leaf"].wire
        if pin_state == "unpinned":
            row.update(_pin_columns(None))
            expected = None
        elif pin_state == "unreadable":
            row["tls_pin_cert"] = "not a certificate"
            expected = UNREADABLE_PIN
        elif pin_state == "http":
            row["base_url"] = HTTP_URL  # R9: a pin never rides http://
            expected = None
        url_name = env_endpoint_names(prefix)[0]
        # The redacted stored-override shape: base URL kept, key stripped.
        stored = {f"{prefix}_MODEL": "broken-key-model", url_name: row["base_url"]}
        if pin_state == "fresh-block":
            stored = {_pin_env(prefix): STALE_PIN}
        env = await _env(stored, prefix, "broken-key-model", store)
        assert f"{prefix}_API_KEY" not in env
        assert env.get(_pin_env(prefix)) == expected
        if pin_state == "fresh-block":
            # Nothing written from the row but the pin.
            assert env == {
                f"{prefix}_MODEL": "broken-key-model",
                _pin_env(prefix): expected,
            }
        else:
            assert env[url_name] == row["base_url"]

    @pytest.mark.asyncio
    async def test_byo_pair_gets_no_pin(self, store, prefix):
        url_name = env_endpoint_names(prefix)[0]
        env = await _env(
            {
                url_name: "https://byo.example/v1",
                f"{prefix}_API_KEY": "sk-byo",
                _pin_env(prefix): STALE_PIN,
            },
            prefix,
            "pinned-model",
            store,
        )
        assert env[url_name] == "https://byo.example/v1"
        assert env[f"{prefix}_API_KEY"] == "sk-byo"
        assert _pin_env(prefix) not in env

    @pytest.mark.asyncio
    async def test_provider_key_row_pops_the_pin(self, store, prefix):
        env = await _env({_pin_env(prefix): STALE_PIN}, prefix, "provider-model", store)
        assert env[f"{prefix}_API_KEY"] == SYSTEM_OPENAI_KEY
        assert _pin_env(prefix) not in env

    @pytest.mark.asyncio
    async def test_endpoint_without_a_pin_pops_a_stale_one(self, store, prefix):
        env = await _env({_pin_env(prefix): STALE_PIN}, prefix, "unpinned-model", store)
        assert env[env_endpoint_names(prefix)[0]] == UNPINNED_URL
        assert _pin_env(prefix) not in env

    @pytest.mark.asyncio
    async def test_http_base_url_never_carries_a_pin(self, store, prefix):
        env = await _env({_pin_env(prefix): STALE_PIN}, prefix, "http-model", store)
        assert env[env_endpoint_names(prefix)[0]] == HTTP_URL
        assert _pin_env(prefix) not in env


# ---------------------------------------------------------------------------
# D3 / D4: the Session path and the KB profile
# ---------------------------------------------------------------------------


class TestSessionReinjection:
    """Review Focus 4: re-injection reads the endpoint row, never the stored
    override, so a re-trust reaches an open Session on its next turn."""

    def _store(self, rows) -> _Store:
        return _Store(
            rows,
            defaults={
                "embedding": "pinned-model",
                "rerank": "pinned-model",
                "whisper": "pinned-model",
            },
        )

    async def _inject(self, co: dict, store: _Store) -> dict:
        return await dc.inject_thread_dispatch_credentials(
            co, user_id="u", include_kb_profile=True, dependencies=_deps(store)
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("change", ["retrusted", "removed"])
    async def test_stored_override_carries_the_rows_current_pin(
        self, rows, pins, change
    ):
        store = self._store(rows)
        co = {
            "llm": {"model": "pinned-model"},
            "auxiliary": {"model": "pinned-model"},
            "subagents": {"llm": {"model": "pinned-model"}},
        }
        first = await self._inject(co, store)
        old = pins["leaf"].wire
        assert first["llm"]["tls_pinned_cert"] == old
        assert first["auxiliary"]["tls_pinned_cert"] == old
        assert first["subagents"]["llm"]["tls_pinned_cert"] == old
        for prefix in ("EMBEDDING", "RERANK", "WHISPER", "KB_EMBEDDING"):
            assert first["env_keys"][_pin_env(prefix)] == old, prefix

        stored = redact_config_override(first)
        new_pin = pins["swap"] if change == "retrusted" else None
        rows[PINNED_ID].update(_pin_columns(new_pin))
        expected = new_pin.wire if new_pin is not None else None

        again = await self._inject(stored, store)
        assert again["llm"]["tls_pinned_cert"] == expected
        assert again["auxiliary"]["tls_pinned_cert"] == expected
        assert again["subagents"]["llm"]["tls_pinned_cert"] == expected
        for prefix in ("EMBEDDING", "RERANK", "WHISPER", "KB_EMBEDDING"):
            assert again["env_keys"].get(_pin_env(prefix)) == expected, prefix
            assert again["env_keys"][f"{prefix}_BASE_URL"] == PINNED_URL

    @pytest.mark.asyncio
    async def test_a_pin_that_turns_unreadable_reaches_the_session_as_the_marker(
        self, rows
    ):
        store = self._store(rows)
        co = {"llm": {"model": "provider-model"}}
        stored = redact_config_override(await self._inject(co, store))
        assert stored["env_keys"]["EMBEDDING_BASE_URL"] == PINNED_URL

        rows[PINNED_ID]["tls_pin_cert"] = "not a certificate"
        again = await self._inject(stored, store)
        env = again["env_keys"]
        for prefix in ("EMBEDDING", "RERANK", "WHISPER", "KB_EMBEDDING"):
            assert env[_pin_env(prefix)] == UNREADABLE_PIN, prefix
            assert f"{prefix}_API_KEY" not in env, prefix
            with pytest.raises(ValueError, match="can't be read"):
                TlsPin.from_env(prefix, env)
        assert env["EMBEDDING_BASE_URL"] == PINNED_URL

    @pytest.mark.asyncio
    async def test_kb_profile_on_a_provider_key_default_drops_a_stale_pin(self, rows):
        store = _Store(rows, defaults={"embedding": "provider-model"})
        env = {_pin_env("KB_EMBEDDING"): STALE_PIN}
        await dc.inject_system_kb_embedding_profile(env, dependencies=_deps(store))
        assert env["KB_EMBEDDING_API_KEY"] == SYSTEM_OPENAI_KEY
        assert _pin_env("KB_EMBEDDING") not in env

    @pytest.mark.asyncio
    async def test_kb_profile_on_a_pinned_default_carries_the_pin(self, rows, pins):
        store = _Store(rows, defaults={"embedding": "pinned-model"})
        env: dict = {}
        await dc.inject_system_kb_embedding_profile(env, dependencies=_deps(store))
        assert env["KB_EMBEDDING_BASE_URL"] == PINNED_URL
        assert env[_pin_env("KB_EMBEDDING")] == pins["leaf"].wire


# ---------------------------------------------------------------------------
# D5: the job path
# ---------------------------------------------------------------------------

JOB = {
    "id": "00000000-0000-0000-0000-000000000010",
    "user_id": "00000000-0000-0000-0000-0000000000aa",
    "project_id": None,
    "config_name": "worker_base",
}

#: The flat slots the job's env-slot loop credentials, by capability default.
JOB_LOOP_SLOTS = [
    ("vision", "VISION"),
    ("whisper", "WHISPER"),
    ("tts", "TTS"),
    ("rerank", "RERANK"),
    ("citation", "CITATION_LLM"),
]


def _job_deps(store: _Store) -> jdc.DispatchCredentialDependencies:
    service = _deps(store)

    async def model_creds(**kwargs):
        return await dc.inject_model_credentials(**kwargs, dependencies=service)

    async def env_creds(**kwargs):
        return await dc.inject_env_key_credentials(**kwargs, dependencies=service)

    async def kb_profile(env_keys):
        return await dc.inject_system_kb_embedding_profile(
            env_keys, dependencies=service
        )

    return jdc.DispatchCredentialDependencies(
        store=store,
        logger=LOGGER,
        resolve_model=service.resolve_model,
        inject_model_credentials=model_creds,
        inject_env_key_credentials=env_creds,
        inject_search_credentials=AsyncMock(),
        inject_system_kb_embedding_profile=kb_profile,
        dispatch_llm_provider_fallback=dc.dispatch_llm_provider_fallback,
        nested_model_slots=dc.nested_model_slots,
    )


def _defaults(model_id: str) -> dict[str, str]:
    """Every capability default on one model."""
    kinds = ["auxiliary", "embedding", *(kind for kind, _ in JOB_LOOP_SLOTS)]
    return {kind: model_id for kind in kinds}


async def _job(co: dict, store: _Store, **kwargs) -> dict:
    return await jdc.inject_dispatch_credentials(
        JOB, co, include_kb_profile=True, dependencies=_job_deps(store), **kwargs
    )


class TestJobPath:
    @pytest.mark.asyncio
    async def test_a_job_on_a_pinned_endpoint_carries_the_pin_in_every_slot(
        self, rows, pins
    ):
        store = _Store(rows, defaults=_defaults("pinned-model"))
        out = await _job(
            {
                "llm": {"model": "pinned-model"},
                "subagents": {"llm": {"model": "pinned-model"}},
            },
            store,
        )
        wire = pins["leaf"].wire
        assert out["llm"]["base_url"] == PINNED_URL
        assert out["llm"]["api_key"] == PINNED_KEY
        assert out["llm"]["tls_pinned_cert"] == wire
        assert out["auxiliary"]["tls_pinned_cert"] == wire
        assert out["subagents"]["llm"]["tls_pinned_cert"] == wire
        env = out["env_keys"]
        prefixes = [prefix for _, prefix in JOB_LOOP_SLOTS]
        for prefix in [*prefixes, "EMBEDDING", "KB_EMBEDDING"]:
            assert env[env_endpoint_names(prefix)[0]] == PINNED_URL, prefix
            assert env[_pin_env(prefix)] == wire, prefix

    @pytest.mark.asyncio
    @pytest.mark.parametrize("model_id", ["provider-model", "unpinned-model"])
    async def test_a_job_off_a_pinned_endpoint_carries_no_pin(self, rows, model_id):
        store = _Store(rows, defaults=_defaults(model_id))
        co = {
            "llm": {"model": model_id, "tls_pinned_cert": STALE_PIN},
            "env_keys": {_pin_env("VISION"): STALE_PIN},
        }
        out = await _job(co, store)
        # Present and None, like `served_model`: never a caller's, never kept.
        assert "tls_pinned_cert" in out["llm"]
        assert out["llm"]["tls_pinned_cert"] is None
        assert out["auxiliary"]["tls_pinned_cert"] is None
        assert not [key for key in out["env_keys"] if key.endswith("_TLS_PINNED_CERT")]

    @pytest.mark.asyncio
    async def test_the_main_slot_never_carries_a_pin_to_http(self, rows):
        """R9: an endpoint edited to http:// keeps its pin; dispatch never
        attaches it there."""
        store = _Store(rows)
        out = await _job({"llm": {"model": "http-model"}}, store)
        assert out["llm"]["base_url"] == HTTP_URL
        assert out["llm"]["tls_pinned_cert"] is None

    @pytest.mark.asyncio
    async def test_a_job_with_no_model_of_its_own_gets_the_defaults_pin(
        self, rows, pins
    ):
        store = _Store(rows, defaults={"chat": "pinned-model"})
        out = await _job({}, store)
        assert out["llm"]["model"] == "pinned-model"
        assert out["llm"]["tls_pinned_cert"] == pins["leaf"].wire

    @pytest.mark.asyncio
    async def test_redispatch_reads_the_rows_current_pin(self, rows, pins):
        """A re-trust reaches the job's next dispatch (a resume): the endpoint
        row is read again, never a stored override."""
        store = _Store(rows)
        first = await _job({"llm": {"model": "pinned-model"}}, store)
        assert first["llm"]["tls_pinned_cert"] == pins["leaf"].wire

        rows[PINNED_ID].update(_pin_columns(pins["swap"]))
        again = await _job(redact_config_override(first), store)
        assert again["llm"]["tls_pinned_cert"] == pins["swap"].wire

        rows[PINNED_ID].update(_pin_columns(None))
        removed = await _job(redact_config_override(again), store)
        assert "tls_pinned_cert" in removed["llm"]
        assert removed["llm"]["tls_pinned_cert"] is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("kind, prefix", JOB_LOOP_SLOTS)
    async def test_a_project_pinned_slot_model_keeps_no_stale_pin(
        self, rows, kind, prefix
    ):
        """The loop's `continue` for an already-set ``{PREFIX}_MODEL`` (a
        project or expert override) pops the pin: no injector ran for it."""
        store = _Store(rows, defaults={kind: "pinned-model"})
        co = {
            "llm": {"model": "provider-model"},
            "env_keys": {
                f"{prefix}_MODEL": "project-model",
                _pin_env(prefix): STALE_PIN,
            },
        }
        out = await _job(co, store)
        env = out["env_keys"]
        assert env[f"{prefix}_MODEL"] == "project-model"
        assert _pin_env(prefix) not in env

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "preset", [None, "CITATION_LLM_BASE_URL", "CITATION_LLM_URL"]
    )
    async def test_the_citation_slot_carries_its_pin_under_either_url_name(
        self, rows, pins, preset
    ):
        """The citation package reads ``CITATION_LLM_URL``, the agent's
        transport ``CITATION_LLM_BASE_URL``: both name the pinned endpoint and
        the pin rides ``CITATION_LLM_TLS_PINNED_CERT`` for either."""
        store = _Store(rows, defaults={"citation": "pinned-model"})
        env: dict = {_pin_env("CITATION_LLM"): STALE_PIN}
        if preset:
            env[preset] = "https://stale.example/v1"
        out = await _job({"llm": {"model": "provider-model"}, "env_keys": env}, store)
        env = out["env_keys"]
        assert env["CITATION_LLM_BASE_URL"] == PINNED_URL
        assert env["CITATION_LLM_URL"] == PINNED_URL
        assert env[_pin_env("CITATION_LLM")] == pins["leaf"].wire

    @pytest.mark.asyncio
    async def test_an_unreadable_pin_refuses_the_jobs_own_model(
        self, rows, pins, caplog
    ):
        """R10: never dispatched without its pin; refused like D1, naming the
        endpoint."""
        caplog.set_level(logging.DEBUG)
        rows[CORRUPT_ID]["tls_pin_cert"] = pins["swap"].pem  # fingerprint mismatch
        store = _Store(rows)
        with pytest.raises(ma.ModelUnavailable) as raised:
            await _job({"llm": {"model": "corrupt-pin-model"}}, store)
        entries = raised.value.entries
        assert [(e.slot, e.reason) for e in entries] == [("llm", "endpoint_pin")]
        assert entries[0].reason.endpoint == "corrupt-pin"
        assert raised.value.message(where=ma.WHERE_JOB) == (
            "The certificate pinned for the model endpoint 'corrupt-pin' can't "
            "be read. Trust its certificate again or remove the pin in "
            "Admin → Models."
        )
        errors = _errors(caplog)
        assert any("corrupt-pin" in m and JOB["id"] in m for m in errors)
        assert not any(PINNED_KEY in m for m in errors)

    @pytest.mark.asyncio
    async def test_a_flat_copy_with_an_unreadable_pin_gets_no_transport(self, rows):
        store = _Store(rows)
        out = await _job(
            {"llm": {"model": "corrupt-pin-model", "tls_pinned_cert": STALE_PIN}},
            store,
            strict=False,
        )
        assert "base_url" not in out["llm"]
        assert "api_key" not in out["llm"]
        assert out["llm"]["tls_pinned_cert"] is None

    @pytest.mark.asyncio
    async def test_an_undecryptable_key_refuses_with_no_pin(self, rows):
        store = _Store(rows)
        out = await _job(
            {"llm": {"model": "broken-key-model", "tls_pinned_cert": STALE_PIN}},
            store,
            strict=False,
        )
        assert "base_url" not in out["llm"]
        assert out["llm"]["tls_pinned_cert"] is None


# ---------------------------------------------------------------------------
# In flight only: redaction
# ---------------------------------------------------------------------------


class TestRedaction:
    """The pin is public, but a stored one would outlive a re-trust: it is
    stripped wherever it appears, like a key, and re-injected from the row."""

    def test_every_pin_is_stripped_and_every_base_url_kept(self):
        co = {
            "llm": {
                "model": "m",
                "base_url": PINNED_URL,
                "tls_pinned_cert": STALE_PIN,
                "summarization": {"model": "s", "tls_pinned_cert": STALE_PIN},
            },
            "auxiliary": {"model": "a", "tls_pinned_cert": STALE_PIN},
            "image": {"model": "i", "tls_pinned_cert": STALE_PIN},
            "subagents": {
                "llm": {"model": "r", "tls_pinned_cert": STALE_PIN},
                "roster": [{"name": "x", "llm": {"tls_pinned_cert": STALE_PIN}}],
            },
            "env_keys": {
                "EMBEDDING_BASE_URL": PINNED_URL,
                "EMBEDDING_TLS_PINNED_CERT": STALE_PIN,
                "VISION_TLS_PINNED_CERT": UNREADABLE_PIN,
                "Citation_Llm_Tls_Pinned_Cert": STALE_PIN,
            },
        }
        out = redact_config_override(co)
        assert out == {
            "llm": {
                "model": "m",
                "base_url": PINNED_URL,
                "summarization": {"model": "s"},
            },
            "auxiliary": {"model": "a"},
            "image": {"model": "i"},
            "subagents": {"llm": {"model": "r"}, "roster": [{"name": "x", "llm": {}}]},
            "env_keys": {"EMBEDDING_BASE_URL": PINNED_URL},
        }
        assert co["llm"]["tls_pinned_cert"] == STALE_PIN  # not mutated

    def test_case_insensitive(self):
        out = redact_config_override({"llm": {"TLS_PINNED_CERT": "x", "model": "m"}})
        assert out == {"llm": {"model": "m"}}

    def test_unrelated_keys_that_mention_tls_are_kept(self):
        co = {"llm": {"tls_pinned": "x", "pinned_cert_note": "y", "tls": {"a": 1}}}
        assert redact_config_override(co) == co

    @pytest.mark.asyncio
    async def test_a_dispatched_job_stores_no_pin(self, rows, pins):
        store = _Store(rows, defaults=_defaults("pinned-model"))
        out = await _job({"llm": {"model": "pinned-model"}}, store)
        stored = redact_config_override(out)
        assert pins["leaf"].wire not in repr(stored)
        assert "tls_pinned_cert" not in repr(stored).lower()
        assert stored["llm"]["base_url"] == PINNED_URL
        assert stored["env_keys"]["EMBEDDING_BASE_URL"] == PINNED_URL


# ---------------------------------------------------------------------------
# Never a caller's: the write boundary and the resolved-config read
# ---------------------------------------------------------------------------


class TestCallerBoundary:
    @pytest.mark.parametrize(
        "override, paths",
        [
            ({"llm": {"model": "m", "tls_pinned_cert": "x"}}, ["llm.tls_pinned_cert"]),
            ({"auxiliary": {"TLS_PINNED_CERT": "x"}}, ["auxiliary.TLS_PINNED_CERT"]),
            ({"image": {"tls_pinned_cert": "x"}}, ["image.tls_pinned_cert"]),
            (
                {"llm": {"summarization": {"tls_pinned_cert": "x"}}},
                ["llm.summarization.tls_pinned_cert"],
            ),
            (
                {"subagents": {"roster": [{"llm": {"tls_pinned_cert": "x"}}]}},
                ["subagents.roster[0].llm.tls_pinned_cert"],
            ),
            (
                {"memory": {"RERANK_TLS_PINNED_CERT": "x"}},
                ["memory.RERANK_TLS_PINNED_CERT"],
            ),
            # env_keys is refused whole, the pin included.
            ({"env_keys": {"EMBEDDING_TLS_PINNED_CERT": "x"}}, ["env_keys"]),
        ],
    )
    def test_a_caller_pin_is_a_transport_key(self, override, paths):
        assert transport_key_paths(override) == paths

    def test_a_caller_pin_is_refused_with_422_naming_the_path(self):
        with pytest.raises(HTTPException) as raised:
            refuse_caller_transport_keys(
                {"llm": {"model": "m", "tls_pinned_cert": STALE_PIN}}
            )
        assert raised.value.status_code == 422
        assert "llm.tls_pinned_cert" in raised.value.detail

    def test_the_resolved_config_read_hides_the_pin_even_unredacted(self):
        """``_TRANSPORT_KEYS``: where a request goes, never shown, on its own
        and not only through the redaction it runs after."""
        blob = {
            "agent": {
                "llm": {"model": "m", "tls_pinned_cert": STALE_PIN},
                "image": {"model": "i", "tls_pinned_cert": STALE_PIN},
                "memory": {"RERANK_TLS_PINNED_CERT": STALE_PIN, "enabled": True},
            }
        }
        assert session_resolved_config._without_transport(blob) == {
            "agent": {
                "llm": {"model": "m"},
                "image": {"model": "i"},
                "memory": {"enabled": True},
            }
        }
