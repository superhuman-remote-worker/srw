"""The git swap driver's contract (connector drivers C3): its spec, the
repository URLs it serves, and how a payload entry names it."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from shared.connectors.builtin import (
    GIT_SWAP_SPEC,
    REPOSITORY_SPEC,
    driver_spec_for_row,
    git_swap_entry,
)
from shared.connectors.contract import (
    CredentialSlot,
    ServiceSpec,
    effective_access,
    validate_spec,
)
from shared.connectors.git_swap import (
    UnservedUpstream,
    connector_path_id,
    driver_repository_url,
    swap_upstream,
)

_VECTORS = (
    Path(__file__).resolve().parents[1]
    / "drivers"
    / "git-swap"
    / "testdata"
    / "upstream_vectors.json"
)
_CONNECTOR = "0d6f3a52-8b1c-4e8e-9a8f-1f2e3d4c5b6a"


def _vectors() -> list[dict]:
    return json.loads(_VECTORS.read_text())["cases"]


class TestUpstreams:
    @pytest.mark.parametrize("case", _vectors(), ids=lambda case: case["input"])
    def test_the_shared_vectors(self, case):
        """The driver (Go) is tested against the same file."""
        if not case["ok"]:
            with pytest.raises(UnservedUpstream):
                swap_upstream(case["input"])
            return
        upstream = swap_upstream(case["input"])
        assert (upstream.url, upstream.host, upstream.path) == (
            case["url"],
            case["host"],
            case["path"],
        )

    def test_the_refusals_say_why(self):
        reasons = {
            "http://github.com/o/r": "not HTTPS",
            "https://u:t@github.com/o/r": "carries credentials",
            "https://github.com:8443/o/r": "port 443",
            "https://github.com/o/r?x": "query",
            "https://github.com/": "no repository path",
        }
        for url, reason in reasons.items():
            with pytest.raises(UnservedUpstream, match=reason):
                swap_upstream(url)

    def test_the_insteadof_base_drops_a_dot_git(self):
        assert (
            swap_upstream("https://h.example/o/r.git").base == "https://h.example/o/r"
        )
        assert swap_upstream("https://h.example/o/r").base == "https://h.example/o/r"

    def test_the_driver_url_carries_the_connector_and_the_repository(self):
        upstream = swap_upstream("https://github.com/o/r.git")
        url = driver_repository_url(
            "https://srw-ep-x.srw-connectors.svc.cluster.local:8443",
            _CONNECTOR.upper(),
            upstream,
        )
        assert url == (
            f"https://srw-ep-x.srw-connectors.svc.cluster.local:8443/{_CONNECTOR}/o/r"
        )

    @pytest.mark.parametrize(
        "endpoint", ["http://x:80", "https://x:8443/", "x:8443", "https://x/p/"]
    )
    def test_the_driver_endpoint_is_https_without_a_path(self, endpoint):
        with pytest.raises(ValueError):
            driver_repository_url(
                endpoint, _CONNECTOR, swap_upstream("https://github.com/o/r")
            )

    @pytest.mark.parametrize("bad", ["", "x", "../" + _CONNECTOR, _CONNECTOR + "/"])
    def test_a_binding_is_named_by_a_connector_uuid(self, bad):
        with pytest.raises(ValueError):
            connector_path_id(bad)


class TestSpec:
    def test_the_spec_is_valid(self):
        assert validate_spec(GIT_SWAP_SPEC) == []

    def test_it_is_a_tls_workspace_service_holding_the_forge_token(self):
        spec = GIT_SWAP_SPEC
        assert spec.plane == "service" and spec.legacy_type == "repository"
        assert spec.service.tls and spec.service.callers == ("workspace",)
        assert spec.credential_delivery == "lease"
        assert spec.delivery_forms == ("checkout", "lease_token")
        assert spec.holds_upstream_credentials
        assert spec.harness_credentials == ("token",)
        # The upstream host from the connector's config, port 443 only.
        assert [(rule.host, rule.ports) for rule in spec.egress] == [
            ("${config.host}", (443,))
        ]

    def test_every_access_level_names_a_real_mechanism(self):
        read_only = GIT_SWAP_SPEC.access_level("ReadOnly")
        read_write = GIT_SWAP_SPEC.access_level("ReadWrite")
        assert not read_only.advisory and "request path" in read_only.enforced_by
        assert "refs/heads/" in read_write.enforced_by
        assert "branch protection" in read_write.enforced_by

    def test_harness_credentials_are_a_lease_drivers_slots(self):
        inline = replace(REPOSITORY_SPEC, harness_credentials=("token",))
        assert "only a lease driver names harness_credentials" in validate_spec(inline)
        unknown = replace(GIT_SWAP_SPEC, harness_credentials=("password",))
        assert any("not a credential slot" in p for p in validate_spec(unknown))
        no_slot = replace(
            GIT_SWAP_SPEC,
            credential_slots=(CredentialSlot("secret", "secret_string", {}),),
        )
        assert any("not a credential slot" in p for p in validate_spec(no_slot))

    def test_tls_is_a_boolean(self):
        broken = replace(
            GIT_SWAP_SPEC, service=replace(GIT_SWAP_SPEC.service, tls="yes")
        )
        assert "service tls is a boolean" in validate_spec(broken)
        assert ServiceSpec().tls is False


class TestEntries:
    def _entry(self, **fields):
        return {
            "type": "repository",
            "name": "r",
            "connection_url": "https://github.com/o/r",
            "credentials": {"token": "t"},
            "project_read_only": False,
            **fields,
        }

    def test_a_swap_entry_names_the_swap_driver(self):
        assert git_swap_entry(self._entry(git_swap={}))
        assert driver_spec_for_row(self._entry(git_swap={})) is GIT_SWAP_SPEC
        assert (
            driver_spec_for_row(self._entry(git_swap={"url": "https://x/y"}))
            is GIT_SWAP_SPEC
        )

    def test_a_refused_or_plain_entry_stays_a_repository(self):
        for entry in (
            self._entry(),
            self._entry(git_swap={"unavailable": "off"}),
            self._entry(git_swap="yes"),
        ):
            assert not git_swap_entry(entry)
            assert driver_spec_for_row(entry) is REPOSITORY_SPEC

    def test_only_a_repository_entry_can_name_it(self):
        assert not git_swap_entry({"type": "generic", "git_swap": {}})

    def test_a_read_only_link_binds_read_only(self):
        assert effective_access(self._entry(git_swap={}), GIT_SWAP_SPEC) == "ReadWrite"
        assert (
            effective_access(
                self._entry(git_swap={}, project_read_only=True), GIT_SWAP_SPEC
            )
            == "ReadOnly"
        )
