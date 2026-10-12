"""Safety contract and expectations of the local D2 matrix gate (never run here)."""

from __future__ import annotations

import copy
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

from shared.connectors.builtin import (
    BUILTIN_SPECS,
    DEVELOPMENT_SPECS,
    MANAGED_MCP_SPECS,
    OFFICIAL_SERVICE_SPECS,
)

ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = ROOT / "scripts" / "k3d-connector-matrix-gate.py"
_SPEC = importlib.util.spec_from_file_location("k3d_connector_matrix_gate", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = gate
_SPEC.loader.exec_module(gate)

_FIXTURES = ROOT / "cockpit/src/app/core/models/fixtures"
MATRIX = json.loads((_FIXTURES / "connector-drivers.json").read_text())
#: The rows the managed MCP servers add where the chart installs them.
MANAGED = json.loads((_FIXTURES / "connector-drivers-managed.json").read_text())
NAMES = [spec.name for spec in BUILTIN_SPECS]
#: This checkout's spec classes, as the gate reads them.
SPECS = gate.spec_classes()
#: The built-ins alone: every other driver is a stranger.
BUILTIN_ONLY = gate.SpecClasses(builtin=tuple(NAMES))


def _driver(name: str) -> dict:
    return next(d for d in MATRIX["drivers"] if d["name"] == name)


def test_a_type_variant_is_not_the_types_driver():
    """srw.mcp-remote/v1 serves some mcp rows; the form and the link rows
    still take srw.mcp/v1 as the mcp type's driver."""
    owners = [
        d["name"]
        for d in MATRIX["drivers"]
        if d["legacy_type"] == "mcp" and gate.owns_type(d)
    ]
    assert owners == ["srw.mcp/v1"]
    assert gate.owns_type(_driver("srw.mcp-remote/v1")) is False
    # A matrix from before D3a has no flag: every typed driver owned its type.
    assert gate.owns_type({"legacy_type": "mcp"}) is True


@pytest.fixture
def no_cluster(monkeypatch):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: pytest.fail("must not touch the cluster")
    )


@pytest.mark.parametrize(
    "argv",
    [
        ["--context", "k3d-other"],
        ["--namespace", "default"],
        ["--user", "Robert'); DROP"],
        ["--base-url", "https://cockpit.example.com"],
    ],
)
def test_refuses_anything_outside_the_local_cluster(argv, no_cluster):
    assert gate.main(argv) == 2


def test_dry_run_prints_the_plan_and_touches_nothing(no_cluster, capsys):
    assert gate.main([]) == 0
    out = capsys.readouterr().out
    for phase in ("preflight", "api", "page", "picker", "links"):
        assert f"- {phase}:" in out


def test_the_password_never_reaches_an_argument(monkeypatch):
    seen: list[list[str]] = []

    def fake_run(argv, **kwargs):
        seen.append(argv)
        body = json.dumps({"status": 200, "body": json.dumps(MATRIX)})
        return subprocess.CompletedProcess(argv, 0, stdout=body + "\n", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    args = gate.build_parser().parse_args(["--run", "--password", "s3cret-pw"])
    gate.MatrixGate(args).api()
    assert seen and all("s3cret-pw" not in " ".join(argv) for argv in seen)
    assert gate._scrub("token s3cret-pw") == "token <redacted>"


class TestExpectations:
    def test_the_built_in_matrix_passes(self):
        assert gate.matrix_problems(MATRIX, SPECS) == []

    def test_every_list_comes_from_the_checkouts_specs(self):
        assert SPECS == gate.SpecClasses(
            builtin=tuple(NAMES),
            official=tuple(spec.name for spec in OFFICIAL_SERVICE_SPECS),
            managed=tuple(spec.name for spec in MANAGED_MCP_SPECS),
            development=tuple(spec.name for spec in DEVELOPMENT_SPECS),
        )
        assert "srw.git-swap/v1" in SPECS.official
        assert "srw.gitea-mcp/v1" in SPECS.managed
        assert SPECS.kind_of("srw.env/v1") == "builtin"
        assert SPECS.kind_of("community.example/v1") is None

    def test_offered_levels_mirror_the_cockpit(self):
        for kind, choices in gate.LITERAL_CHOICES.items():
            driver = next(d for d in MATRIX["drivers"] if d["legacy_type"] == kind)
            assert gate.picker_expectation(driver)[0] == choices
        assert gate.LITERAL_CHOICES == {
            "mcp": ["read_write"],
            "kb": ["read_only"],
            "postgresql": ["read_only", "read_write"],
        }
        assert gate.picker_expectation(_driver("srw.env/v1"))[0] == []

    def test_the_access_hint_says_what_the_creators_tag_does(self):
        # The tag removes the write tools for everyone (decision 31): no
        # enforced_by line, a hint per kind of driver, and each hint the
        # cockpit names exists in its catalogue.
        hints = json.loads(gate.EN.read_text())["datasources"]["form"]
        expected = {
            "srw.kb/v1": "accessKbHint",
            "srw.mcp/v1": "accessReadWriteOnlyHint",
            "srw.generic/v1": "accessAdvisoryHint",
            "srw.credentials/v1": "accessAdvisoryHint",
            "srw.kubeconfig/v1": "accessAdvisoryHint",
            "srw.ssh-key/v1": "accessAdvisoryHint",
            "srw.generic-file/v1": "accessAdvisoryHint",
            "srw.repository/v1": "accessReadOnlyHint",
            "srw.postgresql/v1": "accessReadOnlyHint",
            "srw.neo4j/v1": "accessReadOnlyHint",
            "srw.mongodb/v1": "accessReadOnlyHint",
            "srw.webdav/v1": "accessReadOnlyHint",
        }
        for name, hint in expected.items():
            assert gate.picker_expectation(_driver(name))[1] == hint, name
            assert "everyone who uses" in hints[hint], hint
            assert "enforced" not in hints[hint].lower(), hint

    def test_a_link_binds_the_level_its_read_only_says(self):
        shape, level, noted = gate.link_expectation(_driver("srw.postgresql/v1"), True)
        assert shape == "switch" and "READ ONLY transaction" in level["enforced_by"]
        assert not noted
        assert gate.link_expectation(_driver("srw.postgresql/v1"), None)[1]["id"] == (
            "ReadWrite"
        )
        assert gate.link_expectation(_driver("srw.mcp/v1"), True)[0] == "badge"
        assert gate.link_expectation(_driver("srw.mcp/v1"), True)[1]["tools"] == "*"
        assert gate.link_expectation(_driver("srw.kb/v1"), None)[1]["id"] == "ReadOnly"
        for kind, link, own, is_global, shape in gate.LINK_ROWS:
            driver = next(d for d in MATRIX["drivers"] if d["legacy_type"] == kind)
            assert gate.link_expectation(driver, link, own, is_global)[0] == shape

    @pytest.mark.parametrize(
        ("link", "own", "is_global", "level", "noted"),
        [
            # A read-write or unset link cannot lift the creator's tag ...
            (False, True, False, "ReadOnly", True),
            (None, True, False, "ReadOnly", True),
            # ... nor a public connector's with no mode set (decision 32).
            (None, None, True, "ReadOnly", True),
            (False, None, True, "ReadOnly", True),
            # Published read-write: the link decides.
            (False, False, True, "ReadWrite", False),
            (True, False, True, "ReadOnly", False),
            # Untagged: the link decides, as before.
            (None, None, False, "ReadWrite", False),
            (True, None, False, "ReadOnly", False),
        ],
    )
    def test_a_link_binds_the_stricter_of_itself_and_the_creators_tag(
        self, link, own, is_global, level, noted
    ):
        shape, bound, shown = gate.link_expectation(
            _driver("srw.postgresql/v1"), link, own, is_global
        )
        assert bound["id"] == level
        assert shown is noted
        # A tag the link cannot lift replaces the switch.
        assert shape == ("badge" if noted else "switch")

    def test_the_link_rows_cover_the_creators_tag(self):
        rows = {
            (kind, link, own, is_global)
            for kind, link, own, is_global, _ in gate.LINK_ROWS
        }
        assert ("postgresql", False, True, False) in rows
        assert ("postgresql", None, None, True) in rows
        assert ("postgresql", False, False, True) in rows
        note = json.loads(gate.EN.read_text())["projectDetail"]["datasources"][
            "accessCreatorReadOnly"
        ]
        assert "everyone who uses it" in note

    def test_a_drifted_matrix_is_reported(self):
        drifted = copy.deepcopy(MATRIX)
        postgres = next(
            d for d in drifted["drivers"] if d["name"] == "srw.postgresql/v1"
        )
        postgres["access_levels"][0]["enforced_by"] = ""
        postgres["trust"]["tier"] = "custom"
        postgres["egress"]["enforced"]["status"] = "verified"
        postgres["credential_slots"] = [
            {"name": "x", "schema": {"properties": {"p": {"default": "pw"}}}}
        ]
        problems = gate.matrix_problems(drifted, SPECS)
        assert any("no enforced_by line" in p for p in problems)
        assert any("not built-in" in p for p in problems)
        assert any("egress enforced" in p for p in problems)
        assert any("carries a value" in p for p in problems)
        assert gate.matrix_problems(MATRIX, gate.SpecClasses(builtin=tuple(NAMES[1:])))

    def test_a_development_driver_must_be_labelled_development(self):
        from orchestrator.services.connector_drivers import builtin_connector_drivers
        from orchestrator.services.connector_drivers.matrix import capability_matrix

        with_probe = json.loads(
            json.dumps(capability_matrix(builtin_connector_drivers(lease_probe=True)))
        )
        # The k3d profile installs the lease probe: accepted, as development.
        assert gate.matrix_problems(with_probe, SPECS) == []
        assert [d["name"] for d in with_probe["drivers"] if gate.is_development(d)] == [
            "srw.lease-probe/v1"
        ]
        # Without the development names it is a stranger, as before.
        assert gate.matrix_problems(with_probe, BUILTIN_ONLY) == [
            "srw.lease-probe/v1 is no driver spec of this checkout"
        ]
        # Labelled built-in and trusted, it is a product bug the gate names.
        mislabelled = copy.deepcopy(with_probe)
        mislabelled["drivers"][-1]["trust"] = {"tier": "builtin", "trusted": True}
        assert gate.matrix_problems(mislabelled, SPECS) == [
            "srw.lease-probe/v1 is not labelled development and untrusted"
        ]
        # The k3d profile also installs the echo service (D5): a service-plane
        # driver whose egress columns are hosting statuses, not "not applicable".
        from orchestrator.services.connector_drivers.matrix import HostingStatus

        for hosting in (HostingStatus(enabled=True), None):
            with_echo = json.loads(
                json.dumps(
                    capability_matrix(
                        builtin_connector_drivers(
                            lease_probe=True, echo_service_image="r/echo:1"
                        ),
                        hosting=hosting,
                    )
                )
            )
            assert gate.matrix_problems(with_echo, SPECS) == []
        with_echo["drivers"][-1]["egress"]["enforced"]["status"] = "not_applicable"
        assert gate.matrix_problems(with_echo, SPECS) == [
            "srw.echo-service/v1 egress enforced is not a hosting status"
        ]
        # A built-in labelled development is refused too.
        demoted = copy.deepcopy(with_probe)
        demoted["drivers"][0]["trust"]["tier"] = "development"
        assert any(
            "srw.env/v1 is not built-in" in p
            for p in gate.matrix_problems(demoted, SPECS)
        )


def _installed_everywhere() -> dict:
    """The matrix of a deployment installing every optional driver SRW ships,
    with service-pod hosting on (the regression run's k3d profile at most)."""
    from orchestrator.services.connector_drivers import builtin_connector_drivers
    from orchestrator.services.connector_drivers.matrix import (
        HostingStatus,
        capability_matrix,
    )
    from shared.connectors.contract import managed_mcp_driver

    registry = builtin_connector_drivers(
        lease_probe=True,
        echo_service_image="r/echo:1",
        git_swap_image="r/git-swap:1",
        managed_mcp_images={
            spec.name: f"r/{spec.legacy_type}:1"
            for spec in MANAGED_MCP_SPECS + DEVELOPMENT_SPECS
            if managed_mcp_driver(spec)
        },
    )
    return json.loads(
        json.dumps(capability_matrix(registry, hosting=HostingStatus(enabled=True)))
    )


def _row(matrix: dict, name: str) -> dict:
    return next(d for d in matrix["drivers"] if d["name"] == name)


class TestOfficialAndManagedDrivers:
    """The drivers added since D2: C3's git swap and D5a's managed MCP servers."""

    def test_a_deployment_installing_them_all_passes(self):
        matrix = _installed_everywhere()
        names = [d["name"] for d in matrix["drivers"]]
        for spec in OFFICIAL_SERVICE_SPECS + MANAGED_MCP_SPECS + DEVELOPMENT_SPECS:
            assert spec.name in names
        assert gate.matrix_problems(matrix, SPECS) == []

    def test_the_drift_the_regression_run_hit_is_named(self):
        # The gate from before: built-ins and development drivers only.
        matrix = _installed_everywhere()
        old = gate.SpecClasses(builtin=tuple(NAMES), development=SPECS.development)
        assert gate.matrix_problems(matrix, old) == [
            "srw.git-swap/v1 is no driver spec of this checkout",
            "srw.gitea-mcp/v1 is no driver spec of this checkout",
        ]

    def test_an_official_driver_must_be_trusted_with_its_image(self):
        matrix = _installed_everywhere()
        _row(matrix, "srw.git-swap/v1")["trust"]["tier"] = "custom"
        assert gate.matrix_problems(matrix, SPECS) == [
            "srw.git-swap/v1 is not labelled trusted"
        ]
        matrix = _installed_everywhere()
        _row(matrix, "srw.git-swap/v1")["trust"]["image"] = None
        assert gate.matrix_problems(matrix, SPECS) == ["srw.git-swap/v1 names no image"]

    @pytest.mark.parametrize(
        "trust",
        [
            {"tier": "custom", "trusted": False, "claims_declared_by_author": True},
            {"tier": "managed", "trusted": True},
            {"tier": "managed", "claims_declared_by_author": True},
            {"tier": "trusted", "trusted": True},
        ],
    )
    def test_a_managed_server_must_be_managed_untrusted_and_srws_word(self, trust):
        matrix = _installed_everywhere()
        _row(matrix, "srw.gitea-mcp/v1")["trust"].update(trust)
        assert gate.matrix_problems(matrix, SPECS) == [
            "srw.gitea-mcp/v1 is not labelled managed, untrusted and SRW's word"
        ]

    def test_a_managed_server_names_its_image_and_offers_its_levels(self):
        matrix = _installed_everywhere()
        gitea = _row(matrix, "srw.gitea-mcp/v1")
        gitea["trust"]["image"] = ""
        gitea["config_schema"]["properties"]["access"]["enum"] = ["ReadWrite"]
        assert gate.matrix_problems(matrix, SPECS) == [
            "srw.gitea-mcp/v1 names no image",
            "srw.gitea-mcp/v1 config access is not its levels",
        ]

    def test_each_list_keeps_its_own_order(self):
        specs = gate.SpecClasses(builtin=(), managed=("a/v1", "b/v1"))
        rows = [
            {**copy.deepcopy(MANAGED["drivers"][0]), "name": name}
            for name in ("b/v1", "a/v1")
        ]
        assert gate.matrix_problems({"drivers": rows}, specs) == [
            "managed drivers ['b/v1', 'a/v1'] are not in the order of ('a/v1', 'b/v1')"
        ]
        # Either alone is fine: only an installed one is listed.
        assert gate.matrix_problems({"drivers": rows[:1]}, specs) == []

    def test_a_registered_driver_the_account_sees_is_its_registrations(self):
        matrix = copy.deepcopy(MATRIX)
        # A registered image runs in a driver pod (D6): its egress columns
        # are a hosting status, as a service driver's.
        row = {
            **copy.deepcopy(_driver("srw.generic/v1")),
            "name": "acme.env/v1",
            "legacy_type": "image_driver",
            "serves_stored_type": False,
            "plane": "bind_time",
            "trust": {
                "tier": "custom",
                "trusted": False,
                "image": "r/acme:1",
                "claims_declared_by_author": True,
            },
            "registration": {"id": "r1"},
            "egress": {
                "declared": {"rules": [], "needs_dns": None},
                "enforced": {"status": "not_enforced"},
                "installation": {"status": "not_enforced"},
            },
        }
        matrix["drivers"].append(row)
        assert gate.matrix_problems(matrix, SPECS) == []
        row["trust"]["tier"] = "builtin"
        assert gate.matrix_problems(matrix, SPECS) == [
            "registered driver acme.env/v1 is not labelled trusted or custom"
        ]
        # Without a registration it is a stranger.
        del row["registration"]
        row["trust"]["tier"] = "custom"
        assert "acme.env/v1 is no driver spec of this checkout" in gate.matrix_problems(
            matrix, SPECS
        )

    def test_the_picker_offers_a_managed_server_its_own_levels(self):
        (gitea,) = (d for d in MANAGED["drivers"] if d["name"] == "srw.gitea-mcp/v1")
        assert gate.is_managed(gitea) and gate.owns_type(gitea)
        assert gate.picker_expectation(gitea) == (
            ["read_only", "read_write"],
            "accessReadOnlyHint",
        )
        assert gate.level_ids(gitea) == ["ReadOnly", "ReadWrite"]
        assert gate.access_choice(gitea) == gate.level_ids(gitea)
        # A built-in without a config choice has none.
        assert gate.access_choice(_driver("srw.postgresql/v1")) is None
