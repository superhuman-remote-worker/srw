from __future__ import annotations

import dataclasses
import io
import json
from pathlib import Path
import shutil
import stat
import subprocess
import tarfile
import threading

import pytest
import yaml

from tests.e2e.app import harness


class FakeRunner(harness.CommandRunner):
    def __init__(self, responses: list[harness.CommandResult]):
        self.responses = list(responses)
        self.commands: list[list[str]] = []

    def run(self, argv, **kwargs):
        self.commands.append(list(argv))
        if not self.responses:
            raise AssertionError(f"unexpected command: {argv}")
        return self.responses.pop(0)


def test_cluster_name_guard_never_accepts_shared_or_lookalike_names() -> None:
    assert (
        harness.validate_cluster_name("srw-e2e-20260824-123456-ab12cd34")
        == "srw-e2e-20260824-123456-ab12cd34"
    )
    for unsafe in (
        "srw",
        "srw-e2e",
        "srw-e2e-prod!",
        "other-srw-e2e-20260824-abcd1234",
        "SRW-E2E-20260824-abcd1234",
        "srw-e2e-../srw",
    ):
        with pytest.raises(harness.SafetyError):
            harness.validate_cluster_name(unsafe)


def test_owned_cluster_guard_checks_live_container_id_and_k3d_labels(
    tmp_path: Path,
) -> None:
    state_root = tmp_path / "state"
    store = harness.StateStore(state_root)
    ledger = store.initialize("20260824-123456-ab12cd34")
    container_id = "a" * 64
    ledger["created_by_run"] = True
    ledger["server_container_id"] = container_id
    store.persist(ledger)
    runner = FakeRunner(
        [
            harness.CommandResult(
                0,
                f"{container_id}|{ledger['cluster_name']}|server\n",
            )
        ]
    )
    application = harness.ApplicationE2EHarness(state_root, runner)

    application._assert_owned_cluster(ledger)

    assert runner.commands[0][0:3] == ["docker", "inspect", "--format"]
    assert runner.commands[0][-1] == f"k3d-{ledger['cluster_name']}-server-0"


def test_owned_cluster_guard_rejects_recreated_cluster_identity(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    store = harness.StateStore(state_root)
    ledger = store.initialize("20260824-123456-ab12cd34")
    ledger["created_by_run"] = True
    ledger["server_container_id"] = "a" * 64
    store.persist(ledger)
    runner = FakeRunner(
        [
            harness.CommandResult(
                0,
                f"{'b' * 64}|{ledger['cluster_name']}|server\n",
            )
        ]
    )

    with pytest.raises(harness.SafetyError, match="no longer matches"):
        harness.ApplicationE2EHarness(state_root, runner)._assert_owned_cluster(ledger)


def test_down_requires_post_delete_absence_before_clearing_ownership(
    tmp_path: Path,
) -> None:
    state_root = tmp_path / "state"
    store = harness.StateStore(state_root)
    ledger = store.initialize("20260824-123456-ab12cd34")
    container_id = "a" * 64
    ledger["created_by_run"] = True
    ledger["server_container_id"] = container_id
    store.persist(ledger)
    runner = FakeRunner(
        [
            harness.CommandResult(0, "live\n"),
            harness.CommandResult(
                0,
                f"{container_id}|{ledger['cluster_name']}|server\n",
            ),
            harness.CommandResult(0),
            harness.CommandResult(
                0,
                json.dumps([{"name": ledger["cluster_name"]}]),
            ),
        ]
    )
    application = harness.ApplicationE2EHarness(state_root, runner)

    with pytest.raises(harness.SafetyError, match="did not remove"):
        application.down(ledger)

    assert store.active_path.exists()


def test_authoritative_run_is_red_when_teardown_fails(monkeypatch) -> None:
    ledger = {"created_by_run": True}

    class Store:
        @staticmethod
        def load():
            return ledger

    class Application:
        store = Store()

        @staticmethod
        def up(_profile_name):
            return ledger

        @staticmethod
        def test_owned(_ledger):
            return None

        @staticmethod
        def cleanup(_ledger):
            return None

        @staticmethod
        def down(_ledger):
            raise harness.HarnessError("verified teardown failure")

    monkeypatch.setattr(harness, "_safe_error_text", lambda exc, _app: str(exc))

    assert harness._run_authoritative(Application()) == 1


def test_dirty_run_success_is_not_reported_as_authoritative(capsys) -> None:
    ledger = {"created_by_run": True, "authoritative": False}

    class Store:
        @staticmethod
        def load():
            return ledger

    class Application:
        store = Store()

        @staticmethod
        def up(_profile_name):
            return ledger

        @staticmethod
        def test_owned(_ledger):
            return None

        @staticmethod
        def cleanup(_ledger):
            return None

        @staticmethod
        def down(_ledger):
            return None

    assert harness._run_authoritative(Application()) == 0
    output = capsys.readouterr().out
    assert "non-authoritative dirty-tree pinned-virtual golden journey passed" in output
    assert "[e2e-app] authoritative pinned-virtual golden journey passed" not in output


def test_teardown_does_not_treat_a_corrupt_active_ledger_as_absent(
    tmp_path: Path,
) -> None:
    state_root = tmp_path / "state"
    store = harness.StateStore(state_root)
    store.initialize("20260824-123456-ab12cd34")
    store.active_path.chmod(0o644)

    error = harness._best_effort_down(
        harness.ApplicationE2EHarness(state_root, FakeRunner([]))
    )

    assert isinstance(error, harness.SafetyError)
    assert store.active_path.exists()


def test_state_ledger_rejects_run_directory_escape(tmp_path: Path) -> None:
    store = harness.StateStore(tmp_path / "state")
    ledger = store.initialize("20260824-123456-ab12cd34")
    ledger["run_dir"] = str(tmp_path / "outside")

    with pytest.raises(harness.SafetyError, match="outside"):
        store.validate(ledger)


def test_state_ledger_binds_the_selected_profile_and_rejects_unknown_values(
    tmp_path: Path,
) -> None:
    store = harness.StateStore(tmp_path / "state")
    ledger = store.initialize(
        "20260824-123456-ab12cd34", profile_name="stateless-sandbox"
    )

    assert (
        harness.profile_from_ledger(ledger)
        == harness.APPLICATION_E2E_PROFILES["stateless-sandbox"]
    )

    ledger["profile"] = "not-a-profile"
    with pytest.raises(harness.SafetyError, match="unknown application E2E profile"):
        store.validate(ledger)


def test_state_store_rejects_preexisting_unowned_root_without_chmod(
    tmp_path: Path,
) -> None:
    unowned = tmp_path / "preexisting"
    unowned.mkdir(mode=0o700)
    before = stat.S_IMODE(unowned.stat().st_mode)

    with pytest.raises(harness.SafetyError, match="not harness-owned"):
        harness.StateStore(unowned).initialize("20260824-123456-ab12cd34")

    assert stat.S_IMODE(unowned.stat().st_mode) == before


def test_default_state_store_creates_only_its_missing_trusted_output_parent(
    tmp_path: Path, monkeypatch
) -> None:
    cockpit = tmp_path / "cockpit"
    cockpit.mkdir()
    default_root = cockpit / "test-results/app-harness"
    monkeypatch.setattr(harness, "DEFAULT_STATE_ROOT", default_root)

    ledger = harness.StateStore(default_root).initialize("20260824-123456-ab12cd34")

    assert default_root.parent.is_dir()
    assert Path(ledger["run_dir"]).parent == default_root
    assert (default_root / harness.STATE_ROOT_MARKER).is_file()


def test_active_claim_is_exclusive_and_cannot_be_overwritten(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    first = harness.StateStore(state_root)
    first_ledger = first.initialize("20260824-123456-ab12cd34")
    active_before = first.active_path.read_bytes()

    with pytest.raises(harness.SafetyError, match="already active"):
        harness.StateStore(state_root).initialize("20260824-123457-bc23de45")
    with pytest.raises(harness.SafetyError, match="already active"):
        harness.write_private_json_exclusive(
            first.active_path,
            {"schema": 1, "owner": harness.OWNER, "run_id": "foreign"},
        )

    assert first.active_path.read_bytes() == active_before
    assert (
        harness.read_private_json(first.active_path)["run_id"] == first_ledger["run_id"]
    )


def test_clear_active_refuses_a_foreign_run_claim(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    store = harness.StateStore(state_root)
    ledger = store.initialize("20260824-123456-ab12cd34")
    foreign = dict(ledger)
    foreign["run_id"] = "20260824-123457-bc23de45"
    foreign["cluster_name"] = "srw-e2e-20260824-123457-bc23de45"
    harness.write_private_json(store.active_path, foreign)

    with pytest.raises(harness.SafetyError, match="different run"):
        store.clear_active(ledger)

    assert harness.read_private_json(store.active_path)["run_id"] == foreign["run_id"]


def test_state_lock_blocks_clear_and_new_owner_until_stale_persist_finishes(
    tmp_path: Path,
) -> None:
    state_root = tmp_path / "state"
    stale_store = harness.StateStore(state_root)
    stale_ledger = stale_store.initialize("20260824-123456-ab12cd34")
    checked = threading.Event()
    resume = threading.Event()
    clear_done = threading.Event()
    thread_errors: list[BaseException] = []
    original_assert = stale_store._assert_active_run

    def pausing_assert(ledger) -> None:
        original_assert(ledger)
        checked.set()
        if not resume.wait(timeout=5):
            raise AssertionError("test did not resume stale persist")

    stale_store._assert_active_run = pausing_assert  # type: ignore[method-assign]

    def stale_persist() -> None:
        try:
            stale_store.persist(stale_ledger)
        except BaseException as exc:  # surfaced by the main test thread
            thread_errors.append(exc)

    def clear_first_owner() -> None:
        try:
            harness.StateStore(state_root).clear_active(stale_ledger)
        except BaseException as exc:  # surfaced by the main test thread
            thread_errors.append(exc)
        finally:
            clear_done.set()

    stale_thread = threading.Thread(target=stale_persist)
    clear_thread = threading.Thread(target=clear_first_owner)
    stale_thread.start()
    assert checked.wait(timeout=2)
    clear_thread.start()
    # The clearer cannot pass its owner check while stale persist holds the
    # state-root lock after its own owner check.
    assert not clear_done.wait(timeout=0.2)
    resume.set()
    stale_thread.join(timeout=2)
    clear_thread.join(timeout=2)
    stale_store._assert_active_run = original_assert  # type: ignore[method-assign]

    assert not stale_thread.is_alive()
    assert not clear_thread.is_alive()
    assert thread_errors == []
    replacement_store = harness.StateStore(state_root)
    replacement = replacement_store.initialize("20260824-123457-bc23de45")

    # Exact stale-A -> cleared-A -> initialized-B -> stale-A-resumes contract:
    # the old ledger can no longer overwrite B's active authority.
    with pytest.raises(harness.SafetyError, match="different run"):
        stale_store.persist(stale_ledger)
    assert (
        harness.read_private_json(replacement_store.active_path)["run_id"]
        == replacement["run_id"]
    )


def test_origin_guard_requires_explicit_remote_opt_in() -> None:
    assert harness.validate_origin("http://localhost:8080") == "http://localhost:8080"
    assert harness.validate_origin("https://app.localhost") == "https://app.localhost"
    assert (
        harness.validate_origin("http://srw-e2e.test", owned_host="srw-e2e.test")
        == "http://srw-e2e.test"
    )
    with pytest.raises(harness.SafetyError, match="explicit"):
        harness.validate_origin("https://shared.example.com")
    assert (
        harness.validate_origin("https://shared.example.com", allow_remote=True)
        == "https://shared.example.com"
    )
    for malformed in (
        "file:///tmp/app",
        "https://user:password@example.com",
        "https://example.com/path",
        "https://example.com?token=x",
    ):
        with pytest.raises(harness.SafetyError):
            harness.validate_origin(malformed, allow_remote=True)


def test_private_secret_files_are_0600_and_bundle_repr_is_redacted(
    tmp_path: Path,
) -> None:
    bundle = harness.SecretBundle.generate("20260824-123456-ab12cd34")
    path = tmp_path / "credentials.json"
    harness.write_private_json(path, dataclasses.asdict(bundle))

    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert bundle.admin_password not in repr(bundle)
    assert bundle.provider_control_token not in repr(bundle)
    restored = harness.SecretBundle.from_json(harness.read_private_json(path))
    assert restored.admin_username == bundle.admin_username


def test_diagnostic_redaction_removes_known_and_structural_secrets() -> None:
    secret = "correct-horse-battery-staple"
    raw = (
        f"Authorization: Bearer {secret}\n"
        f"GET /x?token={secret}\n"
        f"WebSocket wss://app.test/p/thread/ws?t={secret}\n"
        f'{{"messages":[{{"content":"E2E-private"}}]}}\n'
        "ordinary readiness line\n"
    )

    redacted = harness.sanitize_diagnostic(raw, [secret])

    assert secret not in redacted
    assert "E2E-private" not in redacted
    assert "ordinary readiness line" in redacted
    assert "[REDACTED" in redacted


def test_log_diagnostics_keep_metadata_but_never_free_form_error_text() -> None:
    raw = (
        "2026-08-24T12:00:00Z ERROR status=503 user prompt was private words\n"
        "ordinary healthy line\n"
        "WARNING E2E-private-run secret-like arbitrary message\n"
    )

    sanitized = harness.sanitize_log_diagnostic(raw)

    assert "private words" not in sanitized
    assert "arbitrary message" not in sanitized
    assert "severity=error" in sanitized
    assert "status=503" in sanitized
    assert "run_correlated=true" in sanitized


def test_diagnostics_rejects_a_symlink_without_chmodding_target(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    store = harness.StateStore(state_root)
    ledger = store.initialize("20260824-123456-ab12cd34")
    target = tmp_path / "outside"
    target.mkdir(mode=0o755)
    before = stat.S_IMODE(target.stat().st_mode)
    (Path(ledger["run_dir"]) / "diagnostics").symlink_to(
        target, target_is_directory=True
    )

    with pytest.raises(harness.SafetyError, match="not a regular directory"):
        harness.ApplicationE2EHarness(state_root).diagnostics(ledger)

    assert stat.S_IMODE(target.stat().st_mode) == before


def test_attach_prerequisites_do_not_require_cluster_tools(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(harness.shutil, "which", lambda name: f"/bin/{name}")
    runner = FakeRunner([harness.CommandResult(0, "27.0.0\n")])

    harness.ApplicationE2EHarness(
        tmp_path / "state", runner
    ).check_attach_prerequisites()

    assert runner.commands == [["docker", "version", "--format", "{{.Server.Version}}"]]


def test_readiness_layer_records_incremental_elapsed_evidence(tmp_path: Path) -> None:
    store = harness.StateStore(tmp_path / "state")
    ledger = store.initialize("20260824-123456-ab12cd34")

    harness.ApplicationE2EHarness(store.root)._mark_layer(ledger, "cluster-api")

    evidence = harness.read_private_json(Path(ledger["run_dir"]) / "readiness.json")
    assert evidence["last_completed_layer"] == "cluster-api"
    assert evidence["layer_timings"]["cluster-api"]["elapsed_ms"] >= 0
    assert evidence["layer_timings"]["cluster-api"]["completed_at"].endswith("+00:00")


def test_command_composition_is_current_sha_owned_and_non_atomic(
    tmp_path: Path,
) -> None:
    sha = "a" * 40
    run_id = "20260824-123456-ab12cd34"
    images, build_commands = harness.build_image_commands(sha, run_id)

    assert set(images) == {
        "orchestrator",
        "agent",
        "cockpit",
        "provider",
        "playwright",
    }
    assert all(sha[:12] in image for image in images.values())
    assert all(command[:2] == ["docker", "build"] for command in build_commands)
    assert f"SRW_SOURCE_REVISION={sha}" in build_commands[0]
    assert f"BUILD_SHA={sha}" in build_commands[1]
    assert harness.cluster_create_command(tmp_path / "k3d.yaml") == [
        "k3d",
        "cluster",
        "create",
        "--config",
        str(tmp_path / "k3d.yaml"),
    ]
    helm_command = harness.helm_install_command(
        tmp_path / "kubeconfig", tmp_path / "images.yaml"
    )
    assert "--atomic" not in helm_command
    assert "--wait-for-jobs" in helm_command
    assert helm_command[0:3] == ["helm", "upgrade", "--install"]


def test_stateless_profile_adds_current_source_workspace_image_and_values(
    tmp_path: Path,
) -> None:
    sha = "a" * 40
    run_id = "20260824-123456-ab12cd34"
    profile = harness.resolve_profile("stateless-sandbox")
    images, commands = harness.build_image_commands(
        sha, run_id, include_workspace=profile.include_workspace_image
    )

    assert set(images) == {
        "orchestrator",
        "agent",
        "cockpit",
        "provider",
        "playwright",
        "workspace",
    }
    workspace_command = commands[-1]
    assert "docker/Dockerfile.workspace" in workspace_command
    assert images["workspace"] in workspace_command

    generated = yaml.safe_load(harness._image_values(images, sha, run_id))
    assert generated["image"]["workspace"] == {
        "repository": "srw-e2e-workspace",
        "tag": images["workspace"].split(":", 1)[1],
        "digest": "",
        "pullPolicy": "IfNotPresent",
    }
    assert generated["provenance"]["components"]["workspace"]["sourceRevision"] == sha

    command = harness.helm_install_command(
        tmp_path / "kubeconfig", tmp_path / "images.yaml", profile.values_files
    )
    base_index = command.index(str(harness.VALUES_FILE))
    overlay_index = command.index(str(harness.STATELESS_SANDBOX_VALUES_FILE))
    image_index = command.index(str(tmp_path / "images.yaml"))
    assert base_index < overlay_index < image_index


def test_dependency_images_use_host_platform_archives_before_k3d_import(
    tmp_path: Path,
) -> None:
    assert harness.DEPENDENCY_IMAGES == (
        "busybox:1.36",
        "postgres:15",
        "pgvector/pgvector:pg15",
        "quay.io/keycloak/keycloak:26.2",
        "rancher/mirrored-library-busybox:1.36.1",
    )
    images = {
        "orchestrator": "srw-e2e-orchestrator:test",
        "agent": "srw-e2e-agent:test",
        "cockpit": "srw-e2e-cockpit:test",
        "provider": "srw-e2e-provider:test",
    }
    groups = harness.image_import_groups(images)
    assert len(groups) == len(harness.DEPENDENCY_IMAGES) + 1
    assert [refs for _, refs in groups[:-1]] == [
        (dependency,) for dependency in harness.DEPENDENCY_IMAGES
    ]
    assert groups[-1] == ("application", tuple(images.values()))

    images["workspace"] = "srw-e2e-workspace:test"
    stateless_groups = harness.image_import_groups(images)
    assert stateless_groups[-1] == (
        "application",
        tuple(images.values()),
    )

    for label, image_refs in groups:
        archive = tmp_path / f"{label}.tar"
        save = harness.docker_image_save_command(image_refs, archive, "linux/amd64")
        assert save[:7] == [
            "docker",
            "image",
            "save",
            "--platform",
            "linux/amd64",
            "--output",
            str(archive),
        ]
        assert save[7:] == list(image_refs)
        command = harness.k3d_image_import_command(
            archive, "srw-e2e-20260824-123456-ab12cd34"
        )
        assert command[:4] == ["k3d", "image", "import", str(archive)]
        assert command[4:6] == [
            "--cluster",
            "srw-e2e-20260824-123456-ab12cd34",
        ]
        assert command[-2:] == ["--mode", "direct"]


def test_k3d_profile_configures_http_with_a_dynamic_host_port() -> None:
    profile = yaml.safe_load(harness.K3D_TEMPLATE.read_text(encoding="utf-8"))

    assert profile["ports"] == [{"port": "0:80", "nodeFilters": ["loadbalancer"]}]


@pytest.mark.parametrize(
    "platform",
    ("", "amd64", "windows/amd64", "linux/amd64;touch-x", "linux/../amd64"),
)
def test_container_platform_validation_fails_closed(platform: str) -> None:
    with pytest.raises(harness.SafetyError, match="platform"):
        harness.validate_container_platform(platform)


def test_dependency_image_inspection_proves_platform_without_api_1_49() -> None:
    image_id = "a" * 64

    command = harness.docker_image_identity_command("busybox:1.36")

    assert command == [
        "docker",
        "image",
        "inspect",
        "--format",
        "{{.Os}}/{{.Architecture}}|{{.Id}}",
        "busybox:1.36",
    ]
    assert "--platform" not in command
    assert (
        harness.validate_docker_image_identity(
            f"linux/amd64|sha256:{image_id}\n", "linux/amd64"
        )
        == f"sha256:{image_id}"
    )
    with pytest.raises(harness.SafetyError, match="platform identity"):
        harness.validate_docker_image_identity(
            f"linux/arm64|sha256:{image_id}", "linux/amd64"
        )


def test_docker_archive_runtime_id_uses_config_digest_not_manifest_digest(
    tmp_path: Path,
) -> None:
    config_digest = "b" * 64
    manifest_digest = "c" * 64
    payload = json.dumps(
        [
            {
                "Config": f"blobs/sha256/{config_digest}",
                "RepoTags": ["busybox:1.36"],
                "Layers": [f"blobs/sha256/{manifest_digest}"],
            }
        ]
    ).encode()
    archive = tmp_path / "busybox.tar"
    with tarfile.open(archive, mode="w") as stream:
        member = tarfile.TarInfo("manifest.json")
        member.size = len(payload)
        stream.addfile(member, io.BytesIO(payload))

    assert harness.docker_archive_config_ids(archive) == {
        "docker.io/library/busybox:1.36": f"sha256:{config_digest}"
    }
    assert manifest_digest not in next(
        iter(harness.docker_archive_config_ids(archive).values())
    )


def _runtime_image_test_ledger(
    tmp_path: Path, profile_name: str = harness.DEFAULT_PROFILE_NAME
) -> tuple[harness.StateStore, dict, dict[str, str]]:
    store = harness.StateStore(tmp_path / "state")
    ledger = store.initialize("20260824-123456-ab12cd34", profile_name)
    profile = harness.resolve_profile(profile_name)
    images, _commands = harness.build_image_commands(
        "a" * 40,
        str(ledger["run_id"]),
        include_workspace=profile.include_workspace_image,
    )
    components = ["orchestrator", "agent", "cockpit", "provider"]
    if profile.include_workspace_image:
        components.append("workspace")
    tags = [
        *(
            harness.canonical_containerd_tag(image)
            for image in harness.DEPENDENCY_IMAGES
        ),
        *(
            harness.canonical_containerd_tag(images[component])
            for component in components
        ),
    ]
    runtime_ids = {
        tag: f"sha256:{index:064x}" for index, tag in enumerate(tags, start=1)
    }
    ledger.update(
        {
            "created_by_run": True,
            "server_container_id": "d" * 64,
            "images": images,
            "runtime_image_ids": runtime_ids,
        }
    )
    store.persist(ledger)
    return store, ledger, runtime_ids


def test_runtime_image_verifier_matches_cri_config_ids_on_both_nodes(
    tmp_path: Path,
) -> None:
    store, ledger, runtime_ids = _runtime_image_test_ledger(tmp_path)
    inventory = json.dumps(
        {
            "images": [
                {"repoTags": [tag], "id": config_id}
                for tag, config_id in runtime_ids.items()
            ]
        }
    )
    runner = FakeRunner(
        [
            harness.CommandResult(
                0,
                f"{ledger['server_container_id']}|{ledger['cluster_name']}|server\n",
            ),
            harness.CommandResult(0, inventory),
            harness.CommandResult(0, inventory),
        ]
    )

    harness.ApplicationE2EHarness(store.root, runner)._verify_imported_node_images(
        ledger
    )

    assert ledger["verified_node_images"] == {"server-0": 9, "agent-0": 9}
    assert all(
        command[-4:] == ["crictl", "images", "-o", "json"]
        for command in runner.commands[1:]
    )


def test_stateless_runtime_image_verifier_includes_workspace_on_both_nodes(
    tmp_path: Path,
) -> None:
    store, ledger, runtime_ids = _runtime_image_test_ledger(
        tmp_path, "stateless-sandbox"
    )
    inventory = json.dumps(
        {
            "images": [
                {"repoTags": [tag], "id": config_id}
                for tag, config_id in runtime_ids.items()
            ]
        }
    )
    runner = FakeRunner(
        [
            harness.CommandResult(
                0,
                f"{ledger['server_container_id']}|{ledger['cluster_name']}|server\n",
            ),
            harness.CommandResult(0, inventory),
            harness.CommandResult(0, inventory),
        ]
    )

    harness.ApplicationE2EHarness(store.root, runner)._verify_imported_node_images(
        ledger
    )

    assert ledger["verified_node_images"] == {"server-0": 10, "agent-0": 10}


def test_runtime_image_verifier_rejects_a_wrong_cri_config_id(
    tmp_path: Path, monkeypatch
) -> None:
    store, ledger, runtime_ids = _runtime_image_test_ledger(tmp_path)
    inventory_items = [
        {"repoTags": [tag], "id": config_id} for tag, config_id in runtime_ids.items()
    ]
    inventory_items[0]["id"] = f"sha256:{'f' * 64}"
    runner = FakeRunner(
        [
            harness.CommandResult(
                0,
                f"{ledger['server_container_id']}|{ledger['cluster_name']}|server\n",
            ),
            harness.CommandResult(0, json.dumps({"images": inventory_items})),
        ]
    )
    moments = iter((0.0, 181.0))
    monkeypatch.setattr(harness.time, "monotonic", lambda: next(moments))
    monkeypatch.setattr(harness.time, "sleep", lambda _seconds: None)

    with pytest.raises(harness.HarnessError, match="inventory did not match"):
        harness.ApplicationE2EHarness(store.root, runner)._verify_imported_node_images(
            ledger
        )


@pytest.mark.parametrize(
    ("image", "expected"),
    [
        ("busybox:1.36", "docker.io/library/busybox:1.36"),
        ("pgvector/pgvector:pg15", "docker.io/pgvector/pgvector:pg15"),
        ("quay.io/keycloak/keycloak:26.2", "quay.io/keycloak/keycloak:26.2"),
        ("localhost:5000/example:1", "localhost:5000/example:1"),
    ],
)
def test_containerd_tag_canonicalization(image: str, expected: str) -> None:
    assert harness.canonical_containerd_tag(image) == expected


def test_playwright_command_uses_pinned_container_and_owned_bridge(
    tmp_path: Path,
) -> None:
    for directory in ("node_modules", "browser-results", "playwright-report"):
        (tmp_path / directory).mkdir()
    env_file = tmp_path / "browser.env"
    harness.write_private_env(env_file, {"APP_E2E_PASSWORD": "secret-value"})

    command = harness.playwright_command(
        env_file=env_file,
        state_dir=tmp_path,
        network="k3d-srw-e2e-owned",
        ingress_ip="172.30.0.3",
        host_gateway="172.30.0.1",
        runner_image="srw-e2e-playwright:test",
        run_id="20260824-123456-ab12cd34",
    )

    version = harness.PLAYWRIGHT_VERSION_FILE.read_text(encoding="utf-8").strip()
    assert version == "1.59.0"
    assert "srw-e2e-playwright:test" in command
    assert ["--network", "k3d-srw-e2e-owned"] == command[
        command.index("--network") : command.index("--network") + 2
    ]
    assert "host.docker.internal:172.30.0.1" in command
    assert "srw-e2e.test:172.30.0.3" in command
    assert "secret-value" not in command
    assert "--workdir" in command
    assert "/work/cockpit" in command
    assert "srw-e2e-browser-20260824-123456-ab12cd34" in command
    assert "process.versions.node" in command[-1]
    assert "npm run test:e2e:app" in command[-1]


def test_attach_network_treats_every_loopback_form_as_the_host() -> None:
    if not harness.sys.platform.startswith("linux"):
        pytest.skip("Docker host networking assertion is Linux-specific")
    assert harness.attach_docker_network("http://localhost") == "host"
    assert harness.attach_docker_network("http://app.localhost") == "host"
    assert harness.attach_docker_network("http://127.0.0.1:8080") == "host"
    assert harness.attach_docker_network("http://[::1]:8080") == "host"
    assert (
        harness.attach_docker_network(
            "https://disposable.example.test", "http://127.0.0.1:9000"
        )
        == "host"
    )
    assert harness.attach_docker_network("https://disposable.example.test") == "bridge"


def test_provider_manifest_substitution_is_exact_and_control_stays_unserviced() -> None:
    image = "srw-e2e-model-fixture:sha-run"
    rendered = harness.render_provider_manifest(image)

    assert harness.PROVIDER_IMAGE_PLACEHOLDER not in rendered
    assert rendered.count(f"image: {image}") == 1
    service = rendered.split("kind: Service", 1)[1]
    assert "port: 8000" in service
    assert "port: 8001" not in service


def test_resource_ledger_accepts_only_exact_uuid_threads() -> None:
    thread_id = "123e4567-e89b-42d3-a456-426614174000"
    document = {
        "schema": 1,
        "run_id": "journey-run",
        "resources": [
            {"kind": "thread", "id": thread_id, "created_at": "2026-08-24T00:00:00Z"}
        ],
        "finalized": False,
    }
    assert harness.ApplicationE2EHarness._resource_thread_ids(document) == [thread_id]

    document["resources"][0]["id"] = "all"
    with pytest.raises(harness.SafetyError, match="invalid thread id"):
        harness.ApplicationE2EHarness._resource_thread_ids(document)


def test_resource_ledger_is_replaceable_only_after_matching_exact_cleanup() -> None:
    thread_id = "123e4567-e89b-42d3-a456-426614174000"
    document = {
        "schema": 1,
        "run_id": "journey-run",
        "resources": [
            {"kind": "thread", "id": thread_id, "created_at": "2026-08-24T00:00:00Z"}
        ],
        "finalized": True,
        "cleanup_complete": False,
    }

    with pytest.raises(harness.SafetyError, match="exact cleanup results"):
        harness.ApplicationE2EHarness._mark_resource_cleanup_complete(document, [])

    harness.ApplicationE2EHarness._mark_resource_cleanup_complete(
        document,
        [{"kind": "thread", "id": thread_id, "status": "404", "forced": "false"}],
    )

    assert document["cleanup_complete"] is True
    assert document["resources"][0]["cleanup_status"] == "verified-absent"
    assert document["resources"][0]["cleaned_at"] == document["cleanup_completed_at"]


@pytest.fixture
def cleanup_clock(monkeypatch: pytest.MonkeyPatch):
    class FakePortForward:
        def __init__(self, **_kwargs):
            self.local_port = 43123

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class Clock:
        now = 0.0

        def sleep(self, seconds):
            self.now += seconds

    clock = Clock()
    monkeypatch.setattr(harness, "PortForward", FakePortForward)
    monkeypatch.setattr(harness.time, "monotonic", lambda: clock.now)
    monkeypatch.setattr(harness.time, "sleep", clock.sleep)
    return clock


def test_exact_cleanup_retries_retryable_force_until_it_converges(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_clock
) -> None:
    responses = iter(
        [
            (409, b""),  # graceful retirement remains busy
            (409, b""),  # force closed admission; cleanup is still converging
            (200, b""),  # same exact forced authority settles on retry
            (404, b""),  # absence proof
        ]
    )
    calls: list[str] = []
    request_timeouts: list[float] = []

    def fake_request(url: str, **kwargs):
        calls.append(url)
        request_timeouts.append(kwargs.get("timeout", 20))
        return next(responses)

    monkeypatch.setenv("APP_E2E_CLEANUP_TIMEOUT_SECONDS", "1")
    monkeypatch.setenv("APP_E2E_FORCE_CLEANUP_TIMEOUT_SECONDS", "3")
    monkeypatch.setattr(harness, "_http_request", fake_request)

    application = harness.ApplicationE2EHarness(tmp_path / "state")
    thread_id = "123e4567-e89b-42d3-a456-426614174000"
    results = application._cleanup_threads(
        {"kubeconfig": str(tmp_path / "kubeconfig.yaml")},
        [thread_id],
        "session=owned",
    )

    assert results == [
        {
            "kind": "thread",
            "id": thread_id,
            "status": "200",
            "forced": "true",
        }
    ]
    assert calls[0].endswith(f"/{thread_id}?permanent=true")
    assert calls[1].endswith(f"/{thread_id}?permanent=true&force=true")
    assert calls[2] == calls[1]
    assert calls[3].endswith(f"/{thread_id}")
    assert request_timeouts[:3] == [1, 3, 1]
    assert cleanup_clock.now == 3


@pytest.mark.parametrize("accepted_status", [200, 202, 204])
def test_exact_cleanup_waits_for_absence_after_accepted_retirement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_clock, accepted_status
) -> None:
    responses = iter(
        [
            (accepted_status, b'{"status":"ending"}'),
            (200, b'{"status":"ending"}'),
            (accepted_status, b'{"status":"ending"}'),
            (404, b""),
        ]
    )
    calls = []

    def fake_request(url: str, **kwargs):
        calls.append((kwargs.get("method", "GET"), url, cleanup_clock.now))
        return next(responses)

    monkeypatch.setattr(harness, "_http_request", fake_request)
    application = harness.ApplicationE2EHarness(tmp_path / "state")
    thread_id = "123e4567-e89b-42d3-a456-426614174000"
    results = application._cleanup_threads(
        {"kubeconfig": str(tmp_path / "kubeconfig.yaml")}, [thread_id], "session=owned"
    )

    path = f"http://127.0.0.1:43123/api/persistent/threads/{thread_id}"
    assert calls == [
        ("DELETE", f"{path}?permanent=true", 0),
        ("GET", path, 0),
        ("DELETE", f"{path}?permanent=true", 2),
        ("GET", path, 2),
    ]
    assert results == [
        {
            "kind": "thread",
            "id": thread_id,
            "status": str(accepted_status),
            "forced": "false",
        }
    ]


def test_exact_cleanup_bounds_pending_retirement_and_fences_force_by_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_clock
) -> None:
    calls = []

    def fake_request(url: str, **kwargs):
        calls.append((kwargs.get("method", "GET"), url, cleanup_clock.now))
        return 200, b'{"status":"ending"}'

    monkeypatch.setenv("APP_E2E_CLEANUP_TIMEOUT_SECONDS", "3")
    monkeypatch.setenv("APP_E2E_FORCE_CLEANUP_TIMEOUT_SECONDS", "3")
    monkeypatch.setattr(harness, "_http_request", fake_request)
    application = harness.ApplicationE2EHarness(tmp_path / "state")
    thread_id = "123e4567-e89b-42d3-a456-426614174000"
    with pytest.raises(
        harness.HarnessError, match="bounded exact-id force cleanup did not settle"
    ):
        application._cleanup_threads(
            {"kubeconfig": str(tmp_path / "kubeconfig.yaml")},
            [thread_id],
            "session=owned",
        )

    path = f"http://127.0.0.1:43123/api/persistent/threads/{thread_id}"
    assert calls == [
        ("DELETE", f"{path}?permanent=true", 0),
        ("GET", path, 0),
        ("DELETE", f"{path}?permanent=true", 2),
        ("GET", path, 2),
        ("DELETE", f"{path}?permanent=true&force=true", 3),
        ("GET", path, 3),
        ("DELETE", f"{path}?permanent=true&force=true", 5),
        ("GET", path, 5),
    ]
    assert cleanup_clock.now == 6


@pytest.mark.parametrize(
    "thread_id", ["all", "../threads", "123e4567-e89b-42d3-a456-426614174000/other"]
)
def test_cleanup_rejects_nonexact_ledger_ids_before_any_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, thread_id: str
) -> None:
    store = harness.StateStore(tmp_path / "state")
    ledger = store.initialize("20260824-123456-ab12cd34")
    harness.write_private_json(
        Path(ledger["run_dir"]) / "browser/browser-resources.json",
        {
            "schema": 1,
            "run_id": "cleanup-unit-run",
            "resources": [{"kind": "thread", "id": thread_id}],
        },
    )
    application = harness.ApplicationE2EHarness(tmp_path / "state")
    monkeypatch.setattr(application, "_assert_owned_cluster", lambda _ledger: None)

    def forbidden_request(*_args, **_kwargs):
        pytest.fail("an invalid resource ledger must never issue a cleanup request")

    monkeypatch.setattr(harness, "_http_request", forbidden_request)
    with pytest.raises(harness.SafetyError, match="invalid thread id"):
        application.cleanup(ledger)


def test_provider_cleanup_waits_for_required_background_work_before_reset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_clock
) -> None:
    run_id = "provider-cleanup-run"
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    ledger = {
        "kubeconfig": str(tmp_path / "kubeconfig.yaml"),
        "run_dir": str(run_dir),
    }
    resource_ledger = {"run_id": run_id}
    states = [
        {
            "pending_calls": 1,
            "remaining_required_responses": 1,
            "unexpected_count": 0,
            "calls": [],
        },
        {
            "pending_calls": 0,
            "remaining_required_responses": 0,
            "unexpected_count": 0,
            "calls": [{"outcome": "success"}],
        },
        {
            "pending_calls": 0,
            "remaining_required_responses": 0,
            "unexpected_count": 0,
            "calls": [{"outcome": "success"}],
        },
    ]
    overview = {
        "runs": [states[-1]],
        "unscoped_unexpected_calls": 0,
        "unscoped_calls_truncated": 0,
        "unscoped_calls": [],
    }
    requests: list[tuple[str, str]] = []

    def fake_request(url: str, **kwargs):
        method = kwargs.get("method", "GET")
        requests.append((method, url))
        if method == "DELETE":
            return 200, b"{}"
        if url.endswith(f"/{run_id}"):
            return 200, json.dumps(states.pop(0)).encode()
        return 200, json.dumps(overview).encode()

    application = harness.ApplicationE2EHarness(tmp_path / "state")
    monkeypatch.setattr(
        application,
        "_load_secrets",
        lambda _ledger: type("Secrets", (), {"provider_control_token": "token"})(),
    )
    monkeypatch.setattr(harness, "_http_request", fake_request)
    monkeypatch.setenv("APP_E2E_PROVIDER_SETTLE_SECONDS", "1")
    application._cleanup_provider_run(ledger, resource_ledger)

    assert requests[-1][0] == "DELETE"
    receipt = json.loads((run_dir / "provider-cleanup-state.json").read_text())
    assert receipt == {"scenario": overview["runs"][0], "overview": overview}


def test_provider_cleanup_refuses_global_unscoped_rejections(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cleanup_clock
) -> None:
    run_id = "provider-unscoped-run"
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    ledger = {
        "kubeconfig": str(tmp_path / "kubeconfig.yaml"),
        "run_dir": str(run_dir),
    }
    settled = {
        "pending_calls": 0,
        "remaining_required_responses": 0,
        "unexpected_count": 0,
        "calls": [{"outcome": "success"}],
    }
    overview = {
        "runs": [settled],
        "unscoped_unexpected_calls": 1,
        "unscoped_calls_truncated": 0,
        "unscoped_calls": [{"outcome": "run_correlation_required"}],
    }
    requests: list[str] = []

    def fake_request(url: str, **kwargs):
        requests.append(kwargs.get("method", "GET"))
        if url.endswith(f"/{run_id}"):
            return 200, json.dumps(settled).encode()
        return 200, json.dumps(overview).encode()

    application = harness.ApplicationE2EHarness(tmp_path / "state")
    monkeypatch.setattr(
        application,
        "_load_secrets",
        lambda _ledger: type("Secrets", (), {"provider_control_token": "token"})(),
    )
    monkeypatch.setattr(harness, "_http_request", fake_request)
    monkeypatch.setenv("APP_E2E_PROVIDER_SETTLE_SECONDS", "0")

    with pytest.raises(harness.HarnessError, match="rejected or unscoped requests"):
        application._cleanup_provider_run(ledger, {"run_id": run_id})
    assert "DELETE" not in requests


def test_exact_cleanup_request_receives_the_full_remaining_lifecycle_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class FakePortForward:
        def __init__(self, **_kwargs):
            self.local_port = 43123

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    responses = iter([(204, b""), (404, b"")])
    request_timeouts: list[float] = []

    def fake_request(_url: str, **kwargs):
        request_timeouts.append(kwargs.get("timeout", 20))
        return next(responses)

    monkeypatch.setattr(harness, "PortForward", FakePortForward)
    monkeypatch.setattr(harness, "_http_request", fake_request)
    monkeypatch.setattr(harness.time, "monotonic", lambda: 100.0)

    application = harness.ApplicationE2EHarness(tmp_path / "state")
    application._cleanup_threads(
        {"kubeconfig": str(tmp_path / "kubeconfig.yaml")},
        ["123e4567-e89b-42d3-a456-426614174000"],
        "session=owned",
    )

    assert request_timeouts == [180, 20]


def test_cookie_header_selects_only_owned_origin() -> None:
    state = {
        "cookies": [
            {"name": "session", "value": "owned", "domain": "srw-e2e.test"},
            {"name": "other", "value": "remote", "domain": "example.com"},
        ]
    }

    assert (
        harness.ApplicationE2EHarness._cookie_header(state, "srw-e2e.test")
        == "session=owned"
    )


def test_credential_cleanup_removes_crash_candidate_and_node_modules(
    tmp_path: Path,
) -> None:
    state_root = tmp_path / "state"
    store = harness.StateStore(state_root)
    ledger = store.initialize("20260824-123456-ab12cd34")
    run_dir = Path(ledger["run_dir"])
    candidate = run_dir / "browser/.auth/journey.json.candidate"
    harness.write_private_json(candidate, {"cookies": []})
    node_modules = run_dir / "node_modules"
    node_modules.mkdir(mode=0o700)
    (node_modules / "package.txt").write_text("test-only", encoding="utf-8")

    harness.ApplicationE2EHarness(state_root)._remove_credentials(ledger)

    assert not candidate.exists()
    assert not node_modules.exists()


def test_image_cleanup_refuses_a_retagged_run_image(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    store = harness.StateStore(state_root)
    ledger = store.initialize("20260824-123456-ab12cd34")
    sha = "a" * 40
    images, _commands = harness.build_image_commands(sha, ledger["run_id"])
    ledger.update({"source_revision": sha, "source_dirty": False, "images": images})
    store.persist(ledger)
    runner = FakeRunner(
        [
            harness.CommandResult(
                0,
                f"sha256:{'b' * 64}|a-different-run|{sha}\n",
            )
        ]
    )

    with pytest.raises(harness.SafetyError, match="mismatched ownership"):
        harness.ApplicationE2EHarness(state_root, runner)._remove_run_images(ledger)

    assert all("rm" not in command for command in runner.commands)


def test_e2e_values_keep_only_required_stack_and_exact_provider_egress() -> None:
    values = yaml.safe_load(harness.VALUES_FILE.read_text(encoding="utf-8"))

    assert values["garage"]["enabled"] is False
    assert values["virtualWorkspace"]["rclone"]["type"] == "memory"
    assert values["workspace"]["accessMode"] == "ReadWriteOnce"
    assert values["workspace"]["pvcEnabled"] is False
    assert values["agent"]["networkPolicy"]["enabled"] is True
    assert values["agent"]["networkPolicy"]["extraEgress"] == [
        {
            "to": [
                {
                    "podSelector": {
                        "matchLabels": {
                            "app.kubernetes.io/name": "srw-e2e-model-fixture"
                        }
                    }
                }
            ],
            "ports": [{"protocol": "TCP", "port": 8000}],
        }
    ]
    for path in (
        ("gitea", "enabled"),
        ("mcp", "enabled"),
        ("opencloud", "enabled"),
        ("nextcloud", "enabled"),
        ("reloader", "enabled"),
    ):
        assert values[path[0]][path[1]] is False
    assert values["databases"]["neo4j"]["enabled"] is False
    models = values["llm"]["seed"]["systemEndpoints"][0]["models"]
    assert models[0]["capabilities"] == ["chat", "auxiliary"]
    assert models[0]["contextWindow"] == 128000
    assert models[1]["capabilities"] == ["embedding"]
    assert models[2]["capabilities"] == ["rerank"]


def test_stateless_sandbox_values_enable_session_executor_and_skill_catalogue() -> None:
    values = yaml.safe_load(
        harness.STATELESS_SANDBOX_VALUES_FILE.read_text(encoding="utf-8")
    )

    assert values == {
        "agent": {
            "skillsDbEnabled": "true",
            "stateless": {
                "enabled": True,
                "replicas": 2,
                "worker": {"enabled": False, "defaultEnabled": False},
            },
        },
        "workspace": {
            "pvcEnabled": True,
            "pvcSize": "1Gi",
            "ephemeralStorageClass": "local-path",
        },
    }


@pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is not installed")
def test_stateless_sandbox_profile_renders_current_executor_and_workspace_images(
    tmp_path: Path,
) -> None:
    sha = "a" * 40
    run_id = "20260824-123456-ab12cd34"
    profile = harness.resolve_profile("stateless-sandbox")
    images, _commands = harness.build_image_commands(
        sha, run_id, include_workspace=True
    )
    image_values = tmp_path / "images.yaml"
    image_values.write_text(
        harness._image_values(images, sha, run_id), encoding="utf-8"
    )
    command = [
        "helm",
        "template",
        "srw-e2e",
        str(harness.REPO_ROOT / "helm"),
        "-n",
        harness.NAMESPACE,
    ]
    for values_file in profile.values_files:
        command.extend(("-f", str(values_file)))
    command.extend(("-f", str(image_values)))
    rendered = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
    ).stdout
    documents = [document for document in yaml.safe_load_all(rendered) if document]
    stateless = next(
        document
        for document in documents
        if document.get("kind") == "Deployment"
        and document.get("metadata", {}).get("name") == "srw-e2e-agent-stateless"
    )
    assert stateless["spec"]["replicas"] == 2
    assert (
        stateless["spec"]["template"]["spec"]["containers"][0]["image"]
        == images["agent"]
    )
    config = next(
        document
        for document in documents
        if document.get("kind") == "ConfigMap"
        and document.get("metadata", {}).get("name") == "srw-e2e-config"
    )
    assert config["data"]["STATELESS_SESSION_ENABLED"] == "true"
    assert config["data"]["STATELESS_WORKER_ENABLED"] == "false"
    assert config["data"]["WORKSPACE_IMAGE"] == images["workspace"]


@pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is not installed")
@pytest.mark.parametrize(
    "profile_name",
    [
        "pinned-virtual",
        "stateless-sandbox",
        "forge-sandbox",
        "cloud-sandbox",
        "officer-watchdog",
        "session-attention",
    ],
)
def test_session_profiles_do_not_render_an_extra_catalog_provider(
    profile_name: str,
) -> None:
    command = [
        "helm",
        "template",
        "srw-e2e",
        str(harness.REPO_ROOT / "helm"),
        "-n",
        harness.NAMESPACE,
    ]
    for values_file in harness.resolve_profile(profile_name).values_files:
        command.extend(("-f", str(values_file)))
    rendered = subprocess.run(
        command, check=True, capture_output=True, text=True, timeout=120
    ).stdout
    documents = [document for document in yaml.safe_load_all(rendered) if document]
    assert not any(
        document.get("metadata", {})
        .get("labels", {})
        .get("app.kubernetes.io/component")
        == "searxng"
        for document in documents
    )
    research_seed = next(
        document
        for document in documents
        if document.get("kind") == "Job"
        and document.get("metadata", {}).get("name") == "srw-e2e-research-provider-seed"
    )
    # A seed-only provider is enough to violate the browser's strict two-model
    # catalogue check, even if its service were omitted from the chart.
    environment = research_seed["spec"]["template"]["spec"]["containers"][0]["env"]
    assert "SEARXNG_BASE_URL" not in {item["name"] for item in environment}


@pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is not installed")
def test_session_attention_profile_renders_a_mail_sink_audit_tier_and_store() -> None:
    """R1.B10's live gate composes the forge profile and adds only its needs.

    SMTP must point at the in-cluster GreenMail sink and nowhere else, the audit
    StatefulSet the harness waits on must render, and so must the object store
    that enables workspace suspension.
    """
    forge = harness.resolve_profile("forge-sandbox")
    profile = harness.resolve_profile("session-attention")
    assert profile.values_files[: len(forge.values_files)] == forge.values_files
    assert profile.values_files[-1] == harness.SESSION_ATTENTION_VALUES_FILE

    command = [
        "helm",
        "template",
        "srw-e2e",
        str(harness.REPO_ROOT / "helm"),
        "-n",
        harness.NAMESPACE,
    ]
    for values_file in profile.values_files:
        command.extend(("-f", str(values_file)))
    rendered = subprocess.run(
        command, check=True, capture_output=True, text=True, timeout=120
    ).stdout
    documents = [document for document in yaml.safe_load_all(rendered) if document]
    names = {
        (document.get("kind"), document.get("metadata", {}).get("name"))
        for document in documents
    }
    for deployment in profile.additional_deployments:
        assert ("Deployment", deployment) in names
    for statefulset in profile.additional_statefulsets:
        assert ("StatefulSet", statefulset) in names
    config = next(
        document
        for document in documents
        if document.get("kind") == "ConfigMap"
        and document.get("metadata", {}).get("name") == "srw-e2e-config"
    )
    assert config["data"]["SMTP_HOST"] == "srw-e2e-greenmail"
    assert config["data"]["SMTP_PORT"] == "3025"
    assert config["data"]["SMTP_USE_TLS"] == "false"


def test_forge_sandbox_profile_composes_the_sandbox_overlay_and_adds_the_forge() -> (
    None
):
    """The forge profile *extends* stateless-sandbox; it never replaces it.

    B03's central lifecycle needs the forge, but its knowledge and citation
    work still needs a workspace-backed session, so both overlays apply and the
    shared baseline is the first file in the list.
    """
    sandbox = harness.resolve_profile("stateless-sandbox")
    forge = harness.resolve_profile("forge-sandbox")

    assert forge.values_files[: len(sandbox.values_files)] == sandbox.values_files
    assert forge.values_files[-1] == harness.FORGE_SANDBOX_VALUES_FILE
    assert forge.values_files[0] == harness.VALUES_FILE
    assert forge.workspace_backend == sandbox.workspace_backend == "sandbox"
    assert forge.execution_lane == sandbox.execution_lane == "stateless"
    assert forge.include_workspace_image is True
    assert forge.stateless_agents is True
    assert forge.forge_enabled is True
    assert forge.additional_deployments == sandbox.additional_deployments
    assert forge.additional_statefulsets == ("srw-e2e-gitea",)
    # The cheap baseline and the sandbox overlay must not have acquired a forge.
    assert harness.resolve_profile("pinned-virtual").forge_enabled is False
    assert sandbox.forge_enabled is False
    assert sandbox.additional_statefulsets == ()
    # And none of the three has acquired a cloud backend.
    for name in ("pinned-virtual", "stateless-sandbox", "forge-sandbox"):
        assert harness.resolve_profile(name).cloud_enabled is False


def test_cloud_sandbox_extends_forge_sandbox_without_replacing_it() -> None:
    """B04's profile *extends* forge-sandbox; it never replaces it.

    B04 needs the workspace-backed stateless session (mounts, staging, End) and
    the forge (repo reads, diff review, export), and adds the one thing neither
    provides: a real main-cloud backend to attest, mount, stage and reload
    against.
    """
    forge = harness.resolve_profile("forge-sandbox")
    cloud = harness.resolve_profile("cloud-sandbox")

    assert cloud.values_files[: len(forge.values_files)] == forge.values_files
    assert cloud.values_files[-1] == harness.CLOUD_SANDBOX_VALUES_FILE
    assert cloud.values_files[0] == harness.VALUES_FILE
    assert cloud.workspace_backend == forge.workspace_backend == "sandbox"
    assert cloud.execution_lane == forge.execution_lane == "stateless"
    assert cloud.include_workspace_image is True
    assert cloud.stateless_agents is True
    assert cloud.forge_enabled is True
    assert cloud.cloud_enabled is True
    # A bound backend is not protected cloud mode. Without the flag every
    # protected route answers "disabled" and the mount builders are never asked
    # for a payload, which is the gap B04's acceptance recorded.
    assert cloud.protected_cloud_enabled is True
    for name in ("pinned-virtual", "stateless-sandbox", "forge-sandbox"):
        assert harness.resolve_profile(name).protected_cloud_enabled is False
    # The forge StatefulSet is inherited and the object store is added: without
    # a blob store a protected session stages nothing and the review surface
    # answers 200 with an empty diff.
    assert set(forge.additional_statefulsets) < set(cloud.additional_statefulsets)
    assert "srw-e2e-garage" in cloud.additional_statefulsets
    # The backend is a Deployment, so it must be waited on as one.
    assert "srw-e2e-nextcloud" in cloud.additional_deployments
    assert set(forge.additional_deployments) <= set(cloud.additional_deployments)


def test_officer_watchdog_extends_forge_sandbox_and_only_flips_reconciliation() -> None:
    """B07's final gate *extends* forge-sandbox; it never replaces it.

    The Officer watchdog's third duty submits a missing runtime to the shared
    durable lifecycle owner, and that owner drops every automatic submission
    while the chart default stands. The overlay exists to flip exactly that
    one decision, so the profile must inherit the forge stack unchanged and
    must not have acquired a cloud backend or protected cloud mode along the
    way.
    """
    forge = harness.resolve_profile("forge-sandbox")
    watchdog = harness.resolve_profile("officer-watchdog")

    assert watchdog.values_files[: len(forge.values_files)] == forge.values_files
    assert watchdog.values_files[-1] == harness.OFFICER_WATCHDOG_VALUES_FILE
    assert watchdog.values_files[0] == harness.VALUES_FILE
    assert watchdog.workspace_backend == forge.workspace_backend == "sandbox"
    assert watchdog.execution_lane == forge.execution_lane == "stateless"
    assert watchdog.include_workspace_image is True
    assert watchdog.stateless_agents is True
    assert watchdog.forge_enabled is True
    assert watchdog.additional_deployments == forge.additional_deployments
    assert watchdog.additional_statefulsets == forge.additional_statefulsets
    # The one behavioural difference, in both directions.
    assert watchdog.persistent_reconciliation_enabled is True
    for name in (
        "pinned-virtual",
        "stateless-sandbox",
        "forge-sandbox",
        "cloud-sandbox",
    ):
        assert harness.resolve_profile(name).persistent_reconciliation_enabled is False
    # And the gate under test did not drag a backend in with it.
    assert watchdog.cloud_enabled is False
    assert watchdog.protected_cloud_enabled is False


@pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is not installed")
def test_officer_watchdog_profile_renders_the_reconciliation_flag(
    tmp_path: Path,
) -> None:
    """The flag must reach the orchestrator, not just the values file.

    The orchestrator Deployment does not ``envFrom`` the shared ConfigMap, so
    a key that renders into the ConfigMap and is never referenced by the
    Deployment would leave the duty just as unobservable as the default.
    """
    sha = "a" * 40
    run_id = "20260824-123456-ab12cd34"
    profile = harness.resolve_profile("officer-watchdog")
    images, _commands = harness.build_image_commands(
        sha, run_id, include_workspace=profile.include_workspace_image
    )
    image_values = tmp_path / "images.yaml"
    image_values.write_text(
        harness._image_values(images, sha, run_id), encoding="utf-8"
    )
    command = [
        "helm",
        "template",
        "srw-e2e",
        str(harness.REPO_ROOT / "helm"),
        "-n",
        harness.NAMESPACE,
    ]
    for values_file in profile.values_files:
        command.extend(("-f", str(values_file)))
    command.extend(("-f", str(image_values)))
    rendered = subprocess.run(
        command, check=True, capture_output=True, text=True, timeout=120
    ).stdout
    documents = [document for document in yaml.safe_load_all(rendered) if document]
    configs = [
        document
        for document in documents
        if document.get("kind") == "ConfigMap"
        and "PERSISTENT_AGENT_RECONCILIATION_ENABLED" in (document.get("data") or {})
    ]
    assert len(configs) == 1
    assert configs[0]["data"]["PERSISTENT_AGENT_RECONCILIATION_ENABLED"] == "true"
    deployments = [
        document
        for document in documents
        if document.get("kind") == "Deployment"
        and document["metadata"]["name"] == "srw-e2e-orchestrator"
    ]
    assert len(deployments) == 1
    referenced = {
        entry["name"]
        for container in deployments[0]["spec"]["template"]["spec"]["containers"]
        for entry in (container.get("env") or [])
        if (entry.get("valueFrom") or {}).get("configMapKeyRef", {}).get("key")
        == "PERSISTENT_AGENT_RECONCILIATION_ENABLED"
    }
    assert referenced == {"PERSISTENT_AGENT_RECONCILIATION_ENABLED"}


@pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is not installed")
def test_cloud_sandbox_renders_exactly_one_bundled_backend(tmp_path: Path) -> None:
    """Nextcloud renders and is the selected backend; OpenCloud does not.

    The chart defaults OpenCloud on and the configmap prefers it over
    Nextcloud, so an overlay that only switched Nextcloud on would deploy a
    second backend and select the wrong one.
    """
    sha = "a" * 40
    run_id = "20260824-123456-ab12cd34"
    profile = harness.resolve_profile("cloud-sandbox")
    images, _commands = harness.build_image_commands(
        sha, run_id, include_workspace=True
    )
    image_values = tmp_path / "images.yaml"
    image_values.write_text(
        harness._image_values(images, sha, run_id), encoding="utf-8"
    )
    command = [
        "helm",
        "template",
        "srw-e2e",
        str(harness.REPO_ROOT / "helm"),
        "-n",
        harness.NAMESPACE,
    ]
    for values_file in profile.values_files:
        command.extend(("-f", str(values_file)))
    command.extend(("-f", str(image_values)))
    rendered = subprocess.run(
        command, check=True, capture_output=True, text=True, timeout=120
    ).stdout
    documents = [document for document in yaml.safe_load_all(rendered) if document]
    names = {
        (document.get("kind"), document.get("metadata", {}).get("name"))
        for document in documents
    }
    assert ("Deployment", "srw-e2e-nextcloud") in names
    assert ("Service", "srw-e2e-nextcloud") in names
    assert not any("opencloud" in (name or "") for _kind, name in names)
    config = next(
        document
        for document in documents
        if document.get("kind") == "ConfigMap"
        and document.get("metadata", {}).get("name") == "srw-e2e-config"
    )
    # Without this the main-cloud endpoints answer "no backend bound" and every
    # mount/stage assertion would be vacuous.
    assert config["data"]["MAIN_CLOUD_BACKEND"] == "nextcloud"
    assert config["data"]["NEXTCLOUD_URL"] == "http://srw-e2e-nextcloud"
    assert "OPENCLOUD_URL" not in config["data"]
    # Filesystem storage: a disposable cluster has no object store.
    backend = next(
        document
        for document in documents
        if document.get("kind") == "Deployment"
        and document.get("metadata", {}).get("name") == "srw-e2e-nextcloud"
    )
    environment = {
        item["name"]: item.get("value")
        for item in backend["spec"]["template"]["spec"]["containers"][0]["env"]
        if "name" in item
    }
    assert "SQLITE_DATABASE" in environment
    assert not any(key.startswith("OBJECTSTORE_S3") for key in environment)


@pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is not installed")
def test_cloud_sandbox_renders_the_protected_effect_lane(tmp_path: Path) -> None:
    """Protected cloud mode is on, and the server lane it needs is deployed.

    ``agent.protectedCloudModeEnabled`` alone is inert: ``nextcloud.py`` answers
    NOT_SUPPORTED on every capability request unless the effect lane is running,
    so the chart derives the lane from the flag on a bundled, internal Nextcloud.
    This asserts the derivation actually happened rather than trusting it, and
    that the effect root is reachable by the orchestrator and by nothing in a
    workspace.
    """
    sha = "a" * 40
    run_id = "20260824-123456-ab12cd34"
    profile = harness.resolve_profile("cloud-sandbox")
    images, _commands = harness.build_image_commands(
        sha, run_id, include_workspace=True
    )
    image_values = tmp_path / "images.yaml"
    image_values.write_text(
        harness._image_values(images, sha, run_id), encoding="utf-8"
    )
    command = [
        "helm",
        "template",
        "srw-e2e",
        str(harness.REPO_ROOT / "helm"),
        "-n",
        harness.NAMESPACE,
    ]
    for values_file in profile.values_files:
        command.extend(("-f", str(values_file)))
    command.extend(("-f", str(image_values)))
    rendered = subprocess.run(
        command, check=True, capture_output=True, text=True, timeout=120
    ).stdout
    documents = [document for document in yaml.safe_load_all(rendered) if document]

    config = next(
        document
        for document in documents
        if document.get("kind") == "ConfigMap"
        and document.get("metadata", {}).get("name") == "srw-e2e-config"
    )
    assert config["data"]["PROTECTED_CLOUD_MODE_ENABLED"] == "true"
    assert config["data"]["NEXTCLOUD_PROTECTED_EFFECT_URL"] == (
        f"http://{harness.PROTECTED_EFFECT_SECRET_NAME}"
    )

    backend = next(
        document
        for document in documents
        if document.get("kind") == "Deployment"
        and document.get("metadata", {}).get("name") == "srw-e2e-nextcloud"
    )
    containers = [
        container["name"]
        for container in backend["spec"]["template"]["spec"]["containers"]
    ]
    assert containers == [
        "nextcloud",
        "nextcloud-protected-effect-fpm",
        "nextcloud-protected-effect-nginx",
    ]

    kinds = {
        (document.get("kind"), document.get("metadata", {}).get("name"))
        for document in documents
    }
    assert ("Service", harness.PROTECTED_EFFECT_SECRET_NAME) in kinds
    # The bundled object store, and the endpoint the chart derives from it.
    assert ("StatefulSet", "srw-e2e-garage") in kinds
    assert config["data"]["S3_ENDPOINT"] == "http://srw-e2e-garage:3900"
    assert ("NetworkPolicy", harness.PROTECTED_EFFECT_SECRET_NAME) in kinds
    # The chart must not create the effect Secret here: `secrets.create` is
    # false on this stack, so the overlay names one and the harness mints it.
    assert ("Secret", harness.PROTECTED_EFFECT_SECRET_NAME) not in kinds

    orchestrator = next(
        document
        for document in documents
        if document.get("kind") == "Deployment"
        and document.get("metadata", {}).get("name") == "srw-e2e-orchestrator"
    )
    environment = {
        item["name"]: item
        for item in orchestrator["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    hmac = environment["NEXTCLOUD_PROTECTED_EFFECT_HMAC_KEY"]["valueFrom"][
        "secretKeyRef"
    ]
    assert hmac["name"] == harness.PROTECTED_EFFECT_SECRET_NAME
    assert hmac["optional"] is False
    # The effect root must never travel in the bundle agent Pods mount whole.
    minted = harness.generated_secret_data(
        harness.SecretBundle.generate(run_id),
        vm_private_key="unused",
        vm_public_key="unused",
    )
    assert "NEXTCLOUD_PROTECTED_EFFECT_HMAC_KEY" not in minted["srw-e2e-app-secrets"]
    assert set(minted[harness.PROTECTED_EFFECT_SECRET_NAME]) == {
        "NEXTCLOUD_PROTECTED_EFFECT_HMAC_KEY"
    }


@pytest.mark.parametrize("profile_name", sorted(harness.APPLICATION_E2E_PROFILES))
def test_generated_app_secret_covers_the_backend_required_secrets(
    profile_name: str,
) -> None:
    """A cloud profile must mint every secret the *application* requires.

    The render check above reads Kubernetes: it can only see a `secretKeyRef`
    the chart marks non-optional. `NEXTCLOUD_AGENT_PASSWORD` is not one of
    those — the orchestrator mounts it `optional: true` — and yet
    `services/cloud/config.py` lists `agent_password` in
    `_REQUIRED_SECRET_ENVS["nextcloud"]`. Without it the main-cloud
    installation authority never initialises, and the backend then reports
    itself *bound* while refusing every effect with "does not support 'durable
    active backend-instance authority'". Nothing fails; project cloud folders,
    user homes and protected mounts simply never exist.

    That is what B04's cloud acceptance actually ran against, so this reads the
    application's own table rather than restating the key by hand.
    """
    profile = harness.resolve_profile(profile_name)
    if not profile.cloud_enabled:
        pytest.skip("profile binds no main-cloud backend")

    from orchestrator.services.cloud.config import _REQUIRED_SECRET_ENVS

    minted = set(
        harness.SecretBundle.generate("20260824-123456-ab12cd34").app_secret_data()
    )
    # The cloud overlay selects the bundled Nextcloud; assert against that
    # backend's own required set rather than a copy of it.
    required = _REQUIRED_SECRET_ENVS["nextcloud"]
    missing = sorted(
        field
        for field, env_vars in required.items()
        if not any(env_var in minted for env_var in env_vars)
    )
    assert not missing, (
        f"profile {profile_name!r} binds nextcloud but mints no secret for "
        f"{missing}; the backend will report itself bound and refuse every "
        "cloud effect"
    )


def test_generated_app_secret_mints_the_ide_credential_root() -> None:
    """Every profile must mint `IDE_CREDENTIAL_KEY`, for the same reason as above.

    The chart mounts it `optional: true` — deliberately, because unset must mean
    "no credential can be derived, so the browser IDE stays contained" and never
    "fall back to an unauthenticated code-server". So the render check two tests
    up cannot see it missing, and neither can Kubernetes: pods start, the API
    answers, and only the IDE lane is quietly dead. `services/ide_proxy.py`
    reports `ide_credential_key_unconfigured` and withholds the URL.

    R1.B05 froze with "the live IDE HTTP/WebSocket proxy is unexercised" as a
    qualification for exactly this reason: its refusals were covered, its success
    path could not be reached on any profile. This asserts the fixture can reach
    it.
    """
    minted = harness.SecretBundle.generate("20260824-123456-ab12cd34").app_secret_data()

    assert "IDE_CREDENTIAL_KEY" in minted, (
        "no profile mints IDE_CREDENTIAL_KEY; the orchestrator will refuse every "
        "browser-IDE URL with ide_credential_key_unconfigured and no pod will "
        "fail to say so"
    )
    # A short root would derive weak per-workspace credentials; the chart's own
    # generator is randAlphaNum(48).
    assert len(minted["IDE_CREDENTIAL_KEY"]) >= 32


def test_profile_overlays_have_no_duplicate_top_level_keys() -> None:
    """A repeated top-level key in an overlay silently drops the first block.

    PyYAML — and Helm — keep the last mapping entry, so appending a second
    ``nextcloud:`` to an overlay that already had one deletes
    ``nextcloud.enabled`` without a word. Nothing else in this suite would
    notice: the render still succeeds, it just deploys the wrong thing.
    """

    for profile in harness.APPLICATION_E2E_PROFILES.values():
        for values_file in profile.values_files:
            document = yaml.safe_load(values_file.read_text(encoding="utf-8"))
            assert isinstance(document, dict)
            text = values_file.read_text(encoding="utf-8")
            top_level = [
                line.split(":", 1)[0]
                for line in text.splitlines()
                if line
                and not line[0].isspace()
                and not line.startswith("#")
                and ":" in line
            ]
            duplicates = sorted({key for key in top_level if top_level.count(key) > 1})
            assert not duplicates, (
                f"{values_file.name} repeats top-level key(s) {duplicates}; "
                "the later block silently replaces the earlier one"
            )


@pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is not installed")
def test_forge_sandbox_renders_the_bundled_forge_without_a_second_database(
    tmp_path: Path,
) -> None:
    """Gitea renders, on sqlite3, with no `srw-giteadb` StatefulSet."""
    sha = "a" * 40
    run_id = "20260824-123456-ab12cd34"
    profile = harness.resolve_profile("forge-sandbox")
    images, _commands = harness.build_image_commands(
        sha, run_id, include_workspace=True
    )
    image_values = tmp_path / "images.yaml"
    image_values.write_text(
        harness._image_values(images, sha, run_id), encoding="utf-8"
    )
    command = [
        "helm",
        "template",
        "srw-e2e",
        str(harness.REPO_ROOT / "helm"),
        "-n",
        harness.NAMESPACE,
    ]
    for values_file in profile.values_files:
        command.extend(("-f", str(values_file)))
    command.extend(("-f", str(image_values)))
    rendered = subprocess.run(
        command, check=True, capture_output=True, text=True, timeout=120
    ).stdout
    documents = [document for document in yaml.safe_load_all(rendered) if document]
    names = {
        (document.get("kind"), document.get("metadata", {}).get("name"))
        for document in documents
    }
    assert ("StatefulSet", "srw-e2e-gitea") in names
    assert ("Service", "srw-e2e-gitea") in names
    assert not any(
        kind == "StatefulSet" and (name or "").endswith("giteadb")
        for kind, name in names
    )
    forge = next(
        document
        for document in documents
        if document.get("kind") == "StatefulSet"
        and document.get("metadata", {}).get("name") == "srw-e2e-gitea"
    )
    environment = {
        item["name"]: item.get("value")
        for item in forge["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert environment["GITEA__database__DB_TYPE"] == "sqlite3"
    config = next(
        document
        for document in documents
        if document.get("kind") == "ConfigMap"
        and document.get("metadata", {}).get("name") == "srw-e2e-config"
    )
    # The orchestrator reaches the forge in-cluster; without this the client
    # reports `Gitea not reachable` and every project falls back.
    assert config["data"]["GITEA_INTERNAL_URL"] == "http://srw-e2e-gitea:3000"
    # Composition is real, not just declared: the sandbox overlay still applies.
    assert config["data"]["STATELESS_SESSION_ENABLED"] == "true"
    assert config["data"]["WORKSPACE_IMAGE"] == images["workspace"]


@pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is not installed")
@pytest.mark.parametrize("profile_name", sorted(harness.APPLICATION_E2E_PROFILES))
def test_generated_app_secret_covers_every_required_rendered_key(
    profile_name: str,
) -> None:
    """Every profile's non-optional secret refs must be keys the harness mints.

    Parametrized over all profiles, not just the baseline. A profile that
    enables a component pulls in that component's `secretKeyRef`s, and a key the
    bundle does not mint is a `CreateContainerConfigError` — which, for anything
    Keycloak-adjacent, stalls every pod that waits on Keycloak, i.e. every pod.
    That is exactly how `cloud-sandbox` first failed: enabling the bundled
    Nextcloud made the Keycloak bootstrap require `NEXTCLOUD_OIDC_CLIENT_SECRET`,
    and the baseline-only render here could not see it.
    """
    profile = harness.resolve_profile(profile_name)
    command = [
        "helm",
        "template",
        "srw-e2e",
        str(harness.REPO_ROOT / "helm"),
        "-n",
        harness.NAMESPACE,
    ]
    for values_file in profile.values_files:
        command.extend(("-f", str(values_file)))
    rendered = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
    ).stdout
    documents = [document for document in yaml.safe_load_all(rendered) if document]
    refs: list[dict] = []

    def walk(value) -> None:
        if isinstance(value, dict):
            if isinstance(value.get("secretKeyRef"), dict):
                refs.append(value["secretKeyRef"])
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    for document in documents:
        walk(document)
    minted = harness.generated_secret_data(
        harness.SecretBundle.generate("20260824-123456-ab12cd34"),
        vm_private_key="unused",
        vm_public_key="unused",
    )
    required_app_keys = {
        ref["key"]
        for ref in refs
        if ref.get("name") == "srw-e2e-app-secrets"
        and ref.get("optional", False) is not True
    }

    # Every required reference, not only the ones aimed at the application
    # bundle. A component whose key lives in a Secret of its own -- the
    # protected-effect root, say -- fails exactly the same way, and filtering on
    # one Secret name cannot see it.
    missing = sorted(
        f"{ref['name']}/{ref['key']}"
        for ref in refs
        if ref.get("optional", False) is not True
        and ref["key"] not in minted.get(ref.get("name"), ())
    )
    assert not missing, (
        f"profile {profile_name!r} renders secret keys the harness never mints: "
        f"{missing}"
    )
    assert {"GITEA_ADMIN_USER", "GITEA_ADMIN_PASSWORD"} <= required_app_keys
    if profile.cloud_enabled:
        assert "NEXTCLOUD_OIDC_CLIENT_SECRET" in required_app_keys
    if profile_name != harness.DEFAULT_PROFILE_NAME:
        return

    shared_config = next(
        document
        for document in documents
        if document.get("kind") == "ConfigMap"
        and document.get("metadata", {}).get("name") == "srw-e2e-config"
    )
    assert shared_config["data"]["PERSISTENT_AGENT_IMAGE_PULL_POLICY"] == "IfNotPresent"

    orchestrator = next(
        document
        for document in documents
        if document.get("kind") == "Deployment"
        and document.get("metadata", {}).get("name") == "srw-e2e-orchestrator"
    )
    env = orchestrator["spec"]["template"]["spec"]["containers"][0]["env"]
    pull_policy = next(
        item for item in env if item.get("name") == "PERSISTENT_AGENT_IMAGE_PULL_POLICY"
    )
    assert pull_policy["valueFrom"]["configMapKeyRef"] == {
        "name": "srw-e2e-config",
        "key": "PERSISTENT_AGENT_IMAGE_PULL_POLICY",
        "optional": True,
    }


def _deploy_chart_fixture(tmp_path: Path) -> tuple[harness.StateStore, dict]:
    store = harness.StateStore(tmp_path / "state")
    ledger = store.initialize("20260824-123456-ab12cd34")
    return store, ledger


def test_deploy_chart_registers_collabora_repo_before_dependency_build(
    tmp_path: Path,
) -> None:
    store, ledger = _deploy_chart_fixture(tmp_path)
    runner = FakeRunner(
        [
            harness.CommandResult(0),
            harness.CommandResult(
                1,
                "",
                "Error: no repository definition for "
                "https://collaboraonline.github.io/online\n",
            ),
        ]
    )

    with pytest.raises(harness.HarnessError, match="Helm dependency build failed"):
        harness.ApplicationE2EHarness(store.root, runner).deploy_chart(ledger)

    assert runner.commands == [
        [
            "helm",
            "repo",
            "add",
            "collabora",
            "https://collaboraonline.github.io/online",
            "--force-update",
        ],
        ["helm", "dependency", "build", str(harness.REPO_ROOT / "helm")],
    ]


def test_deploy_chart_persists_helm_dependency_output_when_the_build_fails(
    tmp_path: Path,
) -> None:
    store, ledger = _deploy_chart_fixture(tmp_path)
    runner = FakeRunner(
        [
            harness.CommandResult(0),
            harness.CommandResult(
                1,
                "",
                "Error: no repository definition for "
                "https://collaboraonline.github.io/online\n",
            ),
        ]
    )

    with pytest.raises(harness.HarnessError):
        harness.ApplicationE2EHarness(store.root, runner).deploy_chart(ledger)

    output = (Path(ledger["run_dir"]) / "helm-dependency-build.txt").read_text(
        encoding="utf-8"
    )
    assert "no repository definition" in output


def test_diagnostics_collects_the_helm_dependency_build_output(
    tmp_path: Path,
) -> None:
    store, ledger = _deploy_chart_fixture(tmp_path)
    run_dir = Path(ledger["run_dir"])
    (run_dir / "helm-dependency-build.txt").write_text(
        "Error: no repository definition for https://collaboraonline.github.io/online\n",
        encoding="utf-8",
    )

    diagnostics_dir = harness.ApplicationE2EHarness(
        store.root, FakeRunner([])
    ).diagnostics(ledger)

    copied = (diagnostics_dir / "helm-dependency-build.txt").read_text(encoding="utf-8")
    assert "no repository definition" in copied
