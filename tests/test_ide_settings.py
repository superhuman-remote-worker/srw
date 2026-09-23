"""Tests for the per-user code-server IDE settings store.

The store persists a user's code-server config (settings.json, keybindings.json,
snippets) in ``users.settings['ide']['files']`` and reconciles config pulled from
workspaces using filesystem mtimes (newest wins, per file).

The fake DB here faithfully replicates the real
``PostgresDB.update_user_settings`` semantics — a SHALLOW top-level JSONB
``||`` merge with ``jsonb_strip_nulls`` — because the store's correctness hinges
on not clobbering sibling keys through that shallow merge.

Also covers the honest-rc fix (§C1d, knowledge-base/knowledge/features/workspace_durability_tiering.md)
for this module's OWN ``tar | zstd`` (capture, ``_ssh_tar_to_file``) and
``zstd -d | tar`` (extract, ``_ssh_untar_from_file``) SSH commands — the third
masking site after C1b (``snapshot_service.py``) and C1c (``ssh_helpers.py``):
a shell pipeline only reports its LAST stage's exit code, so an upstream
failure is hidden unless the remote command is wrapped in ``bash -c`` with a
PIPESTATUS-discriminated verdict. Follows the C1b/C1c real-bash pattern in
``tests/test_snapshot_ssh_extraction.py`` — mocked-subprocess tests can't
catch shell bugs in the verdict tail itself.
"""

import asyncio
import base64
import json
import shlex
import shutil
import subprocess
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.services.ide_settings import (
    CODE_SERVER_USER_DIR,
    GLOBAL_STORAGE_DIR,
    IdeSettingsStore,
    OpenVsxClassifier,
    _ssh_tar_to_file,
    _ssh_untar_from_file,
    build_extension_install_script,
    build_extensions_list_script,
    build_seed_script,
    build_signature_script,
    capture_safe_workspaces,
    capture_ide_profile,
    evict_dead_workspaces,
    is_kubernetes_capture_context,
    parse_extensions_list,
    parse_signature,
    parse_pull_output,
    pull_ide_config,
    reconcile_extensions,
    reconcile_ide_settings,
    resolve_ssh_target,
    seed_ide_config_for_user,
)


def _strip_nulls(obj):
    """Mirror jsonb_strip_nulls: recursively drop keys whose value is null."""
    if isinstance(obj, dict):
        return {k: _strip_nulls(v) for k, v in obj.items() if v is not None}
    if isinstance(obj, list):
        return [_strip_nulls(v) for v in obj]
    return obj


class FakeSettingsDB:
    """In-memory stand-in replicating get/update_user_settings semantics."""

    def __init__(self, initial=None):
        self._settings = {uid: dict(s) for uid, s in (initial or {}).items()}

    async def get_user_settings(self, user_id: str):
        # Real layer returns a fresh dict each call; emulate by copying.
        import copy

        return copy.deepcopy(self._settings.get(user_id, {}))

    async def update_user_settings(self, user_id: str, settings: dict) -> bool:
        # SHALLOW top-level merge (``||``) then strip nulls — exactly the
        # behaviour of the real SQL ``COALESCE(settings,'{}') || $2``.
        current = self._settings.get(user_id, {})
        merged = {**current, **settings}
        self._settings[user_id] = _strip_nulls(merged)
        return True


UID = "11111111-1111-1111-1111-111111111111"


def _f(content, mtime):
    return {"content": content, "mtime": mtime}


class TestApplyPulledFiles:
    @pytest.mark.asyncio
    async def test_stores_files_when_none_exist(self):
        db = FakeSettingsDB()
        store = IdeSettingsStore(db)

        updated = await store.apply_pulled_files(
            UID, {"settings.json": _f('{"theme":"dracula"}', 100.0)}
        )

        assert updated == ["settings.json"]
        files = await store.get_ide_files(UID)
        assert files["settings.json"] == _f('{"theme":"dracula"}', 100.0)

    @pytest.mark.asyncio
    async def test_newer_mtime_overwrites(self):
        db = FakeSettingsDB(
            {UID: {"ide": {"files": {"settings.json": _f("old", 100.0)}}}}
        )
        store = IdeSettingsStore(db)

        updated = await store.apply_pulled_files(
            UID, {"settings.json": _f("new", 200.0)}
        )

        assert updated == ["settings.json"]
        files = await store.get_ide_files(UID)
        assert files["settings.json"] == _f("new", 200.0)

    @pytest.mark.asyncio
    async def test_older_or_equal_mtime_is_skipped(self):
        db = FakeSettingsDB(
            {UID: {"ide": {"files": {"settings.json": _f("current", 200.0)}}}}
        )
        store = IdeSettingsStore(db)

        older = await store.apply_pulled_files(
            UID, {"settings.json": _f("stale", 150.0)}
        )
        equal = await store.apply_pulled_files(
            UID, {"settings.json": _f("same", 200.0)}
        )

        assert older == []
        assert equal == []
        files = await store.get_ide_files(UID)
        assert files["settings.json"] == _f("current", 200.0)

    @pytest.mark.asyncio
    async def test_newest_wins_regardless_of_application_order(self):
        # Two workspaces report the same file at different mtimes. Whichever
        # order the reconciler applies them, the newest must win.
        db1 = FakeSettingsDB()
        db2 = FakeSettingsDB()
        s1, s2 = IdeSettingsStore(db1), IdeSettingsStore(db2)

        await s1.apply_pulled_files(UID, {"settings.json": _f("v100", 100.0)})
        await s1.apply_pulled_files(UID, {"settings.json": _f("v200", 200.0)})

        await s2.apply_pulled_files(UID, {"settings.json": _f("v200", 200.0)})
        await s2.apply_pulled_files(UID, {"settings.json": _f("v100", 100.0)})

        assert (await s1.get_ide_files(UID))["settings.json"] == _f("v200", 200.0)
        assert (await s2.get_ide_files(UID))["settings.json"] == _f("v200", 200.0)

    @pytest.mark.asyncio
    async def test_updating_one_file_preserves_siblings(self):
        # The shallow-merge clobber guard: writing settings.json must not drop
        # keybindings.json already in the store.
        db = FakeSettingsDB(
            {UID: {"ide": {"files": {"keybindings.json": _f("kb", 50.0)}}}}
        )
        store = IdeSettingsStore(db)

        await store.apply_pulled_files(UID, {"settings.json": _f("st", 100.0)})

        files = await store.get_ide_files(UID)
        assert files["keybindings.json"] == _f("kb", 50.0)
        assert files["settings.json"] == _f("st", 100.0)

    @pytest.mark.asyncio
    async def test_preserves_unrelated_top_level_and_ide_keys(self):
        db = FakeSettingsDB(
            {
                UID: {
                    "profile": {"nickname": "ada"},
                    "ide": {"version": 1, "files": {}},
                }
            }
        )
        store = IdeSettingsStore(db)

        await store.apply_pulled_files(UID, {"settings.json": _f("st", 100.0)})

        full = await db.get_user_settings(UID)
        assert full["profile"] == {"nickname": "ada"}  # unrelated top-level key kept
        assert full["ide"]["version"] == 1  # sibling ide sub-key kept
        assert full["ide"]["files"]["settings.json"] == _f("st", 100.0)

    @pytest.mark.asyncio
    async def test_multiple_files_in_one_call(self):
        db = FakeSettingsDB()
        store = IdeSettingsStore(db)

        updated = await store.apply_pulled_files(
            UID,
            {
                "settings.json": _f("st", 100.0),
                "snippets/python.json": _f("sn", 90.0),
            },
        )

        assert set(updated) == {"settings.json", "snippets/python.json"}
        files = await store.get_ide_files(UID)
        assert files["snippets/python.json"] == _f("sn", 90.0)

    @pytest.mark.asyncio
    async def test_empty_pull_is_noop(self):
        db = FakeSettingsDB(
            {UID: {"ide": {"files": {"settings.json": _f("st", 100.0)}}}}
        )
        store = IdeSettingsStore(db)

        updated = await store.apply_pulled_files(UID, {})

        assert updated == []
        assert (await store.get_ide_files(UID))["settings.json"] == _f("st", 100.0)


def _wire(name: str, mtime: int, content: str) -> str:
    """Render one file's section of the remote pull script's stdout."""
    b64 = base64.b64encode(content.encode("utf-8")).decode("ascii")
    return f"__SRWFILE__{name}\t{mtime}\n{b64}\n__SRWEND__\n"


class TestParsePullOutput:
    def test_empty_output_is_empty_dict(self):
        assert parse_pull_output("") == {}

    def test_parses_single_file(self):
        out = _wire("settings.json", 1716800000, '{"workbench.colorTheme":"Dracula"}')
        parsed = parse_pull_output(out)
        assert parsed == {
            "settings.json": {
                "content": '{"workbench.colorTheme":"Dracula"}',
                "mtime": 1716800000.0,
            }
        }

    def test_parses_multiple_files_and_preserves_snippet_paths(self):
        out = (
            _wire("settings.json", 100, "S")
            + _wire("keybindings.json", 110, "K")
            + _wire("snippets/python.json", 90, "P")
        )
        parsed = parse_pull_output(out)
        assert set(parsed) == {
            "settings.json",
            "keybindings.json",
            "snippets/python.json",
        }
        assert parsed["snippets/python.json"] == {"content": "P", "mtime": 90.0}

    def test_preserves_multiline_content_with_newlines(self):
        content = '{\n  "a": 1,\n  "b": 2\n}\n'
        out = _wire("settings.json", 5, content)
        parsed = parse_pull_output(out)
        assert parsed["settings.json"]["content"] == content

    def test_mtime_is_float(self):
        out = _wire("settings.json", 42, "x")
        assert isinstance(parse_pull_output(out)["settings.json"]["mtime"], float)


class TestResolveSshTarget:
    def test_container_uses_pod_ip_and_port(self):
        ctx = {
            "workspace_container": {
                "status": "ready",
                "pod_ip": "10.0.0.5",
                "port": 30022,
            }
        }
        assert resolve_ssh_target(ctx) == ("10.0.0.5", 30022)

    def test_vm_uses_ssh_host_and_ssh_port(self):
        ctx = {"vm": {"status": "ready", "ssh_host": "100.64.0.3", "ssh_port": 22}}
        assert resolve_ssh_target(ctx) == ("100.64.0.3", 22)

    def test_ide_session_uses_pod_ip(self):
        ctx = {"ide_session": {"status": "active", "pod_ip": "10.0.0.9"}}
        host, port = resolve_ssh_target(ctx)
        assert host == "10.0.0.9"

    def test_container_preferred_over_vm(self):
        ctx = {
            "workspace_container": {
                "status": "ready",
                "pod_ip": "10.0.0.5",
                "port": 30022,
            },
            "vm": {"status": "ready", "ssh_host": "100.64.0.3"},
        }
        assert resolve_ssh_target(ctx) == ("10.0.0.5", 30022)

    def test_no_target_returns_none(self):
        assert resolve_ssh_target({}) is None
        assert resolve_ssh_target({"workspace_container": {}}) is None

    def test_stable_service_dns_preferred_over_pod_ip(self):
        """A restarted workspace pod keeps its Service name, not its IP —
        dialing the stale IP was the sweeper's residual 'No route to host'
        source. The stable DNS must win when both are present."""
        ctx = {
            "workspace_container": {
                "status": "ready",
                "host": "workspace-965b0935-309.srw.svc.cluster.local",
                "pod_ip": "10.42.0.99",
                "port": 30022,
            }
        }
        assert resolve_ssh_target(ctx) == (
            "workspace-965b0935-309.srw.svc.cluster.local",
            30022,
        )

    def test_legacy_pod_ip_only_row_still_resolves(self):
        ctx = {
            "workspace_container": {
                "status": "ready",
                "pod_ip": "10.0.0.5",
                "port": 30022,
            }
        }
        assert resolve_ssh_target(ctx) == ("10.0.0.5", 30022)

    def test_host_only_row_uses_default_port(self):
        ctx = {
            "workspace_container": {
                "status": "ready",
                "host": "workspace-abc.srw.svc.cluster.local",
            }
        }
        host, port = resolve_ssh_target(ctx)
        assert host == "workspace-abc.srw.svc.cluster.local"
        assert port > 0


class TestKubernetesCaptureContainment:
    def test_kubernetes_rows_require_exact_capture_authority(self):
        current = {
            "workspace_container": {
                "provisioner": "k8s",
                "pod_ip": "10.0.0.5",
            }
        }
        legacy = {"workspace_container": {"pod_ip": "10.0.0.6"}}

        assert is_kubernetes_capture_context(current)
        assert is_kubernetes_capture_context(legacy)
        assert (
            capture_safe_workspaces(
                [
                    {"id": "current", "context": current},
                    {"id": "legacy", "context": legacy},
                ]
            )
            == []
        )

    def test_explicit_non_kubernetes_rows_remain_capture_candidates(self):
        docker = {
            "id": "docker",
            "context": {
                "workspace_container": {
                    "provisioner": "docker",
                    "host": "workspace-1",
                }
            },
        }
        vm = {
            "id": "vm",
            "context": {
                "config_override": {"workspace": {"backend": "vm"}},
                "workspace_container": {
                    "provisioner": "k8s",
                    "pod_ip": "10.0.0.5",
                },
                "vm": {"status": "ready", "ssh_host": "192.0.2.8"},
            },
        }

        assert capture_safe_workspaces([docker, vm]) == [docker, vm]


class TestBuildSeedScript:
    def test_empty_files_is_noop_script(self):
        # No files → nothing to write; must not emit destructive commands.
        script = build_seed_script({})
        assert "base64 -d" not in script

    def test_writes_content_path_mtime_and_chown(self):
        files = {"settings.json": {"content": '{"a":1}', "mtime": 1716800000.0}}
        script = build_seed_script(files)
        b64 = base64.b64encode('{"a":1}'.encode()).decode("ascii")

        assert b64 in script  # content delivered base64 (binary-safe)
        assert f"{CODE_SERVER_USER_DIR}/settings.json" in script
        assert "touch -d @1716800000.0" in script
        assert f"chown -R agent-host:agent-host {CODE_SERVER_USER_DIR}" in script

    def test_creates_parent_dir_for_snippets(self):
        files = {"snippets/python.json": {"content": "x", "mtime": 5.0}}
        script = build_seed_script(files)
        assert f"mkdir -p {CODE_SERVER_USER_DIR}/snippets" in script

    def test_single_quotes_in_name_do_not_break_out(self):
        # A snippet filename with a quote must be shell-escaped, not injected.
        files = {"snippets/a'b.json": {"content": "x", "mtime": 1.0}}
        script = build_seed_script(files)
        assert "rm -rf" not in script  # sanity: nothing injected
        assert "'\\''" in script  # the POSIX single-quote escape is present


class TestPullIdeConfig:
    @pytest.mark.asyncio
    async def test_parses_runner_stdout(self):
        out = _wire("settings.json", 100, "S")

        async def fake_runner(host, port, script, key_path=None, timeout=20):
            return 0, out, b""

        result = await pull_ide_config("10.0.0.5", 30022, _runner=fake_runner)
        assert result == {"settings.json": {"content": "S", "mtime": 100.0}}

    @pytest.mark.asyncio
    async def test_nonzero_exit_returns_empty(self):
        async def fake_runner(host, port, script, key_path=None, timeout=20):
            return 255, b"", b"ssh: connect timeout"

        result = await pull_ide_config("10.0.0.5", 30022, _runner=fake_runner)
        assert result == {}

    @pytest.mark.asyncio
    async def test_runner_exception_returns_empty(self):
        async def fake_runner(host, port, script, key_path=None, timeout=20):
            raise OSError("boom")

        result = await pull_ide_config("10.0.0.5", 30022, _runner=fake_runner)
        assert result == {}


class TestReconcileIdeSettings:
    @pytest.mark.asyncio
    async def test_pulls_each_workspace_and_applies(self):
        db = FakeSettingsDB()
        store = IdeSettingsStore(db)
        calls = []

        async def pull_fn(host, port):
            calls.append((host, port))
            return {"settings.json": _f("theme", 100.0)}

        workspaces = [
            {
                "user_id": UID,
                "context": {
                    "workspace_container": {
                        "status": "ready",
                        "provisioner": "docker",
                        "pod_ip": "10.0.0.5",
                        "port": 30022,
                    }
                },
            }
        ]
        count = await reconcile_ide_settings(store, workspaces, pull_fn)

        assert calls == [("10.0.0.5", 30022)]
        assert count == 1
        assert (await store.get_ide_files(UID))["settings.json"] == _f("theme", 100.0)

    @pytest.mark.asyncio
    async def test_newest_across_two_workspaces_same_user(self):
        db = FakeSettingsDB()
        store = IdeSettingsStore(db)

        async def pull_fn(host, port):
            return (
                {"settings.json": _f("old", 100.0)}
                if host == "10.0.0.1"
                else {"settings.json": _f("new", 200.0)}
            )

        workspaces = [
            {
                "user_id": UID,
                "context": {
                    "workspace_container": {
                        "status": "ready",
                        "provisioner": "docker",
                        "pod_ip": "10.0.0.2",
                        "port": 30022,
                    }
                },
            },
            {
                "user_id": UID,
                "context": {
                    "workspace_container": {
                        "status": "ready",
                        "provisioner": "docker",
                        "pod_ip": "10.0.0.1",
                        "port": 30022,
                    }
                },
            },
        ]
        await reconcile_ide_settings(store, workspaces, pull_fn)

        assert (await store.get_ide_files(UID))["settings.json"] == _f("new", 200.0)

    @pytest.mark.asyncio
    async def test_skips_workspace_without_target(self):
        store = IdeSettingsStore(FakeSettingsDB())
        called = False

        async def pull_fn(host, port):
            nonlocal called
            called = True
            return {}

        await reconcile_ide_settings(store, [{"user_id": UID, "context": {}}], pull_fn)
        assert called is False

    @pytest.mark.asyncio
    async def test_skips_rows_without_user_id(self):
        store = IdeSettingsStore(FakeSettingsDB())

        async def pull_fn(host, port):
            raise AssertionError("should not pull for row without user_id")

        await reconcile_ide_settings(
            store,
            [
                {
                    "context": {
                        "workspace_container": {
                            "status": "ready",
                            "pod_ip": "x",
                            "port": 30022,
                        }
                    }
                }
            ],
            pull_fn,
        )

    @pytest.mark.asyncio
    async def test_empty_pull_does_not_write(self):
        db = FakeSettingsDB(
            {UID: {"ide": {"files": {"settings.json": _f("keep", 5.0)}}}}
        )
        store = IdeSettingsStore(db)

        async def pull_fn(host, port):
            return {}

        count = await reconcile_ide_settings(
            store,
            [{"user_id": UID, "context": {"vm": {"status": "ready", "ssh_host": "h"}}}],
            pull_fn,
        )
        assert count == 0
        assert (await store.get_ide_files(UID))["settings.json"] == _f("keep", 5.0)

    @pytest.mark.asyncio
    async def test_string_context_is_parsed(self):
        db = FakeSettingsDB()
        store = IdeSettingsStore(db)

        async def pull_fn(host, port):
            return {"settings.json": _f("t", 9.0)}

        ws = [
            {
                "user_id": UID,
                "context": '{"workspace_container": {"status": "ready", "provisioner": "docker", "pod_ip": "10.0.0.5", "port": 30022}}',
            }
        ]
        count = await reconcile_ide_settings(store, ws, pull_fn)
        assert count == 1

    @pytest.mark.asyncio
    async def test_one_failing_pull_does_not_abort_others(self):
        db = FakeSettingsDB()
        store = IdeSettingsStore(db)

        async def pull_fn(host, port):
            if host == "bad":
                raise OSError("boom")
            return {"settings.json": _f("t", 9.0)}

        workspaces = [
            {"user_id": UID, "context": {"vm": {"status": "ready", "ssh_host": "bad"}}},
            {
                "user_id": UID,
                "context": {
                    "workspace_container": {
                        "status": "ready",
                        "provisioner": "docker",
                        "pod_ip": "good",
                        "port": 30022,
                    }
                },
            },
        ]
        count = await reconcile_ide_settings(store, workspaces, pull_fn)
        assert count == 1


class TestSeedIdeConfigForUser:
    @pytest.mark.asyncio
    async def test_no_user_id_is_noop(self):
        called = False

        async def runner(host, port, script, key_path=None, timeout=20):
            nonlocal called
            called = True
            return 0, b"", b""

        ok = await seed_ide_config_for_user(
            FakeSettingsDB(), None, "h", 22, _runner=runner
        )
        assert ok is True
        assert called is False

    @pytest.mark.asyncio
    async def test_no_stored_files_is_noop(self):
        called = False

        async def runner(host, port, script, key_path=None, timeout=20):
            nonlocal called
            called = True
            return 0, b"", b""

        ok = await seed_ide_config_for_user(
            FakeSettingsDB(), UID, "h", 22, _runner=runner
        )
        assert ok is True
        assert called is False

    @pytest.mark.asyncio
    async def test_seeds_stored_files(self):
        db = FakeSettingsDB(
            {UID: {"ide": {"files": {"settings.json": _f('{"t":1}', 100.0)}}}}
        )
        scripts = []

        async def runner(host, port, script, key_path=None, timeout=20):
            scripts.append(script)
            return 0, b"", b""

        ok = await seed_ide_config_for_user(db, UID, "10.0.0.5", 30022, _runner=runner)
        assert ok is True
        assert len(scripts) == 1
        assert base64.b64encode('{"t":1}'.encode()).decode() in scripts[0]


class TestGetIdeFiles:
    @pytest.mark.asyncio
    async def test_returns_empty_when_unset(self):
        store = IdeSettingsStore(FakeSettingsDB())
        assert await store.get_ide_files(UID) == {}

    @pytest.mark.asyncio
    async def test_returns_empty_when_ide_present_but_no_files(self):
        db = FakeSettingsDB({UID: {"ide": {"version": 1}}})
        store = IdeSettingsStore(db)
        assert await store.get_ide_files(UID) == {}


class TestExtensionList:
    def test_parses_id_version_and_theme_flag(self):
        out = (
            "monokai.theme-monokai-pro-vscode@2.0.13\tTHEME\n"
            "ms-python.python@2024.4.1\t-\n"
            "garbage-line-no-tab\n"
        )
        result = parse_extensions_list(out)
        assert result == {
            "monokai.theme-monokai-pro-vscode": {"version": "2.0.13", "theme": True},
            "ms-python.python": {"version": "2024.4.1", "theme": False},
        }

    def test_empty_output_is_empty_dict(self):
        assert parse_extensions_list("") == {}

    def test_list_script_targets_extensions_dir(self):
        script = build_extensions_list_script()
        assert "/var/lib/code-server/extensions" in script
        assert "package.json" in script


class TestOpenVsxClassifier:
    @pytest.mark.asyncio
    async def test_available_returns_openvsx(self):
        calls = []

        async def fake_fetch(url):
            calls.append(url)
            return 200  # Open VSX has it

        clf = OpenVsxClassifier(fetch=fake_fetch)
        src = await clf.classify("monokai.theme-monokai-pro-vscode", "2.0.13")
        assert src == "openvsx"
        assert "monokai/theme-monokai-pro-vscode/2.0.13" in calls[0]

    @pytest.mark.asyncio
    async def test_missing_returns_bytes(self):
        async def fake_fetch(url):
            return 404

        clf = OpenVsxClassifier(fetch=fake_fetch)
        assert await clf.classify("acme.private", "1.0.0") == "bytes"

    @pytest.mark.asyncio
    async def test_result_is_cached(self):
        n = {"calls": 0}

        async def fake_fetch(url):
            n["calls"] += 1
            return 200

        clf = OpenVsxClassifier(fetch=fake_fetch)
        await clf.classify("a.b", "1.0.0")
        await clf.classify("a.b", "1.0.0")
        assert n["calls"] == 1  # cached by (id, version)

    @pytest.mark.asyncio
    async def test_fetch_error_defaults_to_bytes(self):
        async def boom(url):
            raise RuntimeError("network down")

        clf = OpenVsxClassifier(fetch=boom)
        assert await clf.classify("a.b", "1.0.0") == "bytes"


class TestExtensionStore:
    @pytest.mark.asyncio
    async def test_apply_then_get_roundtrip(self):
        db = FakeSettingsDB()
        store = IdeSettingsStore(db)
        items = {
            "monokai.theme-monokai-pro-vscode": {
                "version": "2.0.13",
                "source": "openvsx",
                "theme": True,
            }
        }
        changed = await store.apply_extensions(UID, items)
        assert changed == ["monokai.theme-monokai-pro-vscode"]
        got = await store.get_extensions(UID)
        assert got["monokai.theme-monokai-pro-vscode"]["version"] == "2.0.13"

    @pytest.mark.asyncio
    async def test_newer_version_wins_union_across_calls(self):
        db = FakeSettingsDB()
        store = IdeSettingsStore(db)
        await store.apply_extensions(
            UID, {"a.b": {"version": "1.0.0", "source": "openvsx", "theme": False}}
        )
        # second workspace has a.b older + a new extension c.d
        await store.apply_extensions(
            UID,
            {
                "a.b": {"version": "0.9.0", "source": "openvsx", "theme": False},
                "c.d": {"version": "3.1.0", "source": "openvsx", "theme": False},
            },
        )
        got = await store.get_extensions(UID)
        assert got["a.b"]["version"] == "1.0.0"  # newer kept (union, not clobber)
        assert got["c.d"]["version"] == "3.1.0"  # new one added

    @pytest.mark.asyncio
    async def test_apply_preserves_sibling_files_subtree(self):
        db = FakeSettingsDB({UID: {"ide": {"files": {"settings.json": _f("x", 1.0)}}}})
        store = IdeSettingsStore(db)
        await store.apply_extensions(
            UID, {"a.b": {"version": "1.0.0", "source": "openvsx", "theme": False}}
        )
        files = await store.get_ide_files(UID)
        assert files["settings.json"] == _f("x", 1.0)  # not clobbered by shallow merge


class TestExtensionInstallScript:
    def test_empty_is_noop(self):
        assert build_extension_install_script({}) == "exit 0\n"

    def test_only_openvsx_items_installed(self):
        items = {
            "monokai.theme-monokai-pro-vscode": {
                "version": "2.0.13",
                "source": "openvsx",
                "theme": True,
            },
            "ms-python.python": {
                "version": "2024.4.1",
                "source": "openvsx",
                "theme": False,
            },
            "acme.private": {"version": "1.0.0", "source": "bytes", "theme": False},
        }
        script = build_extension_install_script(items)
        assert "monokai.theme-monokai-pro-vscode@2.0.13" in script
        assert "ms-python.python@2024.4.1" in script
        assert "acme.private" not in script  # bytes source not installed here
        assert "--extensions-dir /var/lib/code-server/extensions" in script
        # theme installs synchronously before the backgrounded block
        theme_idx = script.index("monokai.theme-monokai-pro-vscode")
        bg_idx = script.index("ms-python.python")
        assert theme_idx < bg_idx


class TestReconcileExtensions:
    @pytest.mark.asyncio
    async def test_lists_classifies_and_stores(self):
        db = FakeSettingsDB()
        store = IdeSettingsStore(db)
        workspaces = [
            {
                "user_id": UID,
                "context": {
                    "workspace_container": {
                        "provisioner": "docker",
                        "pod_ip": "10.0.0.1",
                    }
                },
            }
        ]

        async def list_fn(host, port):
            assert (host, port) == ("10.0.0.1", 30022)
            return {"a.b": {"version": "1.0.0", "theme": True}}

        class FakeClf:
            async def classify(self, ext_id, version):
                return "openvsx"

        n = await reconcile_extensions(store, workspaces, list_fn, FakeClf())
        assert n == 1
        got = await store.get_extensions(UID)
        assert got["a.b"] == {"version": "1.0.0", "source": "openvsx", "theme": True}

    @pytest.mark.asyncio
    async def test_unreachable_workspace_skipped(self):
        db = FakeSettingsDB()
        store = IdeSettingsStore(db)
        workspaces = [
            {"user_id": UID, "context": {"workspace_container": {"pod_ip": "10.0.0.1"}}}
        ]

        async def list_fn(host, port):
            raise RuntimeError("ssh refused")

        class FakeClf:
            async def classify(self, ext_id, version):
                return "openvsx"

        assert await reconcile_extensions(store, workspaces, list_fn, FakeClf()) == 0


class TestSeedForUserInstallsExtensions:
    @pytest.mark.asyncio
    async def test_seeds_files_and_installs_openvsx_extensions(self):
        db = FakeSettingsDB(
            {
                UID: {
                    "ide": {
                        "files": {
                            "settings.json": _f(
                                '{"workbench.colorTheme":"Monokai Pro"}', 5.0
                            )
                        },
                        "extensions": {
                            "items": {
                                "monokai.theme-monokai-pro-vscode": {
                                    "version": "2.0.13",
                                    "source": "openvsx",
                                    "theme": True,
                                }
                            }
                        },
                    }
                }
            }
        )
        scripts = []

        async def fake_runner(host, port, script, key_path=None, timeout=20):
            scripts.append(script)
            return 0, b"", b""

        ok = await seed_ide_config_for_user(
            db, UID, "10.0.0.5", 30022, _runner=fake_runner
        )
        assert ok is True
        joined = "\n".join(scripts)
        assert "settings.json" in joined  # files seeded
        assert (
            "monokai.theme-monokai-pro-vscode@2.0.13" in joined
        )  # extension installed


class TestSignature:
    def test_script_covers_extensions_and_globalstorage(self):
        s = build_signature_script()
        assert "/var/lib/code-server/extensions" in s
        assert "/var/lib/code-server/User/globalStorage" in s
        assert "sha256sum" in s

    def test_parse_takes_first_token(self):
        assert parse_signature("abc123  -\n") == "abc123"
        assert parse_signature("") == ""


async def _capture_remote_cmd(tmp_path, remote_path="/some/remote/path") -> str:
    """Drive ``_ssh_tar_to_file`` against a mocked subprocess and return the
    exact ``remote`` command string it built — so tests assert against the
    real construction instead of a hand-duplicated copy that could drift."""
    fake = MagicMock()
    fake.returncode = 0
    fake.stdout = MagicMock()
    fake.stdout.read = AsyncMock(side_effect=[b"tarball-bytes", b""])
    fake.wait = AsyncMock(return_value=None)
    local = tmp_path / "capture.tar.zst"
    with patch(
        "asyncio.create_subprocess_exec", new=AsyncMock(return_value=fake)
    ) as mock_exec:
        await _ssh_tar_to_file("10.0.0.5", 30022, remote_path, str(local), key_path="")
    return mock_exec.call_args.args[-1]


async def _untar_remote_cmd(tmp_path) -> str:
    """Drive ``_ssh_untar_from_file`` against a mocked subprocess and return
    the exact ``remote`` command string it built."""
    local = tmp_path / "extract.tar.zst"
    local.write_bytes(b"fake-archive-bytes")
    fake = MagicMock()
    fake.returncode = 0
    fake.stdin = MagicMock()
    fake.stdin.write = MagicMock()
    fake.stdin.drain = AsyncMock(return_value=None)
    fake.stdin.close = MagicMock()
    fake.wait = AsyncMock(return_value=None)
    with patch(
        "asyncio.create_subprocess_exec", new=AsyncMock(return_value=fake)
    ) as mock_exec:
        await _ssh_untar_from_file("10.0.0.5", 30022, str(local), key_path="")
    return mock_exec.call_args.args[-1]


class TestSshTarToFileCommandShape:
    """Capture-side ``tar | zstd`` honest-rc wrapper (§C1d).

    ``_ssh_tar_to_file`` builds its own SSH remote command (separate from
    snapshot_service's C1b fix) — a shell pipeline only reports the LAST
    stage's exit code, so a fatal ``tar`` failure upstream of ``zstd`` was
    silently masked (a failing tar with zstd still compressing whatever
    partial/empty stream it received reports overall success). Wrap in
    ``bash -c`` (guarantees PIPESTATUS regardless of the agent-host login
    shell) and collapse to an honest verdict: accept tar rc in {0, 1} (rc==1
    is the benign "file changed as we read it" warning on a live workspace)
    with a clean zstd; reject tar rc>=2 or any zstd failure. The caller's
    existing ``proc.returncode == 0 and total > 0`` gate is untouched — it
    now just gates on the honest verdict instead of the masked raw zstd rc.
    """

    @pytest.mark.asyncio
    async def test_wrapped_in_bash_c_with_pipestatus_verdict(self, tmp_path):
        remote_cmd = await _capture_remote_cmd(
            tmp_path, "/var/lib/code-server/User/globalStorage"
        )

        assert remote_cmd.startswith("bash -c '")
        assert remote_cmd.endswith("'")
        assert (
            "tar -cf - -- /var/lib/code-server/User/globalStorage 2>/dev/null"
            in remote_cmd
        )
        assert "| zstd -1 -T0" in remote_cmd
        # PIPESTATUS must be snapshotted into an array in ONE command before
        # anything reads it (see the real-bash class below for why a bare
        # `__x=${PIPESTATUS[0]}` assignment would silently clobber it).
        assert '__ps=("${PIPESTATUS[@]}")' in remote_cmd
        assert "${__ps[0]}" in remote_cmd
        assert "${__ps[1]}" in remote_cmd
        assert "exit 1" in remote_cmd
        assert "exit 0" in remote_cmd
        assert "du -sb -- /var/lib/code-server/User/globalStorage" in remote_cmd

    @pytest.mark.asyncio
    async def test_remote_path_has_no_single_quote_so_embedding_is_safe(self, tmp_path):
        # remote_path is interpolated directly into the single-quoted `-c`
        # body with no shell-escaping — safe only because filesystem paths
        # used here never contain a single quote. Assert that invariant
        # explicitly, and that quoting stays balanced with a realistic path.
        for path in (
            GLOBAL_STORAGE_DIR,
            "/var/lib/code-server/extensions/acme.private-1.2.3-universal",
        ):
            assert "'" not in path
            remote_cmd = await _capture_remote_cmd(tmp_path, path)
            assert path in remote_cmd
            assert remote_cmd.count("'") == 2

    @pytest.mark.asyncio
    async def test_returns_true_when_verdict_accepts(self, tmp_path):
        fake = MagicMock()
        fake.returncode = 0  # the wrapper's own accept code, not raw zstd rc
        fake.stdout = MagicMock()
        fake.stdout.read = AsyncMock(side_effect=[b"data", b""])
        fake.wait = AsyncMock(return_value=None)
        local = tmp_path / "out.tar.zst"

        with patch("asyncio.create_subprocess_exec", new=AsyncMock(return_value=fake)):
            ok = await _ssh_tar_to_file("h", 22, "/p", str(local), key_path="")

        assert ok is True

    @pytest.mark.asyncio
    async def test_returns_false_when_verdict_rejects_despite_bytes_flowing(
        self, tmp_path
    ):
        # Pins the `rc == 0` bool contract: even though bytes flowed to
        # stdout (zstd compressed *something*), a nonzero wrapper verdict
        # must still be reported as failure.
        fake = MagicMock()
        fake.returncode = 1  # the wrapper's reject code
        fake.stdout = MagicMock()
        fake.stdout.read = AsyncMock(side_effect=[b"data", b""])
        fake.wait = AsyncMock(return_value=None)
        local = tmp_path / "out.tar.zst"

        with patch("asyncio.create_subprocess_exec", new=AsyncMock(return_value=fake)):
            ok = await _ssh_tar_to_file("h", 22, "/p", str(local), key_path="")

        assert ok is False


class TestSshUntarFromFileCommandShape:
    """Extract-side ``zstd -d | tar`` honest-rc wrapper (§C1d).

    ``_ssh_untar_from_file`` returns a bool straight off ``proc.returncode``
    (``return proc.returncode == 0`` — confirmed below), so the fix must
    make that raw exit code honest rather than change the caller. Same
    verdict shape as the capture side: accept tar rc in {0, 1}; reject tar
    rc>=2 or any zstd failure — the previously-masked case being a corrupt
    archive's ``zstd -d`` decompression failure hidden behind tar's own
    (last-stage) exit 0.
    """

    @pytest.mark.asyncio
    async def test_wrapped_in_bash_c_with_pipestatus_verdict(self, tmp_path):
        remote_cmd = await _untar_remote_cmd(tmp_path)

        argv = shlex.split(remote_cmd)
        assert argv[:2] == ["flock", "-w"]
        assert argv[3] == "/tmp/.srw-vm-remote-operation.lock"
        assert "timeout" in argv
        assert argv[-5:-1] == ["bash", "-o", "pipefail", "-c"]
        body = argv[-1]
        assert "zstd -dc | python3 -c" in body
        assert "limit=2147483648" in body
        assert "tar --no-same-owner --no-same-permissions" in body
        assert '__ps=("${PIPESTATUS[@]}")' in body
        assert "${__ps[0]}" in body
        assert "${__ps[1]}" in body
        assert "${__ps[2]}" in body
        assert "exit 1" in body
        assert "exit 0" in body

    @pytest.mark.asyncio
    async def test_returns_true_on_accept_verdict(self, tmp_path):
        # Confirms the exact return line: `return proc.returncode == 0`.
        local = tmp_path / "in.tar.zst"
        local.write_bytes(b"x")
        fake = MagicMock()
        fake.returncode = 0
        fake.stdin = MagicMock()
        fake.stdin.write = MagicMock()
        fake.stdin.drain = AsyncMock(return_value=None)
        fake.stdin.close = MagicMock()
        fake.wait = AsyncMock(return_value=None)

        with patch("asyncio.create_subprocess_exec", new=AsyncMock(return_value=fake)):
            ok = await _ssh_untar_from_file("h", 22, str(local), key_path="")

        assert ok is True

    @pytest.mark.asyncio
    async def test_returns_false_on_reject_verdict(self, tmp_path):
        local = tmp_path / "in.tar.zst"
        local.write_bytes(b"x")
        fake = MagicMock()
        fake.returncode = 1  # the wrapper's reject code
        fake.stdin = MagicMock()
        fake.stdin.write = MagicMock()
        fake.stdin.drain = AsyncMock(return_value=None)
        fake.stdin.close = MagicMock()
        fake.wait = AsyncMock(return_value=None)

        with patch("asyncio.create_subprocess_exec", new=AsyncMock(return_value=fake)):
            ok = await _ssh_untar_from_file("h", 22, str(local), key_path="")

        assert ok is False

    @pytest.mark.asyncio
    async def test_cancellation_joins_remote_mutation_before_return(self, tmp_path):
        local = tmp_path / "in.tar.zst"
        local.write_bytes(b"x")
        entered = asyncio.Event()
        release = asyncio.Event()
        fake = MagicMock()
        fake.returncode = None
        fake.stdin.write = MagicMock()
        fake.stdin.drain = AsyncMock(return_value=None)
        fake.stdin.close = MagicMock()
        fake.stderr.read = AsyncMock(side_effect=[b""])
        fake.terminate = MagicMock()
        fake.kill = MagicMock()

        async def _wait():
            entered.set()
            await release.wait()
            fake.returncode = 0
            return 0

        fake.wait = AsyncMock(side_effect=_wait)
        with patch("asyncio.create_subprocess_exec", new=AsyncMock(return_value=fake)):
            mutation = asyncio.create_task(
                _ssh_untar_from_file("h", 22, str(local), key_path="")
            )
            await entered.wait()
            mutation.cancel()
            await asyncio.sleep(0)
            assert not mutation.done()
            fake.terminate.assert_not_called()
            fake.kill.assert_not_called()

            release.set()
            with pytest.raises(asyncio.CancelledError):
                await mutation

        fake.terminate.assert_not_called()
        fake.kill.assert_not_called()


class TestIdeSettingsHonestRcRealBash:
    """Real-``bash`` verdict-matrix proof for both C1d wrappers.

    Mocked-subprocess tests (above) only prove the STRING shape of the
    remote command; they give zero runtime coverage of whether the embedded
    PIPESTATUS shell arithmetic actually computes the right verdict — the
    C1b lesson (see ``test_pipestatus_discrimination_tail_maps_stage_exits_
    correctly`` in ``tests/test_snapshot_ssh_extraction.py``): a subtly
    wrong tail can look right and still be wrong (e.g. a bare
    ``__x=${PIPESTATUS[0]}`` assignment is itself a simple command and
    immediately resets PIPESTATUS, clobbering it before a second read).
    This extracts the real verdict tail from the command strings the module
    actually builds and executes it under real bash with synthetic
    ``( exit A ) | ( exit B )`` stages standing in for tar/zstd, so a
    regression to a broken tail fails here even though every mocked test
    above stays green.
    """

    @staticmethod
    def _tail_after(body: str, anchor: str) -> str:
        return body[body.index(anchor) + len(anchor) :]

    @pytest.mark.asyncio
    async def test_capture_verdict_matrix_under_real_bash(self, tmp_path):
        if shutil.which("bash") is None:
            pytest.skip("bash not available")

        remote_cmd = await _capture_remote_cmd(tmp_path, "/some/remote/path")
        assert remote_cmd.startswith("bash -c '") and remote_cmd.endswith("'")
        body = remote_cmd[len("bash -c '") : -1]
        tail = self._tail_after(body, "| zstd -1 -T0; ")

        def run(tar_rc: int, zstd_rc: int, *, wrapped: bool) -> int:
            pipeline = f"( exit {tar_rc} ) | ( exit {zstd_rc} )"
            script = f"{pipeline}; {tail}" if wrapped else pipeline
            result = subprocess.run(
                ["bash", "-c", script], capture_output=True, text=True
            )
            return result.returncode

        # (tar_rc, zstd_rc) -> honest verdict: accept tar in {0,1} + clean
        # zstd (0); reject tar>=2 or any zstd failure (1).
        cases = [
            (0, 0, 0),
            (1, 0, 0),  # tolerated: benign tar "file changed as we read it"
            (2, 0, 1),  # rejected: fatal tar failure
            (0, 5, 1),  # rejected: zstd failure
        ]
        for tar_rc, zstd_rc, expected in cases:
            got = run(tar_rc, zstd_rc, wrapped=True)
            assert got == expected, (
                f"tar_rc={tar_rc} zstd_rc={zstd_rc}: expected {expected}, got {got}"
            )

        # The load-bearing with/without contrast for capture's OWN masking
        # case: tar fails fatally (rc=2) but zstd, as the pipeline's LAST
        # stage, still exits 0 compressing whatever partial/empty stream it
        # received. The plain (unwrapped) pipeline reports SUCCESS — today's
        # bug — and only stops being a false 0 once the extracted verdict
        # tail runs. Proves the matrix above isn't passing vacuously.
        assert run(2, 0, wrapped=False) == 0
        assert run(2, 0, wrapped=True) != 0

    @pytest.mark.asyncio
    async def test_extract_verdict_matrix_under_real_bash(self, tmp_path):
        if shutil.which("bash") is None:
            pytest.skip("bash not available")

        remote_cmd = await _untar_remote_cmd(tmp_path)
        body = shlex.split(remote_cmd)[-1]
        tail = self._tail_after(body, "-C /; ")

        def run(zstd_rc: int, limiter_rc: int, tar_rc: int, *, wrapped: bool) -> int:
            pipeline = f"( exit {zstd_rc} ) | ( exit {limiter_rc} ) | ( exit {tar_rc} )"
            script = f"{pipeline}; {tail}" if wrapped else pipeline
            result = subprocess.run(
                ["bash", "-c", script], capture_output=True, text=True
            )
            return result.returncode

        # (zstd_rc, tar_rc) -> honest verdict: accept a clean zstd (0) + tar
        # in {0,1}; reject any zstd failure or tar>=2.
        cases = [
            (0, 0, 0, 0),
            (0, 0, 1, 0),  # tolerated tar warning
            (0, 0, 2, 1),  # rejected: fatal tar failure
            (1, 0, 0, 1),  # rejected: masked zstd decompression failure
            (0, 73, 0, 1),  # rejected: decompressed byte cap exceeded
        ]
        for zstd_rc, limiter_rc, tar_rc, expected in cases:
            got = run(zstd_rc, limiter_rc, tar_rc, wrapped=True)
            assert got == expected, (
                f"zstd_rc={zstd_rc} limiter_rc={limiter_rc} tar_rc={tar_rc}: "
                f"expected {expected}, got {got}"
            )

        # The load-bearing with/without contrast for the extract-side
        # masking fix (§C1d, mirrors C1c's zstd-fail proof in
        # tests/test_snapshot_ssh_extraction.py): a corrupt archive's
        # `zstd -d` failure (rc=1) with `tar` still exiting 0 (it received a
        # short/empty stream and didn't itself error) reports SUCCESS on the
        # plain, unwrapped pipeline — exactly today's bug — and only stops
        # being a false 0 once the extracted verdict tail runs.
        assert run(1, 0, 0, wrapped=False) == 0
        assert run(1, 0, 0, wrapped=True) != 0


class TestCaptureProfile:
    @pytest.mark.asyncio
    async def test_skips_when_signature_unchanged(self):
        db = FakeSettingsDB({UID: {"ide": {"extensions": {"sig": "SAME"}}}})
        store = IdeSettingsStore(db)

        async def sig_runner(host, port, script, key_path=None, timeout=20):
            return 0, b"SAME  -\n", b""

        captured = {"called": False}

        class FakeProfileStore:
            async def put_globalstorage(self, *a, **k):
                captured["called"] = True

        n = await capture_ide_profile(
            store,
            UID,
            "10.0.0.1",
            30022,
            FakeProfileStore(),
            _runner=sig_runner,
            _tar_fn=None,
        )
        assert n == 0 and captured["called"] is False


class TestSeedProfile:
    @pytest.mark.asyncio
    async def test_extracts_globalstorage_and_touches_sentinel(self, tmp_path):
        # profile store yields a blob; runner records the extract+sentinel script
        scripts = []

        async def _ok(*a, **k):
            return True

        class FakeProfileStore:
            async def get_globalstorage(self, uid, path):
                with open(path, "wb") as f:
                    f.write(b"GS")
                return True

            async def get_ext_bytes(self, *a, **k):
                return False

        async def fake_runner(host, port, script, key_path=None, timeout=20):
            scripts.append(script)
            return 0, b"", b""

        from orchestrator.services.ide_settings import (
            SEED_STATE_SENTINEL,
            seed_ide_profile,
        )

        ok = await seed_ide_profile(
            user_id=UID,
            ssh_host="h",
            ssh_port=30022,
            profile_store=FakeProfileStore(),
            ext_items={},
            _runner=fake_runner,
            _push_fn=lambda *a, **k: _ok(),
        )
        assert ok is True
        assert any(SEED_STATE_SENTINEL in s for s in scripts)


class TestCaptureBytesExtension:
    @pytest.mark.asyncio
    async def test_resolves_platform_suffixed_folder(self):
        # A bytes-source extension whose on-disk folder carries code-server's
        # platform suffix (e.g. "<id>-<ver>-universal"). Capture must tar the
        # REAL folder, not the bare "<id>-<ver>" guess.
        db = FakeSettingsDB(
            {
                UID: {
                    "ide": {
                        "extensions": {
                            "items": {
                                "acme.private": {
                                    "version": "1.2.3",
                                    "source": "bytes",
                                    "theme": False,
                                }
                            }
                        }
                    }
                }
            }
        )
        store = IdeSettingsStore(db)

        async def runner(host, port, script, key_path=None, timeout=20):
            if "sha256sum" in script:
                return 0, b"NEWSIG  -\n", b""  # signature changed -> proceed
            if "acme.private-1.2.3" in script:  # resolve-ext-dir script
                return 0, b"acme.private-1.2.3-universal\n", b""
            return 0, b"", b""

        tarred = []

        async def tar_fn(
            host, port, remote_path, local_path, key_path=None, timeout=120
        ):
            tarred.append(remote_path)
            return True

        class FakeProfileStore:
            def __init__(self):
                self.put_bytes = None

            async def ext_bytes_exists(self, *a, **k):
                return False

            async def put_globalstorage(self, *a, **k):
                pass

            async def put_ext_bytes(self, user_id, ext_id, version, path):
                self.put_bytes = (ext_id, version)

        ps = FakeProfileStore()
        n = await capture_ide_profile(
            store, UID, "h", 30022, ps, _runner=runner, _tar_fn=tar_fn
        )

        # globalStorage + the resolved bytes folder were tarred
        assert any(t.endswith("/acme.private-1.2.3-universal") for t in tarred)
        # NOT the bare guess
        assert "/var/lib/code-server/extensions/acme.private-1.2.3" not in tarred
        assert ps.put_bytes == ("acme.private", "1.2.3")
        assert n == 2

    @pytest.mark.asyncio
    async def test_skips_when_folder_not_found(self):
        db = FakeSettingsDB(
            {
                UID: {
                    "ide": {
                        "extensions": {
                            "items": {
                                "acme.private": {
                                    "version": "9.9.9",
                                    "source": "bytes",
                                    "theme": False,
                                }
                            }
                        }
                    }
                }
            }
        )
        store = IdeSettingsStore(db)

        async def runner(host, port, script, key_path=None, timeout=20):
            if "sha256sum" in script:
                return 0, b"NEWSIG  -\n", b""
            return 0, b"", b""  # resolve finds nothing

        tarred = []

        async def tar_fn(
            host, port, remote_path, local_path, key_path=None, timeout=120
        ):
            tarred.append(remote_path)
            return True

        class FakeProfileStore:
            async def ext_bytes_exists(self, *a, **k):
                return False

            async def put_globalstorage(self, *a, **k):
                pass

            async def put_ext_bytes(self, *a, **k):
                raise AssertionError("should not upload bytes when folder missing")

        n = await capture_ide_profile(
            store, UID, "h", 30022, FakeProfileStore(), _runner=runner, _tar_fn=tar_fn
        )
        # only globalStorage tarred; the bytes folder was never resolved
        assert tarred == [GLOBAL_STORAGE_DIR]
        assert n == 1


JOB_ID = "22222222-2222-2222-2222-222222222222"
THREAD_ID = "33333333-3333-3333-3333-333333333333"


def _container_row(entity_type, entity_id, **container_extra):
    container = {"status": "ready", "pod_ip": "10.42.0.9", **container_extra}
    return {
        "entity_type": entity_type,
        "id": entity_id,
        "user_id": UID,
        "context": {"workspace_container": container},
    }


def _evict_mocks(live):
    provisioner = MagicMock()
    provisioner.workspace_pod_live = AsyncMock(return_value=live)
    db = MagicMock()
    db.merge_workspace_container_context = AsyncMock(return_value=True)
    db.merge_thread_workspace_context = AsyncMock(return_value=True)
    return provisioner, db


class TestEvictDeadWorkspaces:
    @pytest.mark.asyncio
    async def test_dead_job_pod_is_evicted_and_context_cleared(self):
        provisioner, db = _evict_mocks(live=False)
        row = _container_row("job", JOB_ID)

        kept = await evict_dead_workspaces([row], provisioner, db)

        assert kept == []
        owner = provisioner.workspace_pod_live.await_args.args[0]
        assert (owner.kind, owner.id) == ("job", JOB_ID)
        db.merge_workspace_container_context.assert_awaited_once_with(
            JOB_ID, {"status": "deleted", "pod_ip": None}
        )
        db.merge_thread_workspace_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_dead_thread_pod_is_evicted_via_thread_merge(self):
        provisioner, db = _evict_mocks(live=False)
        row = _container_row("thread", THREAD_ID)
        # Worklist contexts may arrive as JSON strings — must still be probed.
        row["context"] = json.dumps(row["context"])

        kept = await evict_dead_workspaces([row], provisioner, db)

        assert kept == []
        owner = provisioner.workspace_pod_live.await_args.args[0]
        assert (owner.kind, owner.id) == ("session", THREAD_ID)
        db.merge_thread_workspace_context.assert_awaited_once_with(
            THREAD_ID, {"status": "deleted", "pod_ip": None}
        )
        db.merge_workspace_container_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_live_pod_is_kept(self):
        provisioner, db = _evict_mocks(live=True)
        row = _container_row("job", JOB_ID)

        kept = await evict_dead_workspaces([row], provisioner, db)

        assert kept == [row]
        db.merge_workspace_container_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unknown_liveness_is_treated_as_live(self):
        provisioner, db = _evict_mocks(live=None)
        row = _container_row("job", JOB_ID)

        kept = await evict_dead_workspaces([row], provisioner, db)

        assert kept == [row]
        db.merge_workspace_container_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_probe_exception_keeps_the_row(self):
        provisioner, db = _evict_mocks(live=False)
        provisioner.workspace_pod_live = AsyncMock(side_effect=RuntimeError("api down"))
        row = _container_row("job", JOB_ID)

        kept = await evict_dead_workspaces([row], provisioner, db)

        assert kept == [row]
        db.merge_workspace_container_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_vm_row_passes_through_unprobed(self):
        provisioner, db = _evict_mocks(live=False)
        row = {
            "entity_type": "job",
            "id": JOB_ID,
            "user_id": UID,
            "context": {"vm": {"status": "ready", "ssh_host": "vm-1"}},
        }

        kept = await evict_dead_workspaces([row], provisioner, db)

        assert kept == [row]
        provisioner.workspace_pod_live.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_merge_failure_still_evicts_the_dead_row(self):
        provisioner, db = _evict_mocks(live=False)
        db.merge_workspace_container_context = AsyncMock(
            side_effect=RuntimeError("db blip")
        )
        dead = _container_row("job", JOB_ID)
        alive_row = _container_row("thread", THREAD_ID)
        provisioner.workspace_pod_live = AsyncMock(side_effect=[False, True])

        kept = await evict_dead_workspaces([dead, alive_row], provisioner, db)

        assert kept == [alive_row]


class TestSweeperRegistrationShape:
    def test_settings_sweeper_is_leader_gated(self):
        import inspect

        import orchestrator.main as orchestrator_main

        source = inspect.getsource(orchestrator_main.lifespan)
        assert (
            "run_when_leader(code_server_settings_sweeper, _shutdown_event)" in source
        )

    @pytest.mark.asyncio
    async def test_disabled_sweeper_parks_instead_of_returning(self, monkeypatch):
        # A bare return would make run_when_leader respawn (and log) the
        # disabled sweeper every poll second for the whole leadership tenure.
        # R1.B11: the loop lives in its domain owner and takes its
        # collaborators as keyword parameters instead of main's globals.
        from orchestrator.services import ide_settings

        monkeypatch.setenv("IDE_SETTINGS_SYNC_ENABLED", "false")
        db = MagicMock()
        db.list_active_ide_workspaces = AsyncMock(return_value=[])
        provisioner = MagicMock()
        snapshots = MagicMock()
        vms = MagicMock()
        store_factory = MagicMock()
        classifier_factory = MagicMock()
        monkeypatch.setattr(ide_settings, "IdeSettingsStore", store_factory)
        monkeypatch.setattr(ide_settings, "OpenVsxClassifier", classifier_factory)

        shutdown = asyncio.Event()
        waiting = asyncio.Event()
        wait_for_shutdown = shutdown.wait

        async def observed_wait():
            waiting.set()
            return await wait_for_shutdown()

        wait = AsyncMock(side_effect=observed_wait)
        monkeypatch.setattr(shutdown, "wait", wait)
        task = asyncio.create_task(
            ide_settings.code_server_settings_sweeper(
                shutdown,
                db=db,
                container_provisioner=provisioner,
                snapshot_service=snapshots,
                vm_provisioner=vms,
            )
        )
        try:
            await asyncio.wait_for(waiting.wait(), timeout=1)
            assert not task.done(), "disabled sweeper must stay parked until shutdown"
            wait.assert_awaited_once()
            store_factory.assert_not_called()
            classifier_factory.assert_not_called()
            assert db.mock_calls == []
            assert provisioner.mock_calls == []
            assert snapshots.mock_calls == []
            assert vms.mock_calls == []

            shutdown.set()
            await asyncio.wait_for(task, timeout=1)
            wait.assert_awaited_once()
            store_factory.assert_not_called()
            classifier_factory.assert_not_called()
            assert db.mock_calls == []
            assert provisioner.mock_calls == []
            assert snapshots.mock_calls == []
            assert vms.mock_calls == []
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
