"""Both process-zero scans against real processes in a private PID namespace.

Each scenario runs ``tests/_process_zero_namespace.py`` as PID 1 of a fresh
PID namespace (see that module for why this is safe and exact). The scans and
fixtures run as a workspace UID used by nothing else, with no capabilities.

A zero proof is asserted as exit 0 *and* confirmed independently: a target is
stopped only when none of its tasks can run (``executing`` is false) and, for
a dead-leader process, its sibling thread's heartbeat has stopped advancing.
Exit 86 is never accepted as success. It is asserted only where the scenario
deliberately makes a process uninspectable.

``SRW_PROCESS_ZERO_RACE_REPEAT`` (default 3) bounds the exit-race scenarios.
"""

from __future__ import annotations

import os
import uuid

import pytest

from shared.runtime.core.backends.remote import RemoteBackend
from tests._process_zero_namespace import run_scenario

_RACE_REPEAT = int(os.environ.get("SRW_PROCESS_ZERO_RACE_REPEAT", "3"))
_WORKSPACE_TAG_ENV = "SRW_WORKSPACE_PROCESS_TAG"


def _scenario(**kwargs) -> dict:
    try:
        return run_scenario(**kwargs)
    except LookupError as exc:
        pytest.skip(str(exc))


def _backend(**overrides) -> RemoteBackend:
    arguments = dict(
        host="workspace.test",
        workspace_path="/home/agent-host/workspace",
        job_id=str(uuid.uuid4()),
        workspace_generation=str(uuid.uuid4()),
        runtime_incarnation=str(uuid.uuid4()),
    )
    arguments.update(overrides)
    backend = RemoteBackend(**arguments)
    return backend


def _sandbox_backend() -> RemoteBackend:
    thread_id = str(uuid.uuid4())
    return _backend(
        job_id=thread_id,
        workspace_owner_kind="session",
        workspace_owner_id=thread_id,
        expected_host_key_fingerprint="SHA256:" + "A" * 43,
        workspace_tier="sandbox",
        sudo_action="freeze",
    )


def _stateless_backend() -> RemoteBackend:
    backend = _backend()
    backend.set_shell_owner_token(21)
    return backend


def _scan(result: dict, index: int = 0) -> dict:
    return result["scans"][index]


def _stopped(observed: dict) -> bool:
    return not observed["executing"] and not observed.get("heartbeat_advancing", False)


def _running(observed: dict) -> bool:
    if not observed["executing"]:
        return False
    return observed.get("heartbeat_advancing", True)


class TestDedicatedUidScan:
    """The protected sandbox's kernel-UID proof (``workspace_process_zero_v1``)."""

    @pytest.mark.parametrize("terminate", [False, True], ids=["verify", "terminate"])
    def test_an_unreaped_zombie_does_not_block_the_zero_proof(self, terminate):
        command = _sandbox_backend()._dedicated_workspace_uid_process_zero_shell(
            terminate=terminate
        )

        result = _scenario(command=command, fixtures=[{"name": "z", "kind": "zombie"}])

        assert _scan(result)["returncode"] == 0, _scan(result)["stderr"]
        assert result["after"]["z"]["state"] == "Z"
        assert result["after"]["z"]["threads"] == "1"

    def test_verify_reports_a_dead_leader_whose_thread_still_executes(self):
        command = _sandbox_backend()._dedicated_workspace_uid_process_zero_shell(
            terminate=False
        )

        result = _scenario(
            command=command, fixtures=[{"name": "half", "kind": "dead_leader"}]
        )

        half = result["before"]["half"]
        assert (half["state"], half["threads"]) == ("Z", "2")
        assert _running(half)
        scan = _scan(result)
        assert scan["returncode"] == 85, scan
        assert f"residual PIDs: {half['pid']}" in scan["stderr"]
        # Verify never signals: the sibling thread is still executing.
        assert _running(result["after"]["half"])

    @pytest.mark.parametrize("ignore_term", [False, True], ids=["term", "term-ignored"])
    def test_terminate_stops_a_dead_leader_group_before_proving_zero(self, ignore_term):
        command = _sandbox_backend()._dedicated_workspace_uid_process_zero_shell(
            terminate=True
        )

        result = _scenario(
            command=command,
            fixtures=[
                {"name": "half", "kind": "dead_leader", "ignore_term": ignore_term}
            ],
        )

        assert _running(result["before"]["half"])
        scan = _scan(result)
        assert scan["returncode"] == 0, scan
        assert _stopped(result["after"]["half"]), result["after"]
        if ignore_term:
            # SIGTERM is ignored process-wide, so only the SIGKILL escalation
            # after the 5 s TERM budget can have stopped the sibling thread.
            assert scan["elapsed"] >= 4.9

    def test_terminate_spares_other_uids_and_its_own_ancestry(self):
        command = _sandbox_backend()._dedicated_workspace_uid_process_zero_shell(
            terminate=True
        )

        result = _scenario(
            command=command,
            fixtures=[
                {"name": "writer", "kind": "sleeper"},
                {"name": "half", "kind": "dead_leader"},
                {"name": "unrelated", "kind": "sleeper", "uid": "other"},
                {"name": "unrelated_half", "kind": "dead_leader", "uid": "other"},
            ],
        )

        assert _scan(result)["returncode"] == 0, _scan(result)
        after = result["after"]
        assert _stopped(after["writer"]) and _stopped(after["half"])
        assert _running(after["unrelated"]) and _running(after["unrelated_half"])

    @pytest.mark.parametrize("attempt", range(_RACE_REPEAT))
    def test_terminate_settles_while_processes_exit_during_inspection(self, attempt):
        command = _sandbox_backend()._dedicated_workspace_uid_process_zero_shell(
            terminate=True
        )

        result = _scenario(
            command=command,
            fixtures=[
                {"name": "churn", "kind": "churn"},
                {"name": "slow", "kind": "slow_exit"},
            ],
        )

        assert _scan(result)["returncode"] == 0, _scan(result)
        for name in ("churn", "slow"):
            assert _stopped(result["after"][name]), (name, result["after"][name])


class TestTaggedScan:
    """The stateless workspace scan scoped by ``SRW_WORKSPACE_PROCESS_TAG``."""

    @staticmethod
    def _tagged(backend: RemoteBackend) -> dict[str, str]:
        return {_WORKSPACE_TAG_ENV: backend._workspace_process_tag()}

    def _run(self, backend: RemoteBackend, *, terminate: bool, fixtures, **kwargs):
        command = backend._stateless_workspace_process_zero_shell(terminate=terminate)
        # The scan itself carries the tag, as it does when it runs inside a
        # tagged workspace; excluding its own ancestry is part of the proof.
        return _scenario(
            command=command,
            fixtures=fixtures,
            scan_env=self._tagged(backend),
            **kwargs,
        )

    @pytest.mark.parametrize("terminate", [False, True], ids=["verify", "terminate"])
    def test_an_unreaped_zombie_does_not_abort_the_scan(self, terminate):
        backend = _stateless_backend()
        tagged = self._tagged(backend)

        result = self._run(
            backend,
            terminate=terminate,
            fixtures=[
                {"name": "z", "kind": "zombie", "env": tagged},
                {"name": "holder", "kind": "non_reaping_parent", "child_env": tagged},
                {"name": "untagged", "kind": "sleeper"},
            ],
        )

        assert _scan(result)["returncode"] == 0, _scan(result)
        after = result["after"]
        assert after["z"]["state"] == "Z"
        # The untagged holder is not this workspace's process and still holds
        # its exited, tagged child unreaped.
        assert _running(after["holder"]) and after["holder"]["child_state"] == "Z"
        assert _running(after["untagged"])

    def test_terminate_escalates_past_a_term_ignoring_target_beside_a_zombie(self):
        backend = _stateless_backend()
        tagged = self._tagged(backend)
        foreign = {_WORKSPACE_TAG_ENV: _stateless_backend()._workspace_process_tag()}

        result = self._run(
            backend,
            terminate=True,
            fixtures=[
                {"name": "z", "kind": "zombie", "env": tagged},
                {"name": "holder", "kind": "non_reaping_parent", "child_env": tagged},
                {"name": "stubborn", "kind": "term_ignorer", "env": tagged},
                {"name": "untagged", "kind": "sleeper"},
                {"name": "foreign", "kind": "sleeper", "env": foreign},
            ],
        )

        assert _running(result["before"]["stubborn"])
        scan = _scan(result)
        assert scan["returncode"] == 0, scan
        # TERM is ignored, so only SIGKILL after the 5 s budget stops it.
        assert scan["elapsed"] >= 4.9
        after = result["after"]
        assert _stopped(after["stubborn"])
        assert _running(after["untagged"]) and _running(after["foreign"])
        assert _running(after["holder"]) and after["z"]["state"] == "Z"

    def test_verify_reports_a_live_target_beside_a_zombie_without_signalling(self):
        backend = _stateless_backend()
        tagged = self._tagged(backend)

        result = self._run(
            backend,
            terminate=False,
            fixtures=[
                {"name": "z", "kind": "zombie", "env": tagged},
                {"name": "target", "kind": "term_logger", "env": tagged},
            ],
        )

        assert _scan(result)["returncode"] == 85, _scan(result)
        assert _running(result["after"]["target"])
        assert result["after"]["target"]["log"] == []

    def test_verify_finds_a_tagged_dead_leader_through_its_live_thread(self):
        backend = _stateless_backend()

        result = self._run(
            backend,
            terminate=False,
            fixtures=[
                {"name": "half", "kind": "dead_leader", "env": self._tagged(backend)}
            ],
        )

        assert _running(result["before"]["half"])
        assert _scan(result)["returncode"] == 85, _scan(result)
        assert _running(result["after"]["half"])

    @pytest.mark.parametrize("ignore_term", [False, True], ids=["term", "term-ignored"])
    def test_terminate_stops_a_tagged_dead_leader_group(self, ignore_term):
        backend = _stateless_backend()

        result = self._run(
            backend,
            terminate=True,
            fixtures=[
                {
                    "name": "half",
                    "kind": "dead_leader",
                    "env": self._tagged(backend),
                    "ignore_term": ignore_term,
                }
            ],
        )

        scan = _scan(result)
        assert scan["returncode"] == 0, scan
        assert _stopped(result["after"]["half"])
        if ignore_term:
            assert scan["elapsed"] >= 4.9

    @pytest.mark.parametrize("terminate", [False, True], ids=["verify", "terminate"])
    def test_an_untagged_dead_leader_is_not_this_workspace_s_process(self, terminate):
        backend = _stateless_backend()

        result = self._run(
            backend,
            terminate=terminate,
            fixtures=[{"name": "half", "kind": "dead_leader"}],
        )

        assert _scan(result)["returncode"] == 0, _scan(result)
        assert _running(result["after"]["half"])

    @pytest.mark.parametrize("attempt", range(_RACE_REPEAT))
    def test_terminate_settles_while_targets_exit_during_inspection(self, attempt):
        backend = _stateless_backend()
        tagged = self._tagged(backend)

        result = self._run(
            backend,
            terminate=True,
            fixtures=[
                {"name": "slow1", "kind": "slow_exit", "env": tagged},
                {"name": "slow2", "kind": "slow_exit", "env": tagged},
                {"name": "churn", "kind": "churn", "env": tagged},
                {"name": "untagged", "kind": "sleeper"},
            ],
        )

        assert _scan(result)["returncode"] == 0, _scan(result)
        for name in ("slow1", "slow2", "churn"):
            assert _stopped(result["after"][name]), (name, result["after"][name])
        assert _running(result["after"]["untagged"])

    @pytest.mark.parametrize("attempt", range(_RACE_REPEAT))
    def test_verify_proves_zero_while_untagged_processes_churn(self, attempt):
        backend = _stateless_backend()

        result = self._run(
            backend,
            terminate=False,
            scans=5,
            fixtures=[{"name": "churn", "kind": "churn"}],
        )

        assert [scan["returncode"] for scan in result["scans"]] == [0] * 5, result[
            "scans"
        ]

    def test_verify_refuses_beside_an_uninspectable_same_uid_process(self):
        backend = _stateless_backend()

        result = self._run(
            backend,
            terminate=False,
            fixtures=[
                {"name": "opaque", "kind": "non_dumpable"},
                {"name": "untagged", "kind": "sleeper"},
            ],
        )

        assert _scan(result)["returncode"] == 86, _scan(result)
        assert _running(result["after"]["opaque"])
        assert _running(result["after"]["untagged"])

    def test_terminate_retires_readable_targets_then_refuses_the_ambiguity(self):
        backend = _stateless_backend()

        result = self._run(
            backend,
            terminate=True,
            fixtures=[
                {"name": "opaque", "kind": "non_dumpable"},
                {"name": "target", "kind": "term_logger", "env": self._tagged(backend)},
            ],
        )

        assert _scan(result)["returncode"] == 86, _scan(result)
        assert _stopped(result["after"]["target"])
        assert result["after"]["target"]["log"] == ["TERM"]
        # Not tagged and not inspectable: never signalled.
        assert _running(result["after"]["opaque"])
