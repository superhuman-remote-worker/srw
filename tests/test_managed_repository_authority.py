"""Security invariants for repository-scoped managed Git authority."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import httpx
import pytest

from orchestrator.services.gitea import GiteaClient
from orchestrator.services.managed_repository_authority import (
    ManagedRepositoryAuthorityError,
    _deploy_keypair,
    authorize_job_repository_transport,
    authorize_thread_repository_transport,
    create_managed_repository,
    ensure_managed_repository_authority,
    project_repository_access_mode,
    repository_url_has_credentials,
    revoke_and_delete_managed_repository,
    runtime_credential,
)
from shared.runtime.core.managed_repository import (
    ManagedRepositoryMaterializationError,
    materialize_managed_repository_credentials,
)
from shared.runtime.core.backends.remote import RemoteBackend


def _authority(*, repo_name: str = "job-12345678") -> dict:
    private_key, public_key, fingerprint = _deploy_keypair()
    authority_id = uuid4()
    return {
        "id": authority_id,
        "repository_owner": "srw",
        "repo_name": repo_name,
        "authority_kind": "job",
        "authority_id": uuid4(),
        "project_id": None,
        "generation": 1,
        "access_mode": "write",
        "creation_intent_id": uuid4(),
        "clean_repo_url": f"http://gitea:3000/srw/{repo_name}.git",
        "public_key": public_key,
        "public_key_fingerprint": fingerprint,
        "private_key": private_key,
        "status": "active",
    }


def _runtime_payload(authority: dict, *, host: str = "gitea") -> dict:
    authority_slug = str(authority["id"]).replace("-", "")
    alias = f"srw-repo-{authority_slug}"
    return {
        "version": 1,
        "authority_id": str(authority["id"]),
        "generation": authority["generation"],
        "access_mode": authority["access_mode"],
        "repo_name": authority["repo_name"],
        "repository_owner": authority["repository_owner"],
        "clone_url": (
            f"ssh://{alias}/{authority['repository_owner']}/"
            f"{authority['repo_name']}.git"
        ),
        "alias": alias,
        "ssh_host": host,
        "ssh_port": 2222,
        "private_key": authority["private_key"],
        "public_key_fingerprint": authority["public_key_fingerprint"],
    }


class _RecordingBackend:
    supports_shell = True

    def __init__(self) -> None:
        self.commands: list[tuple[str, bytes]] = []
        self.files: dict[str, str] = {}

    @staticmethod
    def resolve_home_path(path: str) -> str:
        return f"/home/agent-host/{path}"

    def write_home_file(self, path: str, content: str) -> None:
        self.files[path] = content

    def execute_with_secret_stdin(
        self, command: str, secret: str | bytes | bytearray, *, timeout: int = 30
    ) -> bool:
        del timeout
        payload = bytes(secret) if not isinstance(secret, str) else secret.encode()
        self.commands.append((command, payload))
        return True


class _SecretOutputChannel:
    def __init__(self, secret: bytes) -> None:
        self.stdout = [secret]
        self.stderr = [secret]

    def recv_ready(self) -> bool:
        return bool(self.stdout)

    def recv(self, _size: int) -> bytes:
        return self.stdout.pop(0)

    def recv_stderr_ready(self) -> bool:
        return bool(self.stderr)

    def recv_stderr(self, _size: int) -> bytes:
        return self.stderr.pop(0)

    def exit_status_ready(self) -> bool:
        return not self.stdout and not self.stderr

    def recv_exit_status(self) -> int:
        return 1


def test_gitea_canonical_url_discards_misconfigured_admin_userinfo(
    monkeypatch,
) -> None:
    monkeypatch.setenv(
        "GITEA_INTERNAL_URL", "http://shared-admin:shared-secret@gitea:3000"
    )
    monkeypatch.setenv("GITEA_ADMIN_USER", "srw")

    client = GiteaClient()
    url = client.clean_repo_url("job-12345678")

    assert url == "http://gitea:3000/srw/job-12345678.git"
    assert not repository_url_has_credentials(url)
    assert "shared-secret" not in url


def test_public_job_ingress_recursively_strips_repository_authority() -> None:
    from orchestrator.schemas.job_create import JobCreate

    body = JobCreate(
        description="caller cannot author repository authority",
        context={
            "git_remote_url": "http://admin:secret@gitea/repo.git",
            "wrapper": {
                "managed_repository_credentials": [{"private_key": "secret"}],
                "repo_name": "foreign-project",
                "ordinary": "kept",
            },
        },
        config_override={
            "wrapper": {
                "repository_auth": {"password": "secret"},
                "ordinary": "kept",
            }
        },
    )

    assert body.context == {"wrapper": {"ordinary": "kept"}}
    assert body.config_override == {"wrapper": {"ordinary": "kept"}}


@pytest.mark.asyncio
async def test_create_repo_ignores_forge_clone_url_and_returns_clean_canonical(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GITEA_INTERNAL_URL", "http://gitea:3000")
    client = GiteaClient()
    client._initialized = True
    intent_marker = str(uuid4())
    response = MagicMock(status_code=201)
    response.json.return_value = {
        "clone_url": "http://admin:shared-secret@gitea:3000/srw/job-x.git"
    }
    verified = MagicMock(status_code=200)
    verified.json.return_value = {
        "name": "job-x",
        "owner": {"login": "srw"},
        "description": client._repository_intent_description(intent_marker),
    }
    transport = MagicMock()
    transport.post = AsyncMock(return_value=response)
    transport.get = AsyncMock(return_value=verified)
    client._get_client = MagicMock(return_value=transport)

    url = await client.create_repo("job-x", intent_marker=intent_marker)

    assert url == "http://gitea:3000/srw/job-x.git"
    assert not repository_url_has_credentials(url)


@pytest.mark.asyncio
async def test_create_repo_name_collision_is_not_treated_as_authority(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GITEA_INTERNAL_URL", "http://gitea:3000")
    client = GiteaClient()
    client._initialized = True
    response = MagicMock(status_code=409)
    foreign = MagicMock(status_code=200)
    foreign.json.return_value = {
        "name": "foreign-project-repo",
        "owner": {"login": "srw"},
        "description": "not this intent",
    }
    transport = MagicMock()
    transport.post = AsyncMock(return_value=response)
    transport.get = AsyncMock(return_value=foreign)
    client._get_client = MagicMock(return_value=transport)

    assert (
        await client.create_repo("foreign-project-repo", intent_marker=str(uuid4()))
        is None
    )


@pytest.mark.asyncio
async def test_create_repo_lost_response_rechecks_exact_durable_marker(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GITEA_INTERNAL_URL", "http://gitea:3000")
    client = GiteaClient()
    client._initialized = True
    intent_marker = str(uuid4())
    verified = MagicMock(status_code=200)
    verified.json.return_value = {
        "name": "job-lost",
        "owner": {"login": "srw"},
        "description": client._repository_intent_description(intent_marker),
    }
    transport = MagicMock()
    transport.post = AsyncMock(side_effect=httpx.ReadError("response lost"))
    transport.get = AsyncMock(return_value=verified)
    client._get_client = MagicMock(return_value=transport)

    first = await client.create_repo("job-lost", intent_marker=intent_marker)
    # Process restart/retry sees a 409 but adopts only the same marker.
    restarted = GiteaClient()
    restarted._initialized = True
    restarted_transport = MagicMock()
    restarted_transport.post = AsyncMock(return_value=MagicMock(status_code=409))
    restarted_transport.get = AsyncMock(return_value=verified)
    restarted._get_client = MagicMock(return_value=restarted_transport)
    second = await restarted.create_repo("job-lost", intent_marker=intent_marker)

    assert first == second == "http://gitea:3000/srw/job-lost.git"
    assert transport.get.await_count == 1
    assert restarted_transport.get.await_count == 1


@pytest.mark.asyncio
async def test_concurrent_create_retry_converges_on_exact_marker(monkeypatch) -> None:
    monkeypatch.setenv("GITEA_INTERNAL_URL", "http://gitea:3000")
    client = GiteaClient()
    client._initialized = True
    intent_marker = str(uuid4())
    created = MagicMock(status_code=201)
    collided = MagicMock(status_code=409)
    verified = MagicMock(status_code=200)
    verified.json.return_value = {
        "name": "job-race-create",
        "owner": {"login": "srw"},
        "description": client._repository_intent_description(intent_marker),
    }
    transport = MagicMock()
    transport.post = AsyncMock(side_effect=[created, collided])
    transport.get = AsyncMock(return_value=verified)
    client._get_client = MagicMock(return_value=transport)

    first, second = await asyncio.gather(
        client.create_repo("job-race-create", intent_marker=intent_marker),
        client.create_repo("job-race-create", intent_marker=intent_marker),
    )

    assert first == second == "http://gitea:3000/srw/job-race-create.git"
    assert transport.post.await_count == 2
    assert transport.get.await_count == 2


@pytest.mark.asyncio
async def test_marker_bound_cleanup_refuses_foreign_collision(monkeypatch) -> None:
    monkeypatch.setenv("GITEA_INTERNAL_URL", "http://gitea:3000")
    client = GiteaClient()
    client._initialized = True
    foreign = MagicMock(status_code=200)
    foreign.json.return_value = {
        "name": "job-collision",
        "owner": {"login": "srw"},
        "description": "foreign repository",
    }
    transport = MagicMock()
    transport.get = AsyncMock(return_value=foreign)
    transport.delete = AsyncMock()
    client._get_client = MagicMock(return_value=transport)

    assert not await client.delete_repo("job-collision", intent_marker=str(uuid4()))
    transport.delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_marker_cleanup_failure_keeps_durable_owner_retryable() -> None:
    authority_id = uuid4()
    intent_id = uuid4()
    marker = uuid4()
    db = MagicMock()
    db.claim_managed_repository_authority_revoke = AsyncMock(
        return_value={"id": authority_id, "forge_key_id": 41}
    )
    db.claim_managed_repository_creation_cleanup = AsyncMock(
        return_value={"id": intent_id, "intent_marker": marker}
    )
    db.finish_managed_repository_authority_revoke = AsyncMock(return_value=True)
    db.finish_managed_repository_creation_cleanup = AsyncMock(return_value=True)
    gitea = MagicMock()
    gitea.repository_owner = "srw"
    gitea.delete_repo_deploy_key = AsyncMock(return_value=True)
    gitea.delete_repo = AsyncMock(return_value=False)

    assert not await revoke_and_delete_managed_repository(db, gitea, "job-collision")
    db.finish_managed_repository_authority_revoke.assert_awaited_once_with(
        str(authority_id)
    )
    db.finish_managed_repository_creation_cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_replayed_completed_cleanup_never_touches_foreign_replacement() -> None:
    intent_id = uuid4()
    db = MagicMock()
    db.claim_managed_repository_authority_revoke = AsyncMock(return_value=None)
    db.claim_managed_repository_creation_cleanup = AsyncMock(
        return_value={
            "id": intent_id,
            "intent_marker": uuid4(),
            "status": "deleted",
        }
    )
    db.finish_managed_repository_creation_cleanup = AsyncMock(return_value=True)
    gitea = MagicMock()
    gitea.repository_owner = "srw"
    gitea.delete_repo = AsyncMock()

    assert await revoke_and_delete_managed_repository(db, gitea, "job-reused")
    gitea.delete_repo.assert_not_awaited()
    db.finish_managed_repository_creation_cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_managed_repository_persists_intent_before_forge_mutation() -> (
    None
):
    marker = uuid4()
    intent_id = uuid4()
    events: list[str] = []

    async def reserve_intent(**_kwargs):
        events.append("intent")
        return {"id": intent_id, "intent_marker": marker, "status": "pending"}

    async def create_repo(*_args, **_kwargs):
        assert events == ["intent"]
        events.append("forge")
        return "http://gitea:3000/srw/job-intent.git"

    async def mark_created(*_args, **_kwargs):
        assert events == ["intent", "forge"]
        events.append("settled")
        return {"id": intent_id, "intent_marker": marker, "status": "created"}

    db = MagicMock()
    db.reserve_managed_repository_creation_intent = AsyncMock(
        side_effect=reserve_intent
    )
    db.mark_managed_repository_created = AsyncMock(side_effect=mark_created)
    db.fail_managed_repository_creation_intent = AsyncMock()
    gitea = MagicMock()
    gitea.repository_owner = "srw"
    gitea.create_repo = AsyncMock(side_effect=create_repo)

    clean_url, created = await create_managed_repository(
        db,
        gitea,
        repo_name="job-intent",
        authority_kind="job",
        authority_id=str(uuid4()),
        project_id=None,
        access_mode="write",
    )

    assert clean_url.endswith("/srw/job-intent.git")
    assert created["status"] == "created"
    assert events == ["intent", "forge", "settled"]
    gitea.create_repo.assert_awaited_once_with("job-intent", intent_marker=str(marker))
    db.mark_managed_repository_created.assert_awaited_once_with(
        str(intent_id), intent_marker=str(marker)
    )


@pytest.mark.asyncio
async def test_foreign_creation_collision_is_terminal_and_non_owning() -> None:
    marker = uuid4()
    intent_id = uuid4()
    db = MagicMock()
    db.reserve_managed_repository_creation_intent = AsyncMock(
        return_value={"id": intent_id, "intent_marker": marker, "status": "pending"}
    )
    db.conflict_managed_repository_creation_intent = AsyncMock(return_value=True)
    db.fail_managed_repository_creation_intent = AsyncMock()
    gitea = MagicMock()
    gitea.repository_owner = "srw"
    gitea.create_repo = AsyncMock(return_value=None)
    gitea.repository_creation_intent_status = AsyncMock(return_value="conflict")

    with pytest.raises(ManagedRepositoryAuthorityError) as exc:
        await create_managed_repository(
            db,
            gitea,
            repo_name="job-foreign",
            authority_kind="job",
            authority_id=str(uuid4()),
            project_id=None,
            access_mode="write",
        )

    assert exc.value.code == "repository_creation_conflict"
    db.conflict_managed_repository_creation_intent.assert_awaited_once_with(
        str(intent_id)
    )
    db.fail_managed_repository_creation_intent.assert_not_awaited()


@pytest.mark.asyncio
async def test_conflicted_creation_cleanup_never_touches_foreign_repository() -> None:
    intent_id = uuid4()
    db = MagicMock()
    db.claim_managed_repository_authority_revoke = AsyncMock(return_value=None)
    db.claim_managed_repository_creation_cleanup = AsyncMock(
        return_value={
            "id": intent_id,
            "intent_marker": uuid4(),
            "status": "conflicted",
        }
    )
    db.finish_managed_repository_creation_cleanup = AsyncMock()
    gitea = MagicMock()
    gitea.repository_owner = "srw"
    gitea.delete_repo = AsyncMock()

    assert await revoke_and_delete_managed_repository(db, gitea, "job-foreign")
    gitea.delete_repo.assert_not_awaited()
    db.finish_managed_repository_creation_cleanup.assert_not_awaited()


@pytest.mark.asyncio
async def test_concurrent_deploy_key_registration_adopts_only_exact_public_key(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GITEA_INTERNAL_URL", "http://gitea:3000")
    client = GiteaClient()
    client._initialized = True
    _private, public_key, _fingerprint = _deploy_keypair()
    empty = MagicMock(status_code=200)
    empty.json.return_value = []
    conflict = MagicMock(status_code=422)
    winner = MagicMock(status_code=200)
    winner.json.return_value = [{"id": 73, "key": public_key, "read_only": False}]
    transport = MagicMock()
    transport.get = AsyncMock(side_effect=[empty, winner])
    transport.post = AsyncMock(return_value=conflict)
    client._get_client = MagicMock(return_value=transport)

    key_id = await client.ensure_repo_deploy_key(
        "job-race",
        title="srw-managed-race",
        public_key=public_key,
        access_mode="write",
    )

    assert key_id == 73
    assert transport.get.await_count == 2


@pytest.mark.asyncio
async def test_deploy_key_access_mode_is_exact_and_read_only_is_not_writable(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GITEA_INTERNAL_URL", "http://gitea:3000")
    client = GiteaClient()
    client._initialized = True
    _private, public_key, _fingerprint = _deploy_keypair()
    empty = MagicMock(status_code=200)
    empty.json.return_value = []
    created = MagicMock(status_code=201)
    created.json.return_value = {"id": 74}
    transport = MagicMock()
    transport.get = AsyncMock(return_value=empty)
    transport.post = AsyncMock(return_value=created)
    client._get_client = MagicMock(return_value=transport)

    assert (
        await client.ensure_repo_deploy_key(
            "project-read",
            title="srw-managed-read",
            public_key=public_key,
            access_mode="read",
        )
        == 74
    )
    assert transport.post.await_args.kwargs["json"]["read_only"] is True


def test_project_repository_access_modes_are_explicit() -> None:
    assert project_repository_access_mode({"is_managed": False}) is None
    assert (
        project_repository_access_mode(
            {"is_managed": True, "role": "knowledge", "read_only": False}
        )
        is None
    )
    assert (
        project_repository_access_mode(
            {"is_managed": True, "role": "reference", "read_only": True}
        )
        == "read"
    )
    assert (
        project_repository_access_mode(
            {"is_managed": True, "role": "source", "read_only": True}
        )
        == "read"
    )
    assert (
        project_repository_access_mode(
            {"is_managed": True, "role": "source", "read_only": False}
        )
        == "write"
    )


@pytest.mark.asyncio
async def test_negative_probe_targets_foreign_repo_without_exposing_key(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GITEA_INTERNAL_URL", "http://gitea:3000")
    monkeypatch.setenv("GITEA_SSH_INTERNAL_HOST", "gitea")
    monkeypatch.setenv("GITEA_SSH_INTERNAL_PORT", "2222")
    monkeypatch.setenv("GITEA_ADMIN_PASSWORD", "must-not-enter-child")
    client = GiteaClient()
    client._initialized = True
    private_key = _authority()["private_key"]
    process = AsyncMock()
    process.returncode = 128
    process.communicate = AsyncMock(return_value=(b"", b""))

    with patch(
        "orchestrator.services.gitea.asyncio.create_subprocess_exec",
        AsyncMock(return_value=process),
    ) as spawn:
        allowed = await client.probe_repo_deploy_key(
            "project-one",
            private_key=private_key,
            access_mode="write",
            target_repo_name="project-two",
        )

    assert allowed is False
    args = spawn.await_args.args
    env = spawn.await_args.kwargs["env"]
    assert "project-two.git" in args[2]
    assert private_key not in " ".join(str(value) for value in args)
    assert private_key not in "\n".join(f"{key}={value}" for key, value in env.items())
    assert "GITEA_ADMIN_PASSWORD" not in env
    assert "must-not-enter-child" not in env.values()


@pytest.mark.asyncio
async def test_authority_is_proven_before_activation() -> None:
    authority = _authority()
    provisioning = {**authority, "status": "provisioning"}
    db = MagicMock()
    db.managed_repository_scope_is_unambiguous = AsyncMock(return_value=True)
    db.reserve_managed_repository_authority = AsyncMock(return_value=provisioning)
    db.record_managed_repository_authority_forge_key = AsyncMock(
        return_value=provisioning
    )
    db.activate_managed_repository_authority = AsyncMock(
        return_value={
            key: value for key, value in authority.items() if key != "private_key"
        }
    )
    db.get_managed_repository_authority = AsyncMock(return_value=authority)
    db.fail_managed_repository_authority = AsyncMock()
    gitea = MagicMock()
    gitea.repository_owner = "srw"
    gitea.clean_repo_url = MagicMock(
        side_effect=lambda name: f"http://gitea:3000/srw/{name}.git"
    )
    gitea.clean_repo_url.return_value = authority["clean_repo_url"]
    gitea.ensure_repo_deploy_key = AsyncMock(return_value=41)
    gitea.probe_repo_deploy_key = AsyncMock(return_value=True)

    result = await ensure_managed_repository_authority(
        db,
        gitea,
        repo_name=authority["repo_name"],
        authority_kind="job",
        authority_id=str(authority["authority_id"]),
        access_mode="write",
    )

    assert result == authority
    gitea.probe_repo_deploy_key.assert_awaited_once_with(
        authority["repo_name"],
        private_key=authority["private_key"],
        access_mode="write",
    )
    db.activate_managed_repository_authority.assert_awaited_once_with(
        str(authority["id"]), forge_key_id=41, access_mode="write"
    )
    db.record_managed_repository_authority_forge_key.assert_awaited_once_with(
        str(authority["id"]),
        repository_owner="srw",
        repo_name=authority["repo_name"],
        authority_kind="job",
        authority_scope_id=str(authority["authority_id"]),
        project_id=None,
        generation=1,
        access_mode="write",
        public_key_fingerprint=authority["public_key_fingerprint"],
        forge_key_id=41,
    )


@pytest.mark.asyncio
async def test_unproven_key_never_activates() -> None:
    authority = {**_authority(), "status": "provisioning"}
    db = MagicMock()
    db.managed_repository_scope_is_unambiguous = AsyncMock(return_value=True)
    db.reserve_managed_repository_authority = AsyncMock(return_value=authority)
    db.record_managed_repository_authority_forge_key = AsyncMock(return_value=authority)
    db.fail_managed_repository_authority = AsyncMock(return_value=True)
    db.get_managed_repository_authority = AsyncMock(return_value=None)
    db.activate_managed_repository_authority = AsyncMock()
    gitea = MagicMock()
    gitea.repository_owner = "srw"
    gitea.clean_repo_url.return_value = authority["clean_repo_url"]
    gitea.ensure_repo_deploy_key = AsyncMock(return_value=41)
    gitea.probe_repo_deploy_key = AsyncMock(return_value=False)

    with pytest.raises(ManagedRepositoryAuthorityError) as exc:
        await ensure_managed_repository_authority(
            db,
            gitea,
            repo_name=authority["repo_name"],
            authority_kind="job",
            authority_id=str(authority["authority_id"]),
            access_mode="write",
        )

    assert exc.value.code == "repository_key_unproven"
    db.activate_managed_repository_authority.assert_not_awaited()


def test_workspace_materialization_never_places_private_key_in_command_or_file() -> (
    None
):
    authority = _authority()
    payload = _runtime_payload(authority)
    private_key = payload["private_key"]
    backend = _RecordingBackend()

    urls = materialize_managed_repository_credentials([payload], backend)

    assert urls == {authority["repo_name"]: payload["clone_url"]}
    assert sum(bool(secret) for _command, secret in backend.commands) == 1
    assert any(private_key.encode() == secret for _command, secret in backend.commands)
    ordinary_text = "\n".join(
        [*(command for command, _secret in backend.commands), *backend.files.values()]
    )
    assert private_key not in ordinary_text
    assert "private_key" not in payload
    assert "IdentityFile" not in ordinary_text
    assert "IdentityAgent" in ordinary_text
    assert "IdentitiesOnly" not in ordinary_text
    assert "@" not in payload["clone_url"]
    materialize_command = next(
        command for command, secret in backend.commands if secret
    )
    assert materialize_command.index("flock -x 9") < materialize_command.index(
        f"Host {payload['alias']}"
    )
    assert materialize_command.index("present") < materialize_command.index(
        f"Host {payload['alias']}"
    )


def test_workspace_materialization_wipes_untransferred_keys_on_early_failure(
    monkeypatch,
) -> None:
    from shared.runtime.core import managed_repository as managed_repository_module

    private_key = bytearray(b"untransferred-private-material")
    payload = {"private_key": "caller-copy-is-removed"}
    validated = {
        "authority_id": str(uuid4()),
        "generation": 1,
        "access_mode": "write",
        "repo_name": "job-early-failure",
        "repository_owner": "srw",
        "alias": f"srw-repo-{uuid4().hex}",
        "ssh_host": "gitea",
        "ssh_port": 2222,
        "clone_url": "ssh://unused/srw/job-early-failure.git",
        "private_key": private_key,
        "public_key_fingerprint": "SHA256:" + "A" * 43,
    }
    monkeypatch.setattr(
        managed_repository_module,
        "_validated_payload",
        lambda _payload: validated,
    )
    backend = _RecordingBackend()
    # Setup fails before any key transfer/agent launch.
    backend.execute_with_secret_stdin = MagicMock(return_value=False)

    with pytest.raises(ManagedRepositoryMaterializationError) as exc:
        materialize_managed_repository_credentials([payload], backend)

    assert exc.value.code == "managed_repository_materialization_failed"
    assert private_key == bytearray(len(private_key))
    assert "private_key" not in payload


def test_internal_transport_repr_and_errors_are_secret_free() -> None:
    from orchestrator.schemas.job_runtime import JobStartRequest

    authority = _authority()
    payload = _runtime_payload(authority)
    request = JobStartRequest(
        job_id=str(uuid4()),
        description="secret-free diagnostic projection",
        managed_repository_credentials=[payload],
    )

    assert payload["private_key"] not in repr(request)
    credential = ManagedRepositoryMaterializationError(
        "managed_repository_workspace_probe_failed"
    )
    assert payload["private_key"] not in str(credential)


def test_vm_transport_requires_an_explicit_routed_ssh_endpoint(monkeypatch) -> None:
    authority = _authority()
    monkeypatch.setenv("GITEA_INTERNAL_URL", "http://gitea:3000")
    monkeypatch.setenv("GITEA_SSH_INTERNAL_HOST", "gitea")
    monkeypatch.delenv("GITEA_SSH_EXTERNAL_HOST", raising=False)

    with pytest.raises(ManagedRepositoryAuthorityError) as exc:
        runtime_credential(authority, backend="vm")

    assert exc.value.code == "repository_ssh_endpoint_unavailable"


def test_vm_transport_uses_only_the_explicit_routed_ssh_endpoint(monkeypatch) -> None:
    authority = _authority()
    monkeypatch.setenv("GITEA_INTERNAL_URL", "http://gitea:3000")
    monkeypatch.setenv("GITEA_SSH_INTERNAL_HOST", "gitea")
    monkeypatch.setenv("GITEA_SSH_EXTERNAL_HOST", "gitea-routed.example.test")
    monkeypatch.setenv("GITEA_SSH_EXTERNAL_PORT", "32222")

    credential = runtime_credential(authority, backend="vm")

    assert credential.ssh_host == "gitea-routed.example.test"
    assert credential.ssh_port == 32222
    assert credential.alias == f"srw-repo-{str(authority['id']).replace('-', '')}"
    assert not repository_url_has_credentials(credential.clone_url)


def test_secret_stdin_output_is_drained_without_logs_or_return_value(caplog) -> None:
    secret = b"private-material-must-not-escape"
    backend = object.__new__(RemoteBackend)

    output, return_code = backend._drain_exec_channel(
        _SecretOutputChannel(secret),
        "trusted bootstrap",
        1,
        sensitive=True,
    )

    assert output == ""
    assert return_code == 1
    assert secret.decode() not in caplog.text


def test_workspace_materialization_rejects_credentialed_or_cross_repo_transport() -> (
    None
):
    authority = _authority()
    payload = _runtime_payload(authority)
    payload["clone_url"] = (
        f"ssh://admin:secret@{payload['alias']}/srw/foreign-project.git"
    )

    with pytest.raises(ManagedRepositoryMaterializationError) as exc:
        materialize_managed_repository_credentials([payload], _RecordingBackend())

    assert exc.value.code == "managed_repository_credential_invalid"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("access_mode", "admin"),
        ("ssh_host", "gitea\nProxyCommand exfiltrate"),
        ("public_key_fingerprint", "SHA256:not-exact"),
        ("alias", "srw-repo-foreign"),
    ],
)
def test_workspace_materialization_rejects_mode_and_ssh_config_injection(
    field: str, value: str
) -> None:
    payload = _runtime_payload(_authority())
    payload[field] = value

    with pytest.raises(ManagedRepositoryMaterializationError) as exc:
        materialize_managed_repository_credentials([payload], _RecordingBackend())

    assert exc.value.code == "managed_repository_credential_invalid"
    assert "private_key" not in payload


@pytest.mark.asyncio
async def test_managed_project_knowledge_repo_is_not_granted_to_job_runtime(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GITEA_SSH_INTERNAL_HOST", "gitea")
    monkeypatch.setenv("GITEA_SSH_INTERNAL_PORT", "2222")
    job_authority = _authority(repo_name="job-12345678")
    knowledge_authority = _authority(repo_name="project-abcdef12-knowledge")
    project_id = str(uuid4())
    repository_id = str(uuid4())
    job = {
        "id": "12345678-1111-4111-8111-111111111111",
        "repo_name": job_authority["repo_name"],
        "project_id": project_id,
        "context": {"git_remote_url": job_authority["clean_repo_url"]},
    }
    repository = {
        "id": repository_id,
        "project_id": project_id,
        "name": knowledge_authority["repo_name"],
        "repo_url": knowledge_authority["clean_repo_url"],
        "role": "knowledge",
        "is_managed": True,
        "credentials": {"password": "forged"},
    }
    db = MagicMock()
    db.managed_repository_scope_is_unambiguous = AsyncMock(return_value=True)
    db.get_project_repositories = AsyncMock(return_value=[])
    db.reserve_managed_repository_authority = AsyncMock(
        side_effect=[job_authority, knowledge_authority]
    )
    db.scrub_job_managed_repository_url = AsyncMock()
    db.scrub_project_managed_repository_url = AsyncMock()
    gitea = MagicMock()
    gitea.repository_owner = "srw"
    gitea.clean_repo_url = MagicMock(
        side_effect=lambda name: f"http://gitea:3000/srw/{name}.git"
    )

    _url, rendered, credentials = await authorize_job_repository_transport(
        db, gitea, job, [repository], backend="sandbox"
    )

    assert len(credentials) == 1
    assert credentials[0]["repo_name"] == job_authority["repo_name"]
    assert rendered == [
        {
            **repository,
            "repo_url": knowledge_authority["clean_repo_url"],
            "credentials": None,
        }
    ]


@pytest.mark.asyncio
async def test_persistent_runtime_receives_no_unused_project_attachment_key(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GITEA_SSH_INTERNAL_HOST", "gitea")
    monkeypatch.setenv("GITEA_SSH_INTERNAL_PORT", "2222")
    thread_id = str(uuid4())
    thread_authority = _authority(repo_name=f"thread-{thread_id[:8]}")
    thread_authority["authority_kind"] = "thread"
    thread_authority["authority_id"] = thread_id
    source = {
        "id": str(uuid4()),
        "project_id": str(uuid4()),
        "name": "project-source",
        "repo_url": "http://gitea:3000/srw/project-source.git",
        "role": "source",
        "read_only": False,
        "is_managed": True,
        "credentials": {"password": "forged"},
    }
    thread = {
        "id": thread_id,
        "project_id": source["project_id"],
        "metadata": {
            "workspace_container": {
                "repo_name": thread_authority["repo_name"],
                "git_remote_url": thread_authority["clean_repo_url"],
            }
        },
    }
    db = MagicMock()
    db.managed_repository_scope_is_unambiguous = AsyncMock(return_value=True)
    db.reserve_managed_repository_authority = AsyncMock(return_value=thread_authority)
    db.scrub_thread_managed_repository_url = AsyncMock()
    gitea = MagicMock()
    gitea.repository_owner = "srw"
    gitea.clean_repo_url = MagicMock(
        side_effect=lambda name: f"http://gitea:3000/srw/{name}.git"
    )

    primary_url, rendered, credentials = await authorize_thread_repository_transport(
        db, gitea, thread, [source], backend="sandbox"
    )

    assert primary_url == credentials[0]["clone_url"]
    assert [item["repo_name"] for item in credentials] == [
        thread_authority["repo_name"]
    ]
    assert rendered == [
        {
            **source,
            "repo_url": "http://gitea:3000/srw/project-source.git",
            "credentials": None,
        }
    ]
    db.reserve_managed_repository_authority.assert_awaited_once()


@pytest.mark.asyncio
async def test_job_runtime_receives_exact_read_mode_for_read_only_attachment(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GITEA_SSH_INTERNAL_HOST", "gitea")
    monkeypatch.setenv("GITEA_SSH_INTERNAL_PORT", "2222")
    project_id = str(uuid4())
    job_authority = _authority()
    job_authority["repo_name"] = f"job-{str(job_authority['authority_id'])[:8]}"
    job_authority["clean_repo_url"] = (
        f"http://gitea:3000/srw/{job_authority['repo_name']}.git"
    )
    read_authority = _authority(repo_name="project-reference")
    read_authority["authority_kind"] = "project_repository"
    read_authority["authority_id"] = uuid4()
    read_authority["project_id"] = project_id
    read_authority["access_mode"] = "read"
    repository = {
        "id": str(read_authority["authority_id"]),
        "project_id": project_id,
        "name": read_authority["repo_name"],
        "repo_url": read_authority["clean_repo_url"],
        "role": "reference",
        "read_only": True,
        "is_managed": True,
        "credentials": None,
    }
    job = {
        "id": str(job_authority["authority_id"]),
        "repo_name": job_authority["repo_name"],
        "project_id": project_id,
        "context": {"git_remote_url": job_authority["clean_repo_url"]},
    }
    db = MagicMock()
    db.managed_repository_scope_is_unambiguous = AsyncMock(return_value=True)
    db.get_project_repositories = AsyncMock(return_value=[])
    db.reserve_managed_repository_authority = AsyncMock(
        side_effect=[job_authority, read_authority]
    )
    db.scrub_job_managed_repository_url = AsyncMock()
    db.scrub_project_managed_repository_url = AsyncMock()
    gitea = MagicMock()
    gitea.repository_owner = "srw"

    _primary_url, rendered, credentials = await authorize_job_repository_transport(
        db, gitea, job, [repository], backend="sandbox"
    )

    by_name = {item["repo_name"]: item for item in credentials}
    assert by_name[job_authority["repo_name"]]["access_mode"] == "write"
    assert by_name[read_authority["repo_name"]]["access_mode"] == "read"
    assert rendered[0]["repo_url"] == by_name[read_authority["repo_name"]]["clone_url"]


@pytest.mark.asyncio
async def test_ambiguous_credentialed_primary_url_never_reaches_runtime() -> None:
    db = MagicMock()
    gitea = MagicMock()
    job = {
        "id": str(uuid4()),
        "context": {
            "git_remote_url": "http://shared-admin:secret@gitea/srw/legacy.git"
        },
    }

    with pytest.raises(ManagedRepositoryAuthorityError) as exc:
        await authorize_job_repository_transport(
            db, gitea, job, None, backend="sandbox"
        )

    assert exc.value.code == "credentialed_repository_transport_refused"


def test_same_cluster_vm_transport_uses_the_internal_ssh_endpoint(monkeypatch) -> None:
    """A same-cluster VM sits on the pod network: it reaches the internal Gitea
    SSH service like a container, and the routed endpoint may not exist."""
    authority = _authority()
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setenv("GITEA_INTERNAL_URL", "http://gitea:3000")
    monkeypatch.setenv("GITEA_SSH_INTERNAL_HOST", "gitea")
    monkeypatch.setenv("GITEA_SSH_INTERNAL_PORT", "2222")
    monkeypatch.delenv("GITEA_SSH_EXTERNAL_HOST", raising=False)

    credential = runtime_credential(authority, backend="vm")

    assert credential.ssh_host == "gitea"
    assert credential.ssh_port == 2222


def test_external_vm_transport_still_requires_the_routed_endpoint(monkeypatch) -> None:
    authority = _authority()
    monkeypatch.setenv("VM_MODE", "external")
    monkeypatch.setenv("GITEA_INTERNAL_URL", "http://gitea:3000")
    monkeypatch.setenv("GITEA_SSH_INTERNAL_HOST", "gitea")
    monkeypatch.delenv("GITEA_SSH_EXTERNAL_HOST", raising=False)

    with pytest.raises(ManagedRepositoryAuthorityError) as exc:
        runtime_credential(authority, backend="vm")

    assert exc.value.code == "repository_ssh_endpoint_unavailable"


@pytest.mark.asyncio
async def test_primary_authority_reads_the_full_row_when_repo_name_is_missing(
    monkeypatch,
) -> None:
    """A dispatcher projection without repo_name must not be mistaken for a job
    without a managed scope — that silently ships a credential-less remote."""
    from unittest.mock import AsyncMock

    from orchestrator.services import managed_repository_authority as mra

    seen: list[dict] = []

    async def _prepare(postgres_db, gitea_client, job):
        seen.append(dict(job))
        return {"id": "authority-1"}

    monkeypatch.setattr(mra, "prepare_job_repository_authority", _prepare)
    db = AsyncMock()
    db.get_job = AsyncMock(return_value={"id": "job-1", "repo_name": "job-12345678"})

    result = await mra.prepare_job_primary_repository_authority(
        db, object(), {"id": "job-1", "status": "created"}
    )

    assert result == {"id": "authority-1"}
    db.get_job.assert_awaited_once_with("job-1")
    assert seen and seen[0]["repo_name"] == "job-12345678"
