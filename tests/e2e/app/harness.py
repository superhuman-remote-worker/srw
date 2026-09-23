#!/usr/bin/env python3
"""Owned-cluster lifecycle for the full-stack Cockpit application journey.

The destructive boundary in this module is intentionally small and unit-testable.
Only a prefix-valid cluster recorded in a mode-0600 ownership ledger, whose
current k3d server container id still matches that ledger, can be deleted.
"""

from __future__ import annotations

import argparse
import base64
import dataclasses
import datetime as dt
import fcntl
import ipaddress
import json
import os
import re
import secrets
import selectors
import shutil
import stat
import subprocess
import sys
import tarfile
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from typing import Any, Final

REPO_ROOT: Final = Path(__file__).resolve().parents[3]
ASSET_ROOT: Final = REPO_ROOT / "tests/e2e/app"
K3D_TEMPLATE: Final = ASSET_ROOT / "k3d.yaml"
VALUES_FILE: Final = ASSET_ROOT / "values-e2e.yaml"
STATELESS_SANDBOX_VALUES_FILE: Final = ASSET_ROOT / "values-stateless-sandbox.yaml"
FORGE_SANDBOX_VALUES_FILE: Final = ASSET_ROOT / "values-forge-sandbox.yaml"
CLOUD_SANDBOX_VALUES_FILE: Final = ASSET_ROOT / "values-cloud-sandbox.yaml"
OFFICER_WATCHDOG_VALUES_FILE: Final = ASSET_ROOT / "values-officer-watchdog.yaml"
SESSION_ATTENTION_VALUES_FILE: Final = ASSET_ROOT / "values-session-attention.yaml"
PROVIDER_MANIFEST: Final = ASSET_ROOT / "deterministic_provider/kubernetes.yaml"
PROVIDER_DOCKERFILE: Final = ASSET_ROOT / "deterministic_provider/Dockerfile"
PLAYWRIGHT_RUNNER_DOCKERFILE: Final = ASSET_ROOT / "Dockerfile.playwright"

OWNER: Final = "srw-application-e2e-harness/v1"
CLUSTER_PREFIX: Final = "srw-e2e-"
CLUSTER_RE: Final = re.compile(r"^srw-e2e-[a-z0-9](?:[a-z0-9-]{5,38}[a-z0-9])$")
RUN_ID_RE: Final = re.compile(r"^[a-z0-9][a-z0-9-]{7,48}$")
K3D_VERSION: Final = "v5.8.3"
K3S_IMAGE: Final = "rancher/k3s:v1.31.5-k3s1"
DEPENDENCY_IMAGES: Final = (
    "busybox:1.36",
    "postgres:15",
    "pgvector/pgvector:pg15",
    "quay.io/keycloak/keycloak:26.2",
    # k3s local-path creates one helper pod per PVC from this image. Import it
    # explicitly so storage provisioning does not depend on node-level DNS.
    "rancher/mirrored-library-busybox:1.36.1",
)
PLAYWRIGHT_VERSION_FILE: Final = REPO_ROOT / ".playwright-version"
NAMESPACE: Final = "srw-e2e"
# helm/Chart.lock pins the Collabora subchart. With a lock file present,
# `helm dependency build` refuses to resolve a repository URL that is not
# registered, so the repo must be added first on every fresh runner.
COLLABORA_HELM_REPOSITORY: Final = "https://collaboraonline.github.io/online"
RELEASE: Final = "srw-e2e"
BASE_HOST: Final = "srw-e2e.test"
BASE_URL: Final = f"http://{BASE_HOST}"
PROVIDER_IMAGE_PLACEHOLDER: Final = "srw-e2e-model-fixture:local"
# The protected-effect lane's dedicated HMAC Secret. The cloud profile names
# it through `nextcloud.protectedEffect.hmacSecretName`; the chart then does
# not create one, which is what lets the lane render under `secrets.create:
# false`.
PROTECTED_EFFECT_SECRET_NAME: Final = "srw-e2e-protected-effect"
PROVIDER_SERVICE_BASE: Final = (
    "http://srw-e2e-model-fixture.srw-e2e.svc.cluster.local:8000/v1"
)
DEFAULT_STATE_ROOT: Final = REPO_ROOT / "cockpit/test-results/app-harness"
STATE_ROOT_MARKER: Final = ".srw-application-e2e-root.json"
RUN_DIRECTORY_MARKER: Final = ".srw-application-e2e-run.json"
STATE_LOCK_FILE: Final = ".srw-application-e2e.lock"
DEFAULT_PROFILE_NAME: Final = "pinned-virtual"

SENSITIVE_LINE_MARKERS: Final = (
    '"authorization"',
    '"cookie"',
    '"messages"',
    '"prompt"',
    '"content"',
    '"password"',
    '"access_token"',
    '"refresh_token"',
    '"id_token"',
    '"api_key"',
    '"input"',
    '"tool_arguments"',
    '"system_prompt"',
    '"user_message"',
    "authorization:",
    "cookie:",
    "srw_runtime_actor_bootstrap",
    "_password=",
    "_token=",
    "_secret=",
    "_api_key=",
)


class HarnessError(RuntimeError):
    """An operator-safe harness failure."""


class SafetyError(HarnessError):
    """A fail-closed ownership/origin violation."""


@dataclasses.dataclass(frozen=True)
class ApplicationE2EProfile:
    name: str
    values_files: tuple[Path, ...]
    workspace_backend: str
    execution_lane: str
    include_workspace_image: bool = False
    additional_deployments: tuple[str, ...] = ()
    #: Extra StatefulSets whose rollout must settle before the journey runs.
    #: Deployments and StatefulSets are separate `kubectl rollout status`
    #: resource kinds, so a forge cannot be waited on through the field above.
    additional_statefulsets: tuple[str, ...] = ()
    #: This profile runs the stateless executor Deployment. A behavioural flag
    #: rather than a name comparison so that adding a profile which composes
    #: the stateless overlay cannot silently skip the executor image check.
    stateless_agents: bool = False
    #: This profile deploys the bundled forge. Project provisioning, project
    #: repositories and knowledge-vault materialisation all go through it.
    forge_enabled: bool = False
    #: This profile deploys a bundled main-cloud backend, so ``MAIN_CLOUD_BACKEND``
    #: resolves and the cloud mount/stage/settings surfaces answer for real
    #: rather than as "no backend bound". A behavioural flag, not a name
    #: comparison, so a later profile composing this overlay cannot silently
    #: skip the backend check.
    cloud_enabled: bool = False
    #: This profile turns protected cloud mode on, so a marked session is
    #: actually admitted: an RO reader is provisioned, the capture overlay is
    #: built, and ``cloud_mount`` reaches the workspace runtime. A bound backend
    #: alone does not do this -- with the mode off every protected route answers
    #: the "disabled" refusal and the mount builders are never asked for a
    #: payload. Behavioural, for the same reason as the flag above.
    protected_cloud_enabled: bool = False
    #: This profile lets the shared durable lifecycle owner accept an
    #: *automatic* submission. The Officer watchdog's third duty hands a
    #: missing runtime to that owner rather than repairing it itself, and the
    #: owner drops every automatic submission while the chart default
    #: (``PERSISTENT_AGENT_RECONCILIATION_ENABLED: false``) stands, so the duty
    #: is unobservable without it. Behavioural, for the same reason as the
    #: flags above: a later profile composing this overlay must not silently
    #: lose the property the check depends on.
    persistent_reconciliation_enabled: bool = False


APPLICATION_E2E_PROFILES: Final = {
    DEFAULT_PROFILE_NAME: ApplicationE2EProfile(
        name=DEFAULT_PROFILE_NAME,
        values_files=(VALUES_FILE,),
        workspace_backend="virtual",
        execution_lane="pinned",
    ),
    "stateless-sandbox": ApplicationE2EProfile(
        name="stateless-sandbox",
        values_files=(VALUES_FILE, STATELESS_SANDBOX_VALUES_FILE),
        workspace_backend="sandbox",
        execution_lane="stateless",
        include_workspace_image=True,
        additional_deployments=("srw-e2e-agent-stateless",),
        stateless_agents=True,
    ),
    "forge-sandbox": ApplicationE2EProfile(
        name="forge-sandbox",
        values_files=(
            VALUES_FILE,
            STATELESS_SANDBOX_VALUES_FILE,
            FORGE_SANDBOX_VALUES_FILE,
        ),
        workspace_backend="sandbox",
        execution_lane="stateless",
        include_workspace_image=True,
        additional_deployments=("srw-e2e-agent-stateless",),
        additional_statefulsets=("srw-e2e-gitea",),
        stateless_agents=True,
        forge_enabled=True,
    ),
    "cloud-sandbox": ApplicationE2EProfile(
        name="cloud-sandbox",
        values_files=(
            VALUES_FILE,
            STATELESS_SANDBOX_VALUES_FILE,
            FORGE_SANDBOX_VALUES_FILE,
            CLOUD_SANDBOX_VALUES_FILE,
        ),
        workspace_backend="sandbox",
        execution_lane="stateless",
        include_workspace_image=True,
        additional_deployments=("srw-e2e-agent-stateless", "srw-e2e-nextcloud"),
        # Garage is the bundled object store. It is waited on here, not merely
        # deployed: staging a captured overlay writes a tar through
        # `snapshot_service.upload_blob_file`, so a protected session whose
        # store is not up yet stages nothing and reports an empty diff.
        additional_statefulsets=("srw-e2e-gitea", "srw-e2e-garage"),
        stateless_agents=True,
        forge_enabled=True,
        cloud_enabled=True,
        protected_cloud_enabled=True,
    ),
    "officer-watchdog": ApplicationE2EProfile(
        name="officer-watchdog",
        values_files=(
            VALUES_FILE,
            STATELESS_SANDBOX_VALUES_FILE,
            FORGE_SANDBOX_VALUES_FILE,
            OFFICER_WATCHDOG_VALUES_FILE,
        ),
        workspace_backend="sandbox",
        execution_lane="stateless",
        include_workspace_image=True,
        additional_deployments=("srw-e2e-agent-stateless",),
        additional_statefulsets=("srw-e2e-gitea",),
        stateless_agents=True,
        forge_enabled=True,
        persistent_reconciliation_enabled=True,
    ),
    # R1.B10's live gate: permission reminders reach a local mail sink, the
    # Officer ceiling reads a real usage ledger, and workspace suspension (so
    # pinned attention sleep and its magic-link wake) has a snapshot store.
    "session-attention": ApplicationE2EProfile(
        name="session-attention",
        values_files=(
            VALUES_FILE,
            STATELESS_SANDBOX_VALUES_FILE,
            FORGE_SANDBOX_VALUES_FILE,
            SESSION_ATTENTION_VALUES_FILE,
        ),
        workspace_backend="sandbox",
        execution_lane="stateless",
        include_workspace_image=True,
        additional_deployments=("srw-e2e-agent-stateless", "srw-e2e-greenmail"),
        additional_statefulsets=(
            "srw-e2e-gitea",
            "srw-e2e-garage",
            "srw-e2e-auditdb",
        ),
        stateless_agents=True,
        forge_enabled=True,
    ),
}


def resolve_profile(name: str) -> ApplicationE2EProfile:
    profile = APPLICATION_E2E_PROFILES.get(name)
    if profile is None:
        raise SafetyError("unknown application E2E profile")
    return profile


def profile_from_ledger(ledger: Mapping[str, Any]) -> ApplicationE2EProfile:
    # Schema-1 ledgers written before profiles existed are pinned-virtual.
    raw = ledger.get("profile", DEFAULT_PROFILE_NAME)
    if not isinstance(raw, str):
        raise SafetyError("ownership ledger profile is invalid")
    return resolve_profile(raw)


@dataclasses.dataclass(frozen=True)
class CommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


class CommandRunner:
    """Subprocess boundary that never includes stdin or secret values in errors."""

    def run(
        self,
        argv: Sequence[str],
        *,
        cwd: Path = REPO_ROOT,
        env: Mapping[str, str] | None = None,
        input_text: str | None = None,
        check: bool = True,
        timeout: float | None = None,
        label: str | None = None,
    ) -> CommandResult:
        merged_env = os.environ.copy()
        if env:
            merged_env.update(env)
        try:
            completed = subprocess.run(
                list(argv),
                cwd=cwd,
                env=merged_env,
                input=input_text,
                text=True,
                capture_output=True,
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            operation = label or Path(argv[0]).name
            raise HarnessError(f"{operation} exceeded its bounded timeout") from exc
        result = CommandResult(
            returncode=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
        )
        if check and result.returncode:
            operation = label or Path(argv[0]).name
            raise HarnessError(f"{operation} failed with exit code {result.returncode}")
        return result


@dataclasses.dataclass(repr=False)
class SecretBundle:
    admin_username: str
    admin_password: str = dataclasses.field(repr=False)
    journey_username: str
    journey_password: str = dataclasses.field(repr=False)
    app_encryption_key: str = dataclasses.field(repr=False)
    postgres_password: str = dataclasses.field(repr=False)
    vector_password: str = dataclasses.field(repr=False)
    audit_password: str = dataclasses.field(repr=False)
    keycloak_db_password: str = dataclasses.field(repr=False)
    realm_test_password: str = dataclasses.field(repr=False)
    keycloak_client_secret: str = dataclasses.field(repr=False)
    provider_api_key: str = dataclasses.field(repr=False)
    provider_control_token: str = dataclasses.field(repr=False)
    session_jwt_secret: str = dataclasses.field(repr=False)
    mcp_internal_key: str = dataclasses.field(repr=False)
    gitea_oidc_secret: str = dataclasses.field(repr=False)
    nextcloud_oidc_secret: str = dataclasses.field(repr=False)
    nextcloud_admin_password: str = dataclasses.field(repr=False)
    #: The `agent-service` account's password. Required, not decorative: the
    #: orchestrator's main-cloud loader lists `agent_password` in
    #: `_REQUIRED_SECRET_ENVS["nextcloud"]`, and without it the installation
    #: authority never initialises -- so the backend is *bound* but every cloud
    #: effect refuses with "does not support durable active backend-instance
    #: authority". Nextcloud's own setup hook creates `agent-service` with this
    #: value, so both halves must read the same key or the orchestrator
    #: authenticates as an account whose password only Nextcloud knows.
    nextcloud_agent_password: str = dataclasses.field(repr=False)
    #: Root key for per-workspace code-server credentials. The orchestrator's
    #: `secretKeyRef` for `IDE_CREDENTIAL_KEY` is `optional: true` and
    #: `services/ide_proxy.py` fails *closed* without it: the thread's IDE
    #: status answers `ide_credential_key_unconfigured` and withholds the URL
    #: rather than handing out one the proxy would refuse. That is the correct
    #: production default and a silent hole in a fixture -- the whole browser
    #: IDE lane (HTTP proxy, WebSocket upgrade, content and stream delivery)
    #: is unreachable, and nothing fails to say so. R1.B05 shipped with exactly
    #: that gap as a recorded qualification. Minted unconditionally like the
    #: Gitea/Nextcloud keys so the secret topology does not vary by profile.
    #:
    #: This one belongs in ``app_secret_data()`` and not in its own Secret, in
    #: deliberate contrast to ``protected_effect_hmac_key``: the chart puts
    #: `IDE_CREDENTIAL_KEY` in the same application Secret in production
    #: (`helm/templates/secret.yaml`), so a dedicated Secret here would make the
    #: fixture exercise a topology no deployment has. The wider question of
    #: agent Pods receiving the whole bundle through ``envFrom`` is a real and
    #: separate posture issue; it is not resolved by making this profile lie.
    ide_credential_key: str = dataclasses.field(repr=False)
    gitea_admin_password: str = dataclasses.field(repr=False)
    #: The protected-effect lane's adoption-once HMAC root. Deliberately NOT a
    #: member of ``app_secret_data()``: dynamic agent Pods consume that bundle
    #: through ``envFrom``, and the effect root must never be readable from a
    #: workspace. It is minted into its own Secret, named by the cloud profile's
    #: ``nextcloud.protectedEffect.hmacSecretName``.
    protected_effect_hmac_key: str = dataclasses.field(repr=False)
    #: The bundled object store's credentials. Garage's `key/import` accepts
    #: only its native format, and `secret.yaml` refuses anything else rather
    #: than generating a replacement, so these are not free-form tokens:
    #: the RPC secret and each S3 secret are 64 hex characters, and an access
    #: key id is "GK" followed by 24. An id and its secret are one credential
    #: and are minted together.
    garage_rpc_secret: str = dataclasses.field(repr=False)
    garage_admin_token: str = dataclasses.field(repr=False)
    snapshot_s3_access_key_id: str = dataclasses.field(repr=False)
    snapshot_s3_secret_access_key: str = dataclasses.field(repr=False)
    virtual_workspace_s3_access_key_id: str = dataclasses.field(repr=False)
    virtual_workspace_s3_secret_access_key: str = dataclasses.field(repr=False)

    @classmethod
    def generate(cls, run_id: str) -> SecretBundle:
        suffix = run_id[-10:]

        def token(size: int = 36) -> str:
            return secrets.token_urlsafe(size)

        return cls(
            admin_username=f"e2e-admin-{suffix}",
            admin_password=token(),
            journey_username=f"e2e-user-{suffix}",
            journey_password=token(),
            app_encryption_key=base64.urlsafe_b64encode(os.urandom(32)).decode(),
            postgres_password=token(),
            vector_password=token(),
            audit_password=token(),
            keycloak_db_password=token(),
            realm_test_password=token(),
            keycloak_client_secret=token(48),
            provider_api_key=token(48),
            provider_control_token=token(48),
            session_jwt_secret=token(48),
            mcp_internal_key=token(48),
            gitea_oidc_secret=token(48),
            nextcloud_oidc_secret=token(48),
            nextcloud_admin_password=token(24),
            nextcloud_agent_password=token(24),
            # `secret.yaml` generates randAlphaNum(48) for this key when the
            # chart owns the Secret; match that length here.
            ide_credential_key=token(36),
            gitea_admin_password=token(36),
            # The chart floor is 32 bytes; 48 matches its own randAlphaNum(48).
            protected_effect_hmac_key=token(48),
            garage_rpc_secret=secrets.token_hex(32),
            garage_admin_token=token(24),
            snapshot_s3_access_key_id=f"GK{secrets.token_hex(12)}",
            snapshot_s3_secret_access_key=secrets.token_hex(32),
            virtual_workspace_s3_access_key_id=f"GK{secrets.token_hex(12)}",
            virtual_workspace_s3_secret_access_key=secrets.token_hex(32),
        )

    @classmethod
    def from_json(cls, value: Mapping[str, Any]) -> SecretBundle:
        fields = {field.name for field in dataclasses.fields(cls)}
        if set(value) != fields or not all(
            isinstance(value[key], str) for key in fields
        ):
            raise SafetyError("credential file has an unexpected schema")
        return cls(**{key: value[key] for key in fields})

    def secret_values(self) -> tuple[str, ...]:
        return tuple(
            str(getattr(self, field.name))
            for field in dataclasses.fields(self)
            if field.repr is False
        )

    def app_secret_data(self) -> dict[str, str]:
        # Empty vendor keys are deliberate: the owned gate must have no usable
        # real-provider credential even when the parent shell does.
        return {
            "APP_ENCRYPTION_KEY": self.app_encryption_key,
            "POSTGRES_USER": "srw",
            "POSTGRES_PASSWORD": self.postgres_password,
            "VECTOR_POSTGRES_USER": "srw",
            "VECTOR_POSTGRES_PASSWORD": self.vector_password,
            "AUDIT_POSTGRES_USER": "srw",
            "AUDIT_POSTGRES_PASSWORD": self.audit_password,
            "KEYCLOAK_ADMIN_USER": "e2e-bootstrap-admin",
            "KEYCLOAK_ADMIN_PASSWORD": self.admin_password,
            "KC_ADMIN_USER": "e2e-bootstrap-admin",
            "KC_ADMIN_PASSWORD": self.admin_password,
            "KC_DB_PASSWORD": self.keycloak_db_password,
            "KC_REALM_ADMIN_PASSWORD": self.realm_test_password,
            "KC_CLIENT_SECRET": self.keycloak_client_secret,
            "GITEA_OIDC_CLIENT_SECRET": self.gitea_oidc_secret,
            "NEXTCLOUD_OIDC_CLIENT_SECRET": self.nextcloud_oidc_secret,
            "NEXTCLOUD_ADMIN_USER": "admin",
            "NEXTCLOUD_ADMIN_PASSWORD": self.nextcloud_admin_password,
            # Not optional in any sense that matters. The orchestrator's
            # `secretKeyRef` for it is `optional: true`, so a render check
            # cannot see it missing -- but `services/cloud/config.py` lists
            # `agent_password` as a REQUIRED secret for the nextcloud backend,
            # and an unset one leaves the installation authority uninitialised.
            # The backend then reports itself bound while refusing every effect
            # with "does not support 'durable active backend-instance
            # authority'": no project cloud folder, no user home, no protected
            # mount. Nextcloud's setup hook creates `agent-service` with this
            # exact value, so the two sides must agree.
            "NEXTCLOUD_AGENT_PASSWORD": self.nextcloud_agent_password,
            # Minted unconditionally like the rest: the profile that enables
            # the bundled object store pulls these in, and a key the harness
            # does not mint is a pod that never starts.
            "GARAGE_RPC_SECRET": self.garage_rpc_secret,
            "GARAGE_ADMIN_TOKEN": self.garage_admin_token,
            "SNAPSHOT_S3_ACCESS_KEY_ID": self.snapshot_s3_access_key_id,
            "SNAPSHOT_S3_SECRET_ACCESS_KEY": self.snapshot_s3_secret_access_key,
            "VIRTUAL_WORKSPACE_S3_ACCESS_KEY_ID": (
                self.virtual_workspace_s3_access_key_id
            ),
            "VIRTUAL_WORKSPACE_S3_SECRET_ACCESS_KEY": (
                self.virtual_workspace_s3_secret_access_key
            ),
            # Minted unconditionally, like the Gitea one above: the Keycloak
            # bootstrap job mounts NEXTCLOUD_OIDC_CLIENT_SECRET by key whenever
            # `nextcloud.enabled`, and a missing key is a
            # CreateContainerConfigError that stalls every pod waiting on
            # Keycloak — which is every pod. Minting it always keeps the secret
            # shape identical across profiles.
            # The orchestrator keeps non-optional bootstrap refs for these
            # even when the bundled Gitea workload is disabled.
            "GITEA_ADMIN_USER": "e2e-gitea-admin",
            "GITEA_ADMIN_PASSWORD": self.gitea_admin_password,
            "SESSION_JWT_SECRET": self.session_jwt_secret,
            "MCP_INTERNAL_KEY": self.mcp_internal_key,
            # Without this the browser IDE lane cannot be exercised at all --
            # see the field's own note. `optional: true` on the chart side
            # means no render check can catch its absence, so
            # `test_generated_app_secret_mints_the_ide_credential_root` guards
            # it the same way the Nextcloud agent password is guarded.
            "IDE_CREDENTIAL_KEY": self.ide_credential_key,
            "OPENAI_API_KEY": "",
            "ANTHROPIC_API_KEY": "",
            "GROQ_API_KEY": "",
            "TAVILY_API_KEY": "",
        }

    def browser_environment(self) -> dict[str, str]:
        return {
            "APP_E2E_USERNAME": self.journey_username,
            "APP_E2E_PASSWORD": self.journey_password,
            "APP_E2E_ADMIN_USERNAME": self.admin_username,
            "APP_E2E_ADMIN_PASSWORD": self.admin_password,
            "APP_E2E_CONTROL_TOKEN": self.provider_control_token,
            "APP_E2E_PROVIDER_BASE_URL": PROVIDER_SERVICE_BASE,
            "APP_E2E_CHAT_MODEL": "e2e-chat",
            "APP_E2E_EMBEDDING_MODEL": "e2e-embedding",
        }


def generated_secret_data(
    bundle: SecretBundle, *, vm_private_key: str, vm_public_key: str
) -> dict[str, dict[str, str]]:
    """Every Kubernetes Secret this harness mints, keyed by name.

    One source of truth. ``create_secrets_and_fixture`` applies exactly these
    manifests, and ``test_harness.py`` checks every non-optional
    ``secretKeyRef`` in every profile's render against them. A required key
    nothing mints is a ``CreateContainerConfigError``, and for anything a
    Keycloak-adjacent pod mounts that stalls every pod which waits on Keycloak
    — a deploy that never finishes rather than a failure that says so.

    All four are minted on every profile, so the secret topology does not vary
    with the overlay set. ``srw-e2e-protected-effect`` is deliberately its own
    Secret rather than a key in the application bundle: dynamic agent Pods
    consume that bundle wholesale through ``envFrom``, and the protected-effect
    root must never be readable from a workspace.
    """

    return {
        "srw-e2e-app-secrets": bundle.app_secret_data(),
        "srw-e2e-vm-ssh-key": {
            "ssh-privatekey": vm_private_key,
            "ssh-publickey": vm_public_key,
        },
        "srw-e2e-model-fixture": {
            "control-token": bundle.provider_control_token,
            "inference-api-key": bundle.provider_api_key,
        },
        PROTECTED_EFFECT_SECRET_NAME: {
            "NEXTCLOUD_PROTECTED_EFFECT_HMAC_KEY": bundle.protected_effect_hmac_key,
        },
    }


def utc_now() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds")


def new_run_id() -> str:
    timestamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d-%H%M%S")
    return f"{timestamp}-{secrets.token_hex(4)}"


def validate_run_id(run_id: str) -> str:
    if not RUN_ID_RE.fullmatch(run_id):
        raise SafetyError("invalid E2E run id")
    return run_id


def validate_cluster_name(cluster_name: str) -> str:
    if not CLUSTER_RE.fullmatch(cluster_name):
        raise SafetyError(
            f"refusing cluster name outside the owned {CLUSTER_PREFIX!r} namespace"
        )
    if cluster_name in {"srw", "srw-e2e", "k3s-default"}:
        raise SafetyError("refusing a shared or non-unique cluster name")
    return cluster_name


def validate_origin(
    raw_url: str,
    *,
    allow_remote: bool = False,
    owned_host: str | None = None,
) -> str:
    parsed = urllib.parse.urlsplit(raw_url)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
    ):
        raise SafetyError("APP_E2E_BASE_URL must be a plain HTTP(S) origin")
    hostname = parsed.hostname.rstrip(".").lower()
    local = hostname == "localhost" or hostname.endswith(".localhost")
    try:
        local = local or ipaddress.ip_address(hostname).is_loopback
    except ValueError:
        pass
    owned = owned_host is not None and hostname == owned_host.lower()
    if not (local or owned or allow_remote):
        raise SafetyError("remote E2E origins require explicit APP_E2E_ALLOW_REMOTE=1")
    return raw_url.rstrip("/")


def attach_docker_network(*origins: str) -> str:
    """Select Docker networking without confusing container and host loopback."""
    host_local = False
    for origin in origins:
        hostname = (urllib.parse.urlsplit(origin).hostname or "").lower()
        local = hostname == "localhost" or hostname.endswith(".localhost")
        try:
            local = local or ipaddress.ip_address(hostname).is_loopback
        except ValueError:
            pass
        host_local = host_local or local
    if not host_local:
        return "bridge"
    if not sys.platform.startswith("linux"):
        raise SafetyError(
            "loopback attach mode requires Linux Docker host networking; use an explicit reachable origin"
        )
    return "host"


def ensure_private_file(path: Path) -> None:
    if path.is_symlink() or not path.is_file():
        raise SafetyError(f"private path is not a regular file: {path.name}")
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode & 0o077:
        raise SafetyError(f"private file permissions are too broad: {path.name}")


def reject_existing_symlink_components(path: Path, label: str) -> None:
    absolute = Path(os.path.abspath(path.expanduser()))
    cursor = Path(absolute.anchor)
    for part in absolute.parts[1:]:
        cursor /= part
        if cursor.is_symlink():
            raise SafetyError(f"{label} must not contain symbolic links")


def _atomic_private_write(path: Path, content: str) -> None:
    if os.path.lexists(path) and (path.is_symlink() or not path.is_file()):
        raise SafetyError(f"refusing to replace non-file private path: {path.name}")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(4)}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        path.chmod(0o600)
    finally:
        if temporary.exists():
            temporary.unlink()


def write_private_json(path: Path, value: Any) -> None:
    _atomic_private_write(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def write_private_json_exclusive(path: Path, value: Any) -> None:
    """Publish a complete private JSON file only if no active owner exists."""
    content = json.dumps(value, indent=2, sort_keys=True) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{secrets.token_hex(4)}.claim")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError as exc:
            raise SafetyError("an E2E harness state is already active") from exc
    finally:
        if temporary.exists():
            temporary.unlink()


def read_private_json(path: Path) -> Any:
    ensure_private_file(path)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SafetyError(f"cannot read private JSON file {path.name}") from exc


def write_private_env(path: Path, values: Mapping[str, str]) -> None:
    lines: list[str] = []
    for key, value in sorted(values.items()):
        if not re.fullmatch(r"[A-Z][A-Z0-9_]*", key):
            raise SafetyError("invalid environment key")
        if "\n" in value or "\r" in value or "\x00" in value:
            raise SafetyError("environment value contains a forbidden character")
        lines.append(f"{key}={value}")
    _atomic_private_write(path, "\n".join(lines) + "\n")


def sanitize_diagnostic(text: str, known_secrets: Iterable[str] = ()) -> str:
    sanitized = text
    for secret in sorted(
        (value for value in known_secrets if value), key=len, reverse=True
    ):
        sanitized = sanitized.replace(secret, "[REDACTED]")
        encoded = urllib.parse.quote(secret, safe="")
        if encoded != secret:
            sanitized = sanitized.replace(encoded, "[REDACTED]")
    sanitized = re.sub(
        r"(?i)(authorization\s*[:=]\s*)(?:bearer\s+)?[^\s,;]+",
        r"\1[REDACTED]",
        sanitized,
    )
    sanitized = re.sub(
        r"""(?i)([?&](?:t|token|key|secret|password)=)[^&#\s"']+""",
        r"\1[REDACTED]",
        sanitized,
    )
    sanitized = re.sub(
        r"-----BEGIN [^-]+-----.*?-----END [^-]+-----",
        "[REDACTED PEM]",
        sanitized,
        flags=re.DOTALL,
    )
    sanitized = re.sub(
        r"\b[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{16,}\b",
        "[REDACTED JWT]",
        sanitized,
    )
    safe_lines: list[str] = []
    for line in sanitized.splitlines():
        lowered = line.lower()
        if any(marker in lowered for marker in SENSITIVE_LINE_MARKERS):
            safe_lines.append("[REDACTED LINE: potentially sensitive payload]")
        else:
            safe_lines.append(line)
    suffix = "\n" if sanitized.endswith("\n") else ""
    return "\n".join(safe_lines) + suffix


def sanitize_log_diagnostic(text: str, known_secrets: Iterable[str] = ()) -> str:
    """Retain severity/status metadata, never free-form application messages."""
    diagnostic = re.compile(
        r"(?:error|warn|fatal|fail(?:ed|ure)?|exception|traceback|timeout|unhealthy|not ready|restart)",
        re.IGNORECASE,
    )
    kept: list[str] = []
    for line in text.splitlines():
        match = diagnostic.search(line)
        if not match:
            continue
        timestamp = re.search(r"\b\d{4}-\d{2}-\d{2}[T ][0-9:.+-]+Z?\b", line)
        status = re.search(
            r"(?i)(?:status(?:_code)?\s*[:=]?\s*|HTTP/\d(?:\.\d)?\s+)([1-5]\d\d)\b",
            line,
        )
        fields = [f"severity={match.group(0).lower().replace(' ', '-')}"]
        if timestamp:
            fields.append(f"timestamp={timestamp.group(0)}")
        if status:
            fields.append(f"status={status.group(1)}")
        if re.search(r"\bE2E-[A-Za-z0-9_-]+", line):
            fields.append("run_correlated=true")
        kept.append("[sanitized log metadata] " + " ".join(fields))
    return sanitize_diagnostic("\n".join(kept) + ("\n" if kept else ""), known_secrets)


def bound_diagnostic(
    text: str, *, max_lines: int = 2_000, max_chars: int = 200_000
) -> str:
    lines = text.splitlines(keepends=True)
    truncated = len(lines) > max_lines
    result = "".join(lines[-max_lines:])
    if len(result) > max_chars:
        result = result[-max_chars:]
        truncated = True
    if truncated:
        result = "[older sanitized diagnostic output truncated]\n" + result
    return result


class StateStore:
    def __init__(self, root: Path):
        expanded = Path(os.path.abspath(root.expanduser()))
        reject_existing_symlink_components(expanded, "E2E state root")
        self.root = expanded
        self.active_path = self.root / "active.json"

    @property
    def root_marker(self) -> Path:
        return self.root / STATE_ROOT_MARKER

    @property
    def lock_path(self) -> Path:
        return self.root / STATE_LOCK_FILE

    @contextmanager
    def _active_lock(self) -> Iterator[None]:
        """Serialize every active-owner comparison and mutation across processes."""
        self._require_owned_root()
        flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(self.lock_path, flags, 0o600)
        except OSError as exc:
            raise SafetyError("cannot open the E2E state ownership lock") from exc
        try:
            lock_stat = os.fstat(descriptor)
            if (
                not stat.S_ISREG(lock_stat.st_mode)
                or stat.S_IMODE(lock_stat.st_mode) & 0o077
            ):
                raise SafetyError("E2E state ownership lock is not a private file")
            deadline = time.monotonic() + 30
            while True:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError as exc:
                    if time.monotonic() >= deadline:
                        raise HarnessError(
                            "timed out waiting for the E2E state ownership lock"
                        ) from exc
                    time.sleep(0.05)
            try:
                yield
            finally:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)

    def _require_owned_root(self) -> None:
        if self.root.is_symlink() or not self.root.is_dir():
            raise SafetyError("E2E state root is absent or not a regular directory")
        if stat.S_IMODE(self.root.stat().st_mode) & 0o077:
            raise SafetyError("E2E state root permissions are too broad")
        if self.root_marker.is_symlink() or not self.root_marker.is_file():
            raise SafetyError("pre-existing E2E state root is not harness-owned")
        marker = read_private_json(self.root_marker)
        if marker != {"schema": 1, "owner": OWNER, "kind": "state-root"}:
            raise SafetyError("E2E state root ownership marker is invalid")

    def _ensure_owned_root(self) -> None:
        if os.path.lexists(self.root):
            self._require_owned_root()
            return
        parent = self.root.parent
        default_root = DEFAULT_STATE_ROOT.resolve()
        if self.root == default_root and not os.path.lexists(parent):
            trusted_parent = parent.parent
            if trusted_parent.is_symlink() or not trusted_parent.is_dir():
                raise SafetyError("trusted E2E output parent is unavailable")
            parent.mkdir(mode=0o700)
        if parent.is_symlink() or not parent.is_dir():
            raise SafetyError("E2E state root parent must be an existing directory")
        self.root.mkdir(mode=0o700)
        if stat.S_IMODE(self.root.stat().st_mode) & 0o077:
            raise SafetyError("new E2E state root permissions are too broad")
        write_private_json(
            self.root_marker,
            {"schema": 1, "owner": OWNER, "kind": "state-root"},
        )

    def create_run_directory(self, run_id: str) -> Path:
        validate_run_id(run_id)
        self._ensure_owned_root()
        run_dir = self.root / run_id
        if os.path.lexists(run_dir):
            raise SafetyError("refusing a pre-existing E2E run directory")
        run_dir.mkdir(mode=0o700)
        if stat.S_IMODE(run_dir.stat().st_mode) & 0o077:
            raise SafetyError("new E2E run directory permissions are too broad")
        write_private_json(
            run_dir / RUN_DIRECTORY_MARKER,
            {"schema": 1, "owner": OWNER, "kind": "run", "run_id": run_id},
        )
        return run_dir

    def initialize(
        self, run_id: str, profile_name: str = DEFAULT_PROFILE_NAME
    ) -> dict[str, Any]:
        validate_run_id(run_id)
        profile = resolve_profile(profile_name)
        self._ensure_owned_root()
        with self._active_lock():
            if os.path.lexists(self.active_path):
                raise SafetyError(
                    "an E2E harness state is already active; run down or select a distinct APP_E2E_STATE_DIR"
                )
            cluster_name = validate_cluster_name(f"{CLUSTER_PREFIX}{run_id}")
            run_dir = self.create_run_directory(run_id)
            ledger: dict[str, Any] = {
                "schema": 1,
                "owner": OWNER,
                "mode": "owned",
                "run_id": run_id,
                "cluster_name": cluster_name,
                "namespace": NAMESPACE,
                "release": RELEASE,
                "profile": profile.name,
                "base_url": BASE_URL,
                "run_dir": str(run_dir),
                "kubeconfig": str(run_dir / "kubeconfig.yaml"),
                "created_by_run": False,
                "server_container_id": "",
                "phase": "initialized",
                "last_completed_layer": "none",
                "images": {},
                "started_at": utc_now(),
                "updated_at": utc_now(),
            }
            self.validate(ledger)
            write_private_json(run_dir / "ledger.json", ledger)
            write_private_json_exclusive(self.active_path, ledger)
            return ledger

    def _assert_active_run(self, ledger: Mapping[str, Any]) -> None:
        if self.active_path.is_symlink() or not self.active_path.is_file():
            raise SafetyError("active E2E ownership claim is absent or invalid")
        active = read_private_json(self.active_path)
        if (
            not isinstance(active, dict)
            or active.get("schema") != 1
            or active.get("owner") != OWNER
            or active.get("run_id") != ledger.get("run_id")
            or active.get("cluster_name") != ledger.get("cluster_name")
        ):
            raise SafetyError("active E2E ownership belongs to a different run")

    def persist(self, ledger: dict[str, Any]) -> None:
        self.validate(ledger)
        with self._active_lock():
            self._persist_locked(ledger)

    def _persist_locked(self, ledger: dict[str, Any]) -> None:
        self._assert_active_run(ledger)
        ledger["updated_at"] = utc_now()
        run_dir = Path(ledger["run_dir"])
        # active.json is the teardown authority. Publish it first so a crash
        # cannot leave the historical copy proving ownership while the active
        # lifecycle copy still says the cluster was never created.
        write_private_json(self.active_path, ledger)
        write_private_json(run_dir / "ledger.json", ledger)

    def load(self, explicit: Path | None = None) -> dict[str, Any]:
        self._require_owned_root()
        with self._active_lock():
            path = explicit.resolve() if explicit else self.active_path
            if not path.exists():
                raise SafetyError("no active owned E2E state was found")
            value = read_private_json(path)
            if not isinstance(value, dict):
                raise SafetyError("ownership ledger must be a JSON object")
            self.validate(value)
            return value

    def validate(self, ledger: Mapping[str, Any]) -> None:
        self._require_owned_root()
        if ledger.get("schema") != 1 or ledger.get("owner") != OWNER:
            raise SafetyError("ownership ledger marker does not match this harness")
        if ledger.get("mode") != "owned":
            raise SafetyError("destructive lifecycle operations require an owned run")
        run_id = validate_run_id(str(ledger.get("run_id", "")))
        expected_cluster = validate_cluster_name(f"{CLUSTER_PREFIX}{run_id}")
        if ledger.get("cluster_name") != expected_cluster:
            raise SafetyError("cluster name does not match the ledger run id")
        run_dir = Path(str(ledger.get("run_dir", ""))).resolve()
        if run_dir.parent != self.root or run_dir.name != run_id:
            raise SafetyError(
                "ledger run directory is outside the configured state root"
            )
        run_marker_path = run_dir / RUN_DIRECTORY_MARKER
        if run_marker_path.is_symlink() or not run_marker_path.is_file():
            raise SafetyError("ledger run directory is not harness-owned")
        run_marker = read_private_json(run_marker_path)
        if run_marker != {
            "schema": 1,
            "owner": OWNER,
            "kind": "run",
            "run_id": run_id,
        }:
            raise SafetyError("ledger run directory ownership marker is invalid")
        kubeconfig = Path(str(ledger.get("kubeconfig", ""))).resolve()
        if kubeconfig != run_dir / "kubeconfig.yaml":
            raise SafetyError("ledger kubeconfig path is not run-owned")
        profile_from_ledger(ledger)

    def clear_active(self, ledger: dict[str, Any]) -> None:
        self.validate(ledger)
        with self._active_lock():
            self._assert_active_run(ledger)
            ledger["phase"] = "down"
            ledger["finished_at"] = utc_now()
            self._persist_locked(ledger)
            self._assert_active_run(ledger)
            self.active_path.unlink()


def cluster_create_command(rendered_config: Path) -> list[str]:
    return ["k3d", "cluster", "create", "--config", str(rendered_config)]


def canonical_containerd_tag(image: str) -> str:
    """Return the tag spelling reported by containerd for a Docker image ref."""

    first_component = image.split("/", maxsplit=1)[0]
    if "/" not in image:
        return f"docker.io/library/{image}"
    if (
        not any(marker in first_component for marker in (".", ":"))
        and first_component != "localhost"
    ):
        return f"docker.io/{image}"
    return image


def validate_container_platform(platform: str) -> str:
    """Validate a Docker/Kubernetes Linux platform without shell interpretation."""

    if not re.fullmatch(
        r"linux/[a-z0-9][a-z0-9_.-]*(?:/[a-z0-9][a-z0-9_.-]*)?", platform
    ):
        raise SafetyError("Docker server returned an unsupported container platform")
    return platform


def docker_image_identity_command(image: str) -> list[str]:
    """Inspect one selected local image without requiring Engine API 1.49."""

    return [
        "docker",
        "image",
        "inspect",
        "--format",
        "{{.Os}}/{{.Architecture}}|{{.Id}}",
        image,
    ]


def validate_docker_image_identity(value: str, platform: str) -> str:
    """Return an immutable image id only for the selected host platform."""

    expected_platform = validate_container_platform(platform)
    parts = value.strip().split("|")
    if (
        len(parts) != 2
        or parts[0] != expected_platform
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", parts[1])
    ):
        raise SafetyError("dependency image has no exact local platform identity")
    return parts[1]


def image_import_groups(images: Mapping[str, str]) -> list[tuple[str, tuple[str, ...]]]:
    """Split imports so dependency failures identify one exact upstream image."""

    components = ["orchestrator", "agent", "cockpit", "provider"]
    if "workspace" in images:
        components.append("workspace")
    if any(not images.get(component) for component in components):
        raise SafetyError("image import requires every deployable E2E image")
    return [
        *(
            (f"dependency-{index}", (dependency,))
            for index, dependency in enumerate(DEPENDENCY_IMAGES, start=1)
        ),
        ("application", tuple(images[component] for component in components)),
    ]


def docker_image_save_command(
    image_refs: Sequence[str], archive: Path, platform: str
) -> list[str]:
    if not image_refs or any(not image for image in image_refs):
        raise SafetyError("image archive requires at least one image")
    return [
        "docker",
        "image",
        "save",
        "--platform",
        validate_container_platform(platform),
        "--output",
        str(archive),
        *image_refs,
    ]


def docker_archive_config_ids(archive: Path) -> dict[str, str]:
    """Map archive tags to the config digests reported by CRI image inventory."""

    if archive.is_symlink() or not archive.is_file():
        raise SafetyError("Docker image archive is not a regular file")
    try:
        with tarfile.open(archive, mode="r:*") as stream:
            member = stream.getmember("manifest.json")
            if not member.isfile() or member.size > 10 * 1024 * 1024:
                raise SafetyError("Docker image archive manifest is invalid")
            manifest_stream = stream.extractfile(member)
            if manifest_stream is None:
                raise SafetyError("Docker image archive manifest is unreadable")
            document = json.loads(manifest_stream.read().decode("utf-8"))
    except (
        KeyError,
        OSError,
        tarfile.TarError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ) as exc:
        raise SafetyError("Docker image archive manifest is unreadable") from exc
    if not isinstance(document, list) or not document:
        raise SafetyError("Docker image archive manifest has an unexpected schema")

    config_ids: dict[str, str] = {}
    for item in document:
        if not isinstance(item, dict):
            raise SafetyError("Docker image archive manifest has an unexpected schema")
        config = item.get("Config")
        tags = item.get("RepoTags")
        if not isinstance(config, str) or not isinstance(tags, list) or not tags:
            raise SafetyError("Docker image archive manifest has an unexpected schema")
        config_match = re.fullmatch(
            r"(?:blobs/sha256/)?([0-9a-f]{64})(?:\.json)?", config
        )
        if config_match is None:
            raise SafetyError("Docker image archive config digest is invalid")
        config_id = f"sha256:{config_match.group(1)}"
        for tag in tags:
            if not isinstance(tag, str) or not tag:
                raise SafetyError("Docker image archive tag is invalid")
            canonical_tag = canonical_containerd_tag(tag)
            previous = config_ids.setdefault(canonical_tag, config_id)
            if previous != config_id:
                raise SafetyError("Docker image archive has conflicting tag contents")
    return config_ids


def k3d_image_import_command(archive: Path, cluster_name: str) -> list[str]:
    cluster_name = validate_cluster_name(cluster_name)
    # k3d v5.8.3's tools-node path can swallow per-node ctr failures and exit
    # zero. Direct mode aggregates them, while the platform-pruned archive
    # avoids the incomplete multi-platform input that made direct mode fail.
    return [
        "k3d",
        "image",
        "import",
        str(archive),
        "--cluster",
        cluster_name,
        "--mode",
        "direct",
    ]


def build_image_commands(
    sha: str,
    run_id: str,
    *,
    dirty: bool = False,
    platform: str | None = None,
    include_workspace: bool = False,
) -> tuple[dict[str, str], list[list[str]]]:
    if not re.fullmatch(r"[0-9a-f]{40,64}", sha):
        raise SafetyError("git revision is not a full hexadecimal commit id")
    evidence = "dirty-" if dirty else ""
    suffix = f"{sha[:12]}-{evidence}{run_id[-8:]}"
    images = {
        "orchestrator": f"srw-e2e-orchestrator:{suffix}",
        "agent": f"srw-e2e-agent:{suffix}",
        "cockpit": f"srw-e2e-cockpit:{suffix}",
        "provider": f"srw-e2e-model-fixture:{suffix}",
        "playwright": f"srw-e2e-playwright:{suffix}",
    }
    release_version = f"e2e-{'dirty-' if dirty else ''}{run_id}"
    common = [
        "--build-arg",
        f"SRW_SOURCE_REVISION={sha}",
        "--build-arg",
        f"SRW_RELEASE_VERSION={release_version}",
    ]
    ownership = [
        "--label",
        f"srw.io/e2e-owner={run_id}",
        "--label",
        f"srw.io/source-revision={sha}",
    ]
    platform_args = (
        ["--platform", validate_container_platform(platform)] if platform else []
    )
    commands = [
        [
            "docker",
            "build",
            *platform_args,
            "--pull=false",
            *ownership,
            *common,
            "-f",
            "docker/Dockerfile.orchestrator",
            "-t",
            images["orchestrator"],
            ".",
        ],
        [
            "docker",
            "build",
            *platform_args,
            "--pull=false",
            *ownership,
            *common,
            "--build-arg",
            f"BUILD_SHA={sha}",
            "-f",
            "docker/Dockerfile.agent",
            "-t",
            images["agent"],
            ".",
        ],
        [
            "docker",
            "build",
            *platform_args,
            "--pull=false",
            *ownership,
            *common,
            "-f",
            "docker/Dockerfile.cockpit",
            "-t",
            images["cockpit"],
            ".",
        ],
        [
            "docker",
            "build",
            *platform_args,
            "--pull=false",
            *ownership,
            "-f",
            str(PROVIDER_DOCKERFILE.relative_to(REPO_ROOT)),
            "-t",
            images["provider"],
            str(PROVIDER_DOCKERFILE.parent.relative_to(REPO_ROOT)),
        ],
        [
            "docker",
            "build",
            *platform_args,
            "--pull=false",
            *ownership,
            "-f",
            str(PLAYWRIGHT_RUNNER_DOCKERFILE.relative_to(REPO_ROOT)),
            "-t",
            images["playwright"],
            ".",
        ],
    ]
    if include_workspace:
        images["workspace"] = f"srw-e2e-workspace:{suffix}"
        commands.append(
            [
                "docker",
                "build",
                *platform_args,
                "--pull=false",
                *ownership,
                *common,
                "-f",
                "docker/Dockerfile.workspace",
                "-t",
                images["workspace"],
                ".",
            ]
        )
    return images, commands


def helm_install_command(
    kubeconfig: Path,
    image_values: Path,
    values_files: Sequence[Path] = (VALUES_FILE,),
) -> list[str]:
    values_args = [argument for path in values_files for argument in ("-f", str(path))]
    return [
        "helm",
        "upgrade",
        "--install",
        RELEASE,
        str(REPO_ROOT / "helm"),
        "--kubeconfig",
        str(kubeconfig),
        "--namespace",
        NAMESPACE,
        "--create-namespace",
        *values_args,
        "-f",
        str(image_values),
        "--wait",
        "--wait-for-jobs",
        "--timeout",
        "15m",
    ]


def _image_values(
    images: Mapping[str, str], sha: str, run_id: str, *, dirty: bool = False
) -> str:
    def split(ref: str) -> tuple[str, str]:
        repository, tag = ref.rsplit(":", 1)
        return repository, tag

    configured_components = ["orchestrator", "agent", "cockpit"]
    if "workspace" in images:
        configured_components.append("workspace")
    lines = ["image:"]
    for component in configured_components:
        repository, tag = split(images[component])
        lines.extend(
            [
                f"  {component}:",
                f"    repository: {repository}",
                f"    tag: {tag}",
                '    digest: ""',
                "    pullPolicy: IfNotPresent",
            ]
        )
    lines.extend(
        [
            "provenance:",
            f"  releaseVersion: e2e-{'dirty-' if dirty else ''}{run_id}",
            "  components:",
        ]
    )
    for component in configured_components:
        lines.extend(
            [
                f"    {component}:",
                f"      sourceRevision: {sha}",
                f"      releaseVersion: e2e-{'dirty-' if dirty else ''}{run_id}",
            ]
        )
    return "\n".join(lines) + "\n"


def render_provider_manifest(provider_image: str) -> str:
    source = PROVIDER_MANIFEST.read_text(encoding="utf-8")
    occurrences = source.count(PROVIDER_IMAGE_PLACEHOLDER)
    if occurrences != 1:
        raise SafetyError(
            "provider manifest image placeholder must occur exactly once before rendering"
        )
    rendered = source.replace(PROVIDER_IMAGE_PLACEHOLDER, provider_image)
    # The control listener must remain unserviced. Check the Service document,
    # not merely the deployment's containerPort declaration.
    service_part = rendered.split("kind: Service", maxsplit=1)
    if len(service_part) != 2 or "port: 8001" in service_part[1]:
        raise SafetyError("provider control port must not be exposed by a Service")
    return rendered


def playwright_command(
    *,
    env_file: Path,
    state_dir: Path,
    network: str,
    ingress_ip: str,
    host_gateway: str,
    base_url: str = BASE_URL,
    attach: bool = False,
    runner_image: str,
    run_id: str,
) -> list[str]:
    version = PLAYWRIGHT_VERSION_FILE.read_text(encoding="utf-8").strip()
    if not re.fullmatch(r"\d+\.\d+\.\d+", version):
        raise SafetyError("invalid .playwright-version pin")
    validate_origin(
        base_url, allow_remote=attach, owned_host=BASE_HOST if not attach else None
    )
    validate_run_id(run_id)
    run_dir = state_dir.resolve()
    container_run_dir = f"/work/cockpit/test-results/app/{run_id}"
    uid_gid = f"{os.getuid()}:{os.getgid()}"
    command = [
        "docker",
        "run",
        "--rm",
        "--init",
        "--name",
        f"srw-e2e-browser-{run_id}",
        "--label",
        f"srw.io/e2e-owner={run_id}",
        "--ipc=host",
        "--user",
        uid_gid,
        "--workdir",
        "/work/cockpit",
        "--network",
        network,
        "--env-file",
        str(env_file),
        "-e",
        "CI=1",
        "-e",
        "NPM_CONFIG_CACHE=/tmp/srw-e2e-npm-cache",
        "-e",
        "XDG_CACHE_HOME=/tmp/srw-e2e-xdg-cache",
        "-v",
        f"{REPO_ROOT}:/work:ro",
        "-v",
        f"{run_dir / 'browser'}:{container_run_dir}:rw",
        "-v",
        f"{run_dir / 'node_modules'}:/work/cockpit/node_modules:rw",
    ]
    if not attach:
        for hostname in (BASE_HOST, f"auth.{BASE_HOST}", f"api.{BASE_HOST}"):
            command.extend(["--add-host", f"{hostname}:{ingress_ip}"])
        command.extend(["--add-host", f"host.docker.internal:{host_gateway}"])
    command.extend(
        [
            runner_image,
            "bash",
            "-c",
            "node -e \"if (process.versions.node.split('.')[0] !== '22') process.exit(42)\" && npm ci --no-audit --no-fund && npm run test:e2e:app",
        ]
    )
    return command


class PortForward(AbstractContextManager["PortForward"]):
    def __init__(
        self,
        *,
        kubeconfig: Path,
        namespace: str,
        resource: str,
        remote_port: int,
        address: str = "127.0.0.1",
        timeout: float = 30,
    ):
        self.kubeconfig = kubeconfig
        self.namespace = namespace
        self.resource = resource
        self.remote_port = remote_port
        self.address = address
        self.timeout = timeout
        self.process: subprocess.Popen[str] | None = None
        self.local_port: int | None = None

    def __enter__(self) -> PortForward:
        argv = [
            "kubectl",
            "--kubeconfig",
            str(self.kubeconfig),
            "-n",
            self.namespace,
            "port-forward",
            "--address",
            self.address,
            self.resource,
            f":{self.remote_port}",
        ]
        self.process = subprocess.Popen(
            argv,
            cwd=REPO_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert self.process.stdout is not None
        selector = selectors.DefaultSelector()
        selector.register(self.process.stdout, selectors.EVENT_READ)
        deadline = time.monotonic() + self.timeout
        buffered: list[str] = []
        try:
            while time.monotonic() < deadline:
                if self.process.poll() is not None:
                    break
                events = selector.select(timeout=min(0.5, deadline - time.monotonic()))
                for key, _mask in events:
                    line = key.fileobj.readline()
                    if not line:
                        continue
                    buffered.append(line.rstrip())
                    match = re.search(r"Forwarding from .*:(\d+) ->", line)
                    if match:
                        self.local_port = int(match.group(1))
                        return self
        finally:
            selector.close()
        self.close()
        summary = sanitize_diagnostic("\n".join(buffered[-3:]))
        raise HarnessError(
            f"port-forward did not become ready: {summary or 'no output'}"
        )

    def close(self) -> None:
        if self.process is None:
            return
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.process.stdout:
            self.process.stdout.close()
        self.process = None

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()


def _http_request(
    url: str,
    *,
    method: str = "GET",
    json_body: Any | None = None,
    form_body: Mapping[str, str] | None = None,
    headers: Mapping[str, str] | None = None,
    expected: Iterable[int] = (200,),
    timeout: float = 20,
) -> tuple[int, bytes]:
    if json_body is not None and form_body is not None:
        raise ValueError("request cannot contain JSON and form bodies")
    request_headers = dict(headers or {})
    data: bytes | None = None
    if json_body is not None:
        data = json.dumps(json_body, separators=(",", ":")).encode()
        request_headers["Content-Type"] = "application/json"
    elif form_body is not None:
        data = urllib.parse.urlencode(form_body).encode()
        request_headers["Content-Type"] = "application/x-www-form-urlencoded"
    request = urllib.request.Request(
        url, data=data, headers=request_headers, method=method
    )
    allowed = set(expected)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read()
            status = response.status
    except urllib.error.HTTPError as exc:
        status = exc.code
        body = exc.read()
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise HarnessError(
            f"HTTP {method} request failed ({type(exc).__name__})"
        ) from exc
    if status not in allowed:
        raise HarnessError(f"HTTP {method} request returned unexpected status {status}")
    return status, body


def _json_body(body: bytes, *, label: str) -> Any:
    try:
        return json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HarnessError(f"{label} returned invalid JSON") from exc


class ApplicationE2EHarness:
    def __init__(self, state_root: Path, runner: CommandRunner | None = None):
        self.store = StateStore(state_root)
        self.runner = runner or CommandRunner()

    @staticmethod
    def _run_dir(ledger: Mapping[str, Any]) -> Path:
        return Path(str(ledger["run_dir"]))

    @staticmethod
    def _kubeconfig(ledger: Mapping[str, Any]) -> Path:
        return Path(str(ledger["kubeconfig"]))

    def _kubectl(self, ledger: Mapping[str, Any], *args: str) -> list[str]:
        return [
            "kubectl",
            "--kubeconfig",
            str(self._kubeconfig(ledger)),
            *args,
        ]

    def _mark_layer(self, ledger: dict[str, Any], layer: str) -> None:
        if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", layer):
            raise SafetyError("readiness layer name is invalid")
        try:
            started_at = dt.datetime.fromisoformat(str(ledger["started_at"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise SafetyError("readiness ledger has an invalid start time") from exc
        if started_at.tzinfo is None:
            raise SafetyError("readiness ledger start time must be timezone-aware")
        completed_at = dt.datetime.now(dt.UTC)
        timings = ledger.get("layer_timings", {})
        if not isinstance(timings, dict):
            raise SafetyError("readiness layer timing ledger is invalid")
        timings[layer] = {
            "completed_at": completed_at.isoformat(timespec="seconds"),
            "elapsed_ms": max(
                0, int((completed_at - started_at).total_seconds() * 1000)
            ),
        }
        ledger["layer_timings"] = timings
        ledger["last_completed_layer"] = layer
        ledger["phase"] = layer
        self.store.persist(ledger)
        evidence = {
            "run_id": ledger["run_id"],
            "profile": profile_from_ledger(ledger).name,
            "last_completed_layer": layer,
            "updated_at": ledger["updated_at"],
            "layer_timings": timings,
        }
        write_private_json(self._run_dir(ledger) / "readiness.json", evidence)
        print(f"[e2e-app] ready: {layer}", flush=True)

    def _load_secrets(self, ledger: Mapping[str, Any]) -> SecretBundle:
        path = self._run_dir(ledger) / "credentials.json"
        value = read_private_json(path)
        if not isinstance(value, dict):
            raise SafetyError("credential file must be a JSON object")
        return SecretBundle.from_json(value)

    def check_prerequisites(self) -> None:
        missing = [
            name
            for name in ("docker", "k3d", "kubectl", "helm", "git", "ssh-keygen")
            if not shutil.which(name)
        ]
        if missing:
            raise HarnessError(f"missing required executable(s): {', '.join(missing)}")
        version = self.runner.run(["k3d", "version"], label="k3d version check").stdout
        match = re.search(r"k3d version (v\d+\.\d+\.\d+)", version)
        if not match or match.group(1) != K3D_VERSION:
            actual = match.group(1) if match else "unknown"
            raise HarnessError(f"k3d {K3D_VERSION} is required (found {actual})")
        if K3S_IMAGE not in K3D_TEMPLATE.read_text(encoding="utf-8"):
            raise SafetyError(
                "committed k3d config does not contain the pinned k3s image"
            )

    def check_attach_prerequisites(self) -> None:
        if not shutil.which("docker"):
            raise HarnessError("missing required executable: docker")
        self.runner.run(
            ["docker", "version", "--format", "{{.Server.Version}}"],
            timeout=30,
            label="Docker daemon readiness",
        )

    def inspect_source(self) -> tuple[str, bool]:
        sha = self.runner.run(
            ["git", "rev-parse", "HEAD"], label="git revision lookup"
        ).stdout.strip()
        if not re.fullmatch(r"[0-9a-f]{40,64}", sha):
            raise SafetyError("git revision is not a full hexadecimal commit id")
        status = self.runner.run(
            ["git", "status", "--porcelain=v1", "--untracked-files=all"],
            label="git working-tree check",
        ).stdout
        dirty = bool(status.strip())
        if dirty and os.environ.get("APP_E2E_ALLOW_DIRTY") != "1":
            raise SafetyError(
                "authoritative current-SHA images require a clean working tree; "
                "set APP_E2E_ALLOW_DIRTY=1 only for non-authoritative local development"
            )
        return sha, dirty

    def ensure_playwright_runner_image(self) -> str:
        version = PLAYWRIGHT_VERSION_FILE.read_text(encoding="utf-8").strip()
        dockerfile = PLAYWRIGHT_RUNNER_DOCKERFILE.read_text(encoding="utf-8")
        if f"mcr.microsoft.com/playwright:v{version}-noble" not in dockerfile:
            raise SafetyError(
                "Playwright runner Dockerfile is out of sync with the version pin"
            )
        if "node:22.22.0-bookworm-slim" not in dockerfile:
            raise SafetyError("Playwright runner must use the exact Node 22 image")
        image = f"srw-e2e-playwright:v{version}-node22.22.0"
        self.runner.run(
            [
                "docker",
                "build",
                "--pull=false",
                "-f",
                str(PLAYWRIGHT_RUNNER_DOCKERFILE.relative_to(REPO_ROOT)),
                "-t",
                image,
                ".",
            ],
            env={"DOCKER_BUILDKIT": "1"},
            timeout=900,
            label="pinned Playwright/Node runner build",
        )
        version_result = self.runner.run(
            ["docker", "run", "--rm", image, "node", "--version"],
            timeout=30,
            label="Playwright runner Node version check",
        ).stdout.strip()
        if not re.fullmatch(r"v22\.\d+\.\d+", version_result):
            raise SafetyError("Playwright runner is not using Node 22")
        return image

    def _record_created_cluster(
        self, ledger: dict[str, Any], *, required: bool
    ) -> bool:
        """Reconcile the one expected server identity after a create attempt."""
        cluster_name = validate_cluster_name(str(ledger["cluster_name"]))
        container_name = f"k3d-{cluster_name}-server-0"
        identity = self.runner.run(
            [
                "docker",
                "inspect",
                "--format",
                '{{.Id}}|{{ index .Config.Labels "k3d.cluster" }}|{{ index .Config.Labels "k3d.role" }}',
                container_name,
            ],
            check=False,
            label="created k3d server reconciliation",
        )
        if identity.returncode:
            if required:
                raise SafetyError("created cluster has no exact server identity")
            return False
        parts = identity.stdout.strip().split("|")
        if (
            len(parts) != 3
            or not re.fullmatch(r"[0-9a-f]{64}", parts[0])
            or parts[1:] != [cluster_name, "server"]
        ):
            raise SafetyError("created cluster server labels are not ownership-safe")

        # This is the critical ownership commit. Persist it before kubeconfig
        # export or any later readiness step can fail.
        ledger["created_by_run"] = True
        ledger["server_container_id"] = parts[0]
        self.store.persist(ledger)

        volume_name = f"k3d-{cluster_name}-images"
        volume = self.runner.run(
            [
                "docker",
                "volume",
                "inspect",
                "--format",
                '{{.Name}}|{{ index .Labels "k3d.cluster" }}|{{ index .Labels "app" }}',
                volume_name,
            ],
            check=False,
            label="created k3d image volume reconciliation",
        )
        if volume.returncode:
            if required:
                raise SafetyError("created cluster has no exact image volume identity")
            return True
        if volume.stdout.strip().split("|") != [volume_name, cluster_name, "k3d"]:
            raise SafetyError(
                "created cluster image volume labels are not ownership-safe"
            )
        ledger["image_volume_name"] = volume_name
        self.store.persist(ledger)
        return True

    def create_cluster(self, ledger: dict[str, Any]) -> None:
        cluster_name = validate_cluster_name(str(ledger["cluster_name"]))
        run_dir = self._run_dir(ledger)
        rendered_config = run_dir / "k3d.yaml"
        template = K3D_TEMPLATE.read_text(encoding="utf-8")
        if template.count("__CLUSTER_NAME__") != 1:
            raise SafetyError(
                "k3d template cluster placeholder must occur exactly once"
            )
        _atomic_private_write(
            rendered_config, template.replace("__CLUSTER_NAME__", cluster_name)
        )
        existing = self.runner.run(
            ["k3d", "cluster", "list", "-o", "json"],
            label="k3d cluster inventory",
        )
        try:
            inventory = json.loads(existing.stdout or "[]")
        except json.JSONDecodeError as exc:
            raise HarnessError("k3d returned an invalid cluster inventory") from exc
        if any(
            item.get("name") == cluster_name
            for item in inventory
            if isinstance(item, dict)
        ):
            raise SafetyError(
                "refusing to adopt an existing cluster, even with an owned prefix"
            )
        try:
            self.runner.run(
                cluster_create_command(rendered_config),
                env={"KUBECONFIG": str(self._kubeconfig(ledger))},
                timeout=240,
                label="owned k3d cluster creation",
            )
        except BaseException:
            # A timeout can happen after Docker created the server. Reconcile
            # only the unique expected container with exact built-in k3d
            # labels; never adopt an arbitrary prefix-matching cluster.
            for attempt in range(5):
                if self._record_created_cluster(ledger, required=False):
                    print(
                        "[e2e-app] reconciled owned cluster after interrupted create",
                        file=sys.stderr,
                    )
                    break
                if attempt < 4:
                    time.sleep(1)
            raise
        self._record_created_cluster(ledger, required=True)
        kubeconfig = self.runner.run(
            ["k3d", "kubeconfig", "get", cluster_name],
            label="isolated kubeconfig export",
        ).stdout
        if f"k3d-{cluster_name}" not in kubeconfig:
            raise SafetyError("exported kubeconfig does not target the owned cluster")
        _atomic_private_write(self._kubeconfig(ledger), kubeconfig)
        self.store.persist(ledger)
        self.runner.run(
            self._kubectl(
                ledger,
                "wait",
                "--for=condition=Ready",
                "nodes",
                "--all",
                "--timeout=180s",
            ),
            timeout=190,
            label="cluster API readiness",
        )
        self._mark_layer(ledger, "cluster-api")

    def _assert_owned_cluster(self, ledger: Mapping[str, Any]) -> None:
        self.store.validate(ledger)
        if ledger.get("created_by_run") is not True:
            raise SafetyError("ledger does not prove that this run created the cluster")
        cluster_name = validate_cluster_name(str(ledger["cluster_name"]))
        expected_id = str(ledger.get("server_container_id", ""))
        if not re.fullmatch(r"[0-9a-f]{64}", expected_id):
            raise SafetyError("ledger does not contain a valid server container id")
        container_name = f"k3d-{cluster_name}-server-0"
        identity = self.runner.run(
            [
                "docker",
                "inspect",
                "--format",
                '{{.Id}}|{{ index .Config.Labels "k3d.cluster" }}|{{ index .Config.Labels "k3d.role" }}',
                container_name,
            ],
            check=False,
            label="owned cluster identity check",
        )
        if identity.returncode:
            raise SafetyError("owned cluster server container is absent")
        parts = identity.stdout.strip().split("|")
        if parts != [expected_id, cluster_name, "server"]:
            raise SafetyError(
                "live k3d server identity no longer matches the ownership ledger"
            )

    def build_and_import_images(self, ledger: dict[str, Any]) -> None:
        sha = str(ledger.get("source_revision", ""))
        dirty = ledger.get("source_dirty") is True
        profile = profile_from_ledger(ledger)
        platform_result = self.runner.run(
            [
                "docker",
                "version",
                "--format",
                "{{.Server.Os}}/{{.Server.Arch}}",
            ],
            label="Docker server platform detection",
        )
        platform = validate_container_platform(platform_result.stdout.strip())
        images, commands = build_image_commands(
            sha,
            str(ledger["run_id"]),
            dirty=dirty,
            platform=platform,
            include_workspace=profile.include_workspace_image,
        )
        ledger["images"] = images
        ledger["image_ids"] = {}
        ledger["image_platform"] = platform
        ledger["runtime_image_ids"] = {}
        self.store.persist(ledger)
        build_env = {"DOCKER_BUILDKIT": "1"}
        dependency_image_ids: dict[str, str] = {}
        for image in DEPENDENCY_IMAGES:
            # ``docker image inspect --platform`` needs Engine API 1.49, but
            # GitHub's current Engine 28.0 fallback exposes 1.48. The ordinary
            # inspect response already carries the selected local image's OS,
            # architecture and immutable id, so prove those explicitly instead.
            inspect_command = docker_image_identity_command(image)
            inspection = self.runner.run(
                inspect_command,
                check=False,
                label=f"cached dependency image inspection ({image})",
            )
            image_id: str | None = None
            if not inspection.returncode:
                try:
                    image_id = validate_docker_image_identity(
                        inspection.stdout, platform
                    )
                except SafetyError:
                    # A tag cached for another architecture is not usable by
                    # this cluster. Pull the selected platform before proving
                    # and recording its immutable local identity.
                    pass
            if image_id is None:
                print(f"[e2e-app] pulling pinned dependency image {image}", flush=True)
                self.runner.run(
                    ["docker", "pull", "--platform", platform, image],
                    timeout=900,
                    label=f"dependency image pull ({image})",
                )
                inspection = self.runner.run(
                    inspect_command,
                    label=f"dependency image inspection ({image})",
                )
                try:
                    image_id = validate_docker_image_identity(
                        inspection.stdout, platform
                    )
                except SafetyError as exc:
                    raise SafetyError(
                        f"dependency image {image} has no exact local platform identity"
                    ) from exc
            else:
                print(f"[e2e-app] using cached dependency image {image}", flush=True)
            dependency_image_ids[image] = image_id
            ledger["dependency_image_ids"] = dependency_image_ids.copy()
            self.store.persist(ledger)

        image_ids: dict[str, str] = {}
        for (component, image_ref), command in zip(
            images.items(), commands, strict=True
        ):
            print(f"[e2e-app] building current-SHA {component} image", flush=True)
            result = self.runner.run(
                command,
                env=build_env,
                check=False,
                timeout=1800,
                label=f"{component} image build",
            )
            build_output = bound_diagnostic(
                sanitize_diagnostic(result.stdout + result.stderr)
            )
            _atomic_private_write(
                self._run_dir(ledger) / f"image-build-{component}.txt",
                build_output,
            )
            if result.returncode:
                raise HarnessError(
                    f"{component} image build failed with exit code {result.returncode}"
                )
            identity = self.runner.run(
                [
                    "docker",
                    "image",
                    "inspect",
                    "--format",
                    '{{.Id}}|{{ index .Config.Labels "srw.io/e2e-owner" }}|{{ index .Config.Labels "srw.io/source-revision" }}',
                    image_ref,
                ],
                label=f"{component} image inspection",
            ).stdout.strip()
            parts = identity.split("|")
            if (
                len(parts) != 3
                or not re.fullmatch(r"sha256:[0-9a-f]{64}", parts[0])
                or parts[1:] != [str(ledger["run_id"]), sha]
            ):
                raise SafetyError(
                    f"{component} build did not produce an owned immutable image"
                )
            image_ids[component] = parts[0]
            ledger["image_ids"] = image_ids.copy()
            self.store.persist(ledger)
        run_dir = self._run_dir(ledger)
        runtime_image_ids: dict[str, str] = {}
        for label, image_refs in image_import_groups(images):
            # Docker's containerd image store can retain a remote multi-platform
            # index while only caching the host-platform blobs. A plain
            # `docker save` then creates an incomplete archive that ctr rejects,
            # even though k3d may exit zero. Export the exact node platform so
            # every descriptor in the archive has local content.
            archive = run_dir / f".image-import-{label}.tar"
            if os.path.lexists(archive):
                raise SafetyError("refusing to overwrite a stale image archive")
            save_result = CommandResult(1)
            import_result = CommandResult(1)
            try:
                save_result = self.runner.run(
                    docker_image_save_command(image_refs, archive, platform),
                    check=False,
                    timeout=1800,
                    label=f"host-platform {label} image archive",
                )
                if save_result.returncode:
                    raise HarnessError(
                        f"host-platform {label} image archive failed with exit code {save_result.returncode}"
                    )
                if archive.is_symlink() or not archive.is_file():
                    raise SafetyError("Docker did not create a regular image archive")
                archive_ids = docker_archive_config_ids(archive)
                expected_tags = {
                    canonical_containerd_tag(image) for image in image_refs
                }
                if set(archive_ids) != expected_tags:
                    raise SafetyError(
                        f"host-platform {label} archive did not contain the exact requested tags"
                    )
                runtime_image_ids.update(archive_ids)
                ledger["runtime_image_ids"] = runtime_image_ids.copy()
                self.store.persist(ledger)
                import_result = self.runner.run(
                    k3d_image_import_command(archive, str(ledger["cluster_name"])),
                    check=False,
                    timeout=1800,
                    label=f"direct {label} image import",
                )
                if import_result.returncode:
                    raise HarnessError(
                        f"direct {label} image import failed with exit code {import_result.returncode}"
                    )
            finally:
                import_output = bound_diagnostic(
                    sanitize_diagnostic(
                        save_result.stdout
                        + save_result.stderr
                        + import_result.stdout
                        + import_result.stderr
                    )
                )
                _atomic_private_write(
                    run_dir / f"image-import-{label}.txt",
                    import_output,
                )
                if archive.is_symlink() or archive.is_file():
                    archive.unlink()
        self._verify_imported_node_images(ledger)
        image_values = self._run_dir(ledger) / "values-images.yaml"
        _atomic_private_write(
            image_values,
            _image_values(images, sha, str(ledger["run_id"]), dirty=dirty),
        )
        self.store.persist(ledger)
        self._mark_layer(ledger, "images-imported")

    def _verify_imported_node_images(self, ledger: dict[str, Any]) -> None:
        """Gate deployment on every runtime node having each required image id."""

        self._assert_owned_cluster(ledger)
        images = ledger.get("images")
        runtime_ids = ledger.get("runtime_image_ids")
        if not isinstance(images, dict) or not isinstance(runtime_ids, dict):
            raise SafetyError("image verification ledger is incomplete")

        expected_tags: set[str] = set()
        components = ["orchestrator", "agent", "cockpit", "provider"]
        if profile_from_ledger(ledger).include_workspace_image:
            components.append("workspace")
        for component in components:
            image = images.get(component)
            if not isinstance(image, str):
                raise SafetyError("deployable image verification ledger is invalid")
            expected_tags.add(canonical_containerd_tag(image))
        for image in DEPENDENCY_IMAGES:
            expected_tags.add(canonical_containerd_tag(image))
        if set(runtime_ids) != expected_tags or not all(
            re.fullmatch(r"sha256:[0-9a-f]{64}", str(image_id))
            for image_id in runtime_ids.values()
        ):
            raise SafetyError("runtime image verification ledger is invalid")
        expected = {str(tag): str(image_id) for tag, image_id in runtime_ids.items()}

        cluster_name = validate_cluster_name(str(ledger["cluster_name"]))
        verified: dict[str, int] = {}
        for role in ("server-0", "agent-0"):
            container = f"k3d-{cluster_name}-{role}"
            deadline = time.monotonic() + 180
            while True:
                inventory = self.runner.run(
                    ["docker", "exec", container, "crictl", "images", "-o", "json"],
                    check=False,
                    timeout=45,
                    label=f"{role} runtime image inventory",
                )
                actual: dict[str, str] = {}
                if inventory.returncode == 0:
                    try:
                        document = json.loads(inventory.stdout)
                    except json.JSONDecodeError:
                        document = None
                    if isinstance(document, dict) and isinstance(
                        document.get("images"), list
                    ):
                        for item in document["images"]:
                            if not isinstance(item, dict) or not isinstance(
                                item.get("id"), str
                            ):
                                continue
                            for tag in item.get("repoTags") or []:
                                if isinstance(tag, str):
                                    actual[tag] = item["id"]
                if all(
                    actual.get(tag) == image_id for tag, image_id in expected.items()
                ):
                    verified[role] = len(expected)
                    break
                if time.monotonic() >= deadline:
                    raise HarnessError(
                        f"{role} runtime image inventory did not match all imported images"
                    )
                time.sleep(2)
        ledger["verified_node_images"] = verified
        self.store.persist(ledger)

    def create_secrets_and_fixture(self, ledger: dict[str, Any]) -> None:
        run_dir = self._run_dir(ledger)
        bundle = SecretBundle.generate(str(ledger["run_id"]))
        write_private_json(run_dir / "credentials.json", dataclasses.asdict(bundle))
        private_key = run_dir / "vm-ssh-key"
        self.runner.run(
            [
                "ssh-keygen",
                "-q",
                "-t",
                "ed25519",
                "-N",
                "",
                "-C",
                "srw-e2e-owned",
                "-f",
                str(private_key),
            ],
            label="ephemeral VM SSH key generation",
        )
        private_key.chmod(0o600)
        (run_dir / "vm-ssh-key.pub").chmod(0o600)
        namespace_manifest = json.dumps(
            {
                "apiVersion": "v1",
                "kind": "Namespace",
                "metadata": {
                    "name": NAMESPACE,
                    "labels": {"srw.io/e2e-owner": str(ledger["run_id"])},
                },
            }
        )
        self.runner.run(
            self._kubectl(ledger, "apply", "-f", "-"),
            input_text=namespace_manifest,
            label="E2E namespace creation",
        )
        manifests = [
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {"name": name, "namespace": NAMESPACE},
                "type": "Opaque",
                "stringData": data,
            }
            for name, data in generated_secret_data(
                bundle,
                vm_private_key=private_key.read_text(encoding="utf-8"),
                vm_public_key=(run_dir / "vm-ssh-key.pub").read_text(encoding="utf-8"),
            ).items()
        ]
        # Secret values travel over stdin, never process argv or harness output.
        for index, manifest in enumerate(manifests, start=1):
            self.runner.run(
                self._kubectl(ledger, "apply", "-f", "-"),
                input_text=json.dumps(manifest),
                label=f"generated Kubernetes Secret {index}",
            )
        provider_manifest = render_provider_manifest(str(ledger["images"]["provider"]))
        self.runner.run(
            self._kubectl(ledger, "-n", NAMESPACE, "apply", "-f", "-"),
            input_text=provider_manifest,
            label="deterministic provider deployment",
        )
        self.runner.run(
            self._kubectl(
                ledger,
                "-n",
                NAMESPACE,
                "rollout",
                "status",
                "deployment/srw-e2e-model-fixture",
                "--timeout=180s",
            ),
            timeout=190,
            label="deterministic provider readiness",
        )
        self._mark_layer(ledger, "fixture-ready")

    def deploy_chart(self, ledger: dict[str, Any]) -> None:
        image_values = self._run_dir(ledger) / "values-images.yaml"
        profile = profile_from_ledger(ledger)
        self.runner.run(
            [
                "helm",
                "repo",
                "add",
                "collabora",
                COLLABORA_HELM_REPOSITORY,
                "--force-update",
            ],
            timeout=120,
            label="Collabora Helm repository registration",
        )
        dependency_result = self.runner.run(
            ["helm", "dependency", "build", str(REPO_ROOT / "helm")],
            check=False,
            timeout=300,
            label="Helm dependency build",
        )
        _atomic_private_write(
            self._run_dir(ledger) / "helm-dependency-build.txt",
            bound_diagnostic(
                sanitize_diagnostic(dependency_result.stdout + dependency_result.stderr)
            ),
        )
        if dependency_result.returncode:
            raise HarnessError(
                "Helm dependency build failed with exit code "
                f"{dependency_result.returncode}"
            )
        lint_command = ["helm", "lint", str(REPO_ROOT / "helm")]
        for values_file in profile.values_files:
            lint_command.extend(("-f", str(values_file)))
        lint_command.extend(("-f", str(image_values)))
        self.runner.run(
            lint_command,
            timeout=120,
            label="E2E Helm lint",
        )
        command = helm_install_command(
            self._kubeconfig(ledger), image_values, profile.values_files
        )
        if "--atomic" in command:
            raise SafetyError(
                "E2E Helm install must preserve failed state for diagnostics"
            )
        self.runner.run(command, timeout=960, label="E2E Helm deployment")
        workloads = [
            f"deployment/{name}"
            for name in (
                "srw-e2e-orchestrator",
                "srw-e2e-cockpit",
                "srw-e2e-keycloak",
                *profile.additional_deployments,
            )
        ]
        workloads.extend(
            f"statefulset/{name}" for name in profile.additional_statefulsets
        )
        for workload in workloads:
            self.runner.run(
                self._kubectl(
                    ledger,
                    "-n",
                    NAMESPACE,
                    "rollout",
                    "status",
                    workload,
                    "--timeout=300s",
                ),
                timeout=310,
                label=f"{workload} rollout readiness",
            )
        self._verify_deployed_images(ledger)
        self._attest_cloud_backend_after_rollout(ledger, profile)
        self._mark_layer(ledger, "helm-workloads")

    def _attest_cloud_backend_after_rollout(
        self, ledger: dict[str, Any], profile: ApplicationE2EProfile
    ) -> None:
        """Restart the orchestrator once the cloud backend is actually up.

        The orchestrator attests its main-cloud installation **once**, during
        `lifespan`, and there is no retry: if the backend was not reachable at
        that moment it logs "installation authority is unavailable" and every
        cloud effect stays disabled for the life of the process. The admin
        reload endpoint cannot recover it either — `reload_active_main_cloud_instance`
        returns `None` when no durable instance exists, which the route reports
        as a 409 telling the caller to retry something that can never succeed.

        On this stack the backend loses that race every time: bundled Nextcloud
        installs itself on first boot and its protected-effect sidecars then
        wait for the front controllers to appear, so it is minutes behind the
        orchestrator. Nothing in the chart makes the orchestrator wait for it —
        deliberately, since the cloud tier is optional and must not gate the
        API.

        The rollouts above have already settled by the time this runs, so one
        restart is enough, and it is the same recovery an operator performs
        when a backend comes up late.
        """
        if not profile.cloud_enabled:
            return
        self.runner.run(
            self._kubectl(
                ledger,
                "-n",
                NAMESPACE,
                "rollout",
                "restart",
                "deployment/srw-e2e-orchestrator",
            ),
            label="orchestrator restart for main-cloud attestation",
        )
        self.runner.run(
            self._kubectl(
                ledger,
                "-n",
                NAMESPACE,
                "rollout",
                "status",
                "deployment/srw-e2e-orchestrator",
                "--timeout=300s",
            ),
            timeout=310,
            label="orchestrator readiness after main-cloud attestation",
        )

    def _verify_deployed_images(self, ledger: Mapping[str, Any]) -> None:
        profile = profile_from_ledger(ledger)
        checks = {
            "deployment/srw-e2e-orchestrator": str(ledger["images"]["orchestrator"]),
            "deployment/srw-e2e-cockpit": str(ledger["images"]["cockpit"]),
            "deployment/srw-e2e-model-fixture": str(ledger["images"]["provider"]),
        }
        if profile.stateless_agents:
            checks["deployment/srw-e2e-agent-stateless"] = str(
                ledger["images"]["agent"]
            )
        for resource, expected in checks.items():
            actual = self.runner.run(
                self._kubectl(
                    ledger,
                    "-n",
                    NAMESPACE,
                    "get",
                    resource,
                    "-o",
                    "jsonpath={.spec.template.spec.containers[0].image}",
                ),
                label=f"{resource} image verification",
            ).stdout
            if actual != expected:
                raise SafetyError(f"{resource} is not using its current-SHA image")
        agent_image = self.runner.run(
            self._kubectl(
                ledger,
                "-n",
                NAMESPACE,
                "get",
                "configmap/srw-e2e-config",
                "-o",
                "jsonpath={.data.AGENT_IMAGE}",
            ),
            label="dynamic agent image verification",
        ).stdout
        if agent_image != str(ledger["images"]["agent"]):
            raise SafetyError("dynamic agent configuration is not current-SHA pinned")
        if profile.include_workspace_image:
            workspace_image = self.runner.run(
                self._kubectl(
                    ledger,
                    "-n",
                    NAMESPACE,
                    "get",
                    "configmap/srw-e2e-config",
                    "-o",
                    "jsonpath={.data.WORKSPACE_IMAGE}",
                ),
                label="dynamic workspace image verification",
            ).stdout
            if workspace_image != str(ledger["images"]["workspace"]):
                raise SafetyError(
                    "dynamic workspace configuration is not current-SHA pinned"
                )

    def create_keycloak_users(self, ledger: dict[str, Any]) -> None:
        bundle = self._load_secrets(ledger)
        with PortForward(
            kubeconfig=self._kubeconfig(ledger),
            namespace=NAMESPACE,
            resource="service/srw-e2e-keycloak",
            remote_port=8080,
        ) as forward:
            assert forward.local_port is not None
            root = f"http://127.0.0.1:{forward.local_port}"
            _status, token_body = _http_request(
                f"{root}/realms/master/protocol/openid-connect/token",
                method="POST",
                form_body={
                    "client_id": "admin-cli",
                    "grant_type": "password",
                    "username": "e2e-bootstrap-admin",
                    "password": bundle.admin_password,
                },
            )
            token_document = _json_body(token_body, label="Keycloak admin token")
            token = (
                token_document.get("access_token")
                if isinstance(token_document, dict)
                else None
            )
            if not isinstance(token, str) or not token:
                raise HarnessError("Keycloak admin token response is incomplete")
            headers = {"Authorization": f"Bearer {token}"}
            for username, password, roles in (
                (bundle.admin_username, bundle.admin_password, ("admin",)),
                # No legacy `user` role: security/auth.py maps it to approved.
                # The journey's first /api/auth/me must JIT-create a pending row.
                (bundle.journey_username, bundle.journey_password, ()),
            ):
                user = {
                    "username": username,
                    "email": f"{username}@example.test",
                    "emailVerified": True,
                    "enabled": True,
                    "firstName": "E2E",
                    "lastName": "Journey",
                    "credentials": [
                        {"type": "password", "value": password, "temporary": False}
                    ],
                }
                _http_request(
                    f"{root}/admin/realms/srw/users",
                    method="POST",
                    json_body=user,
                    headers=headers,
                    expected=(201,),
                )
                query = urllib.parse.urlencode({"username": username, "exact": "true"})
                _status, users_body = _http_request(
                    f"{root}/admin/realms/srw/users?{query}", headers=headers
                )
                users = _json_body(users_body, label="Keycloak exact user lookup")
                if (
                    not isinstance(users, list)
                    or len(users) != 1
                    or users[0].get("username") != username
                ):
                    raise HarnessError(
                        "Keycloak did not return the exact generated identity"
                    )
                user_id = users[0].get("id")
                if not isinstance(user_id, str) or not user_id:
                    raise HarnessError("Keycloak generated identity has no id")
                role_documents: list[dict[str, Any]] = []
                for role in roles:
                    _status, role_body = _http_request(
                        f"{root}/admin/realms/srw/roles/{urllib.parse.quote(role)}",
                        headers=headers,
                    )
                    role_document = _json_body(
                        role_body, label="Keycloak realm role lookup"
                    )
                    if (
                        not isinstance(role_document, dict)
                        or role_document.get("name") != role
                    ):
                        raise HarnessError("Keycloak realm role lookup was not exact")
                    role_documents.append(role_document)
                if role_documents:
                    _http_request(
                        f"{root}/admin/realms/srw/users/{urllib.parse.quote(user_id)}/role-mappings/realm",
                        method="POST",
                        json_body=role_documents,
                        headers=headers,
                        expected=(204,),
                    )
        self._mark_layer(ledger, "keycloak-identities")

    def provider_preflight(self, ledger: dict[str, Any]) -> None:
        bundle = self._load_secrets(ledger)
        with (
            PortForward(
                kubeconfig=self._kubeconfig(ledger),
                namespace=NAMESPACE,
                resource="service/srw-e2e-model-fixture",
                remote_port=8000,
            ) as inference,
            PortForward(
                kubeconfig=self._kubeconfig(ledger),
                namespace=NAMESPACE,
                resource="deployment/srw-e2e-model-fixture",
                remote_port=8001,
            ) as control,
        ):
            assert inference.local_port is not None and control.local_port is not None
            inference_root = f"http://127.0.0.1:{inference.local_port}"
            control_root = f"http://127.0.0.1:{control.local_port}"
            inference_headers = {"Authorization": f"Bearer {bundle.provider_api_key}"}
            control_headers = {
                "Authorization": f"Bearer {bundle.provider_control_token}"
            }
            _http_request(f"{inference_root}/health")
            _http_request(f"{control_root}/control/health", headers=control_headers)
            preflight_id = f"preflight-{str(ledger['run_id'])[-12:]}"
            _http_request(
                f"{control_root}/control/scenarios/{preflight_id}/arm",
                method="POST",
                json_body={"scenario": "reply", "required_responses": 2},
                headers=control_headers,
                expected=(201,),
            )
            try:
                _status, models_body = _http_request(
                    f"{inference_root}/v1/models", headers=inference_headers
                )
                models = _json_body(models_body, label="provider model list")
                ids = (
                    {
                        item.get("id")
                        for item in models.get("data", [])
                        if isinstance(item, dict)
                    }
                    if isinstance(models, dict)
                    else set()
                )
                if ids != {"e2e-chat", "e2e-embedding"}:
                    raise HarnessError("provider model catalogue is not exact")
                base_chat = {
                    "model": "e2e-chat",
                    "messages": [
                        {"role": "user", "content": f"E2E-{preflight_id} contract"}
                    ],
                    "metadata": {"e2e_run_id": preflight_id},
                }
                _status, completion_body = _http_request(
                    f"{inference_root}/v1/chat/completions",
                    method="POST",
                    json_body={**base_chat, "stream": False},
                    headers=inference_headers,
                )
                completion = _json_body(completion_body, label="provider completion")
                content = (
                    completion.get("choices", [{}])[0].get("message", {}).get("content")
                    if isinstance(completion, dict)
                    else None
                )
                if content != f"E2E_REPLY:{preflight_id}":
                    raise HarnessError(
                        "provider non-streaming reply is not deterministic"
                    )
                _status, stream_body = _http_request(
                    f"{inference_root}/v1/chat/completions",
                    method="POST",
                    json_body={
                        **base_chat,
                        "stream": True,
                        "stream_options": {"include_usage": True},
                    },
                    headers=inference_headers,
                )
                stream = stream_body.decode("utf-8", errors="strict")
                if (
                    "data: [DONE]" not in stream
                    or '"finish_reason":"stop"' not in stream
                    or '"usage"' not in stream
                ):
                    raise HarnessError("provider SSE contract is incomplete")
                _status, embedding_body = _http_request(
                    f"{inference_root}/v1/embeddings",
                    method="POST",
                    json_body={
                        "model": "e2e-embedding",
                        "input": f"E2E-{preflight_id}",
                        "metadata": {"e2e_run_id": preflight_id},
                    },
                    headers=inference_headers,
                )
                embedding = _json_body(embedding_body, label="provider embedding")
                vector = (
                    embedding.get("data", [{}])[0].get("embedding")
                    if isinstance(embedding, dict)
                    else None
                )
                if not isinstance(vector, list) or len(vector) != 4096:
                    raise HarnessError("provider embedding dimension is not 4096")
                _status, rerank_body = _http_request(
                    f"{inference_root}/v1/rerank",
                    method="POST",
                    json_body={
                        "model": "e2e-embedding",
                        "query": f"E2E-{preflight_id}",
                        "documents": ["first", "second"],
                        "metadata": {"e2e_run_id": preflight_id},
                    },
                    headers=inference_headers,
                )
                rerank = _json_body(rerank_body, label="provider rerank")
                if not isinstance(rerank, dict) or len(rerank.get("results", [])) != 2:
                    raise HarnessError("provider rerank contract is incomplete")
                _status, state_body = _http_request(
                    f"{control_root}/control/scenarios/{preflight_id}",
                    headers=control_headers,
                )
                state = _json_body(state_body, label="provider preflight state")
                if (
                    not isinstance(state, dict)
                    or state.get("remaining_required_responses") != 0
                    or state.get("unexpected_count") != 0
                    or state.get("pending_calls") != 0
                ):
                    raise HarnessError(
                        "provider preflight accounting did not close cleanly"
                    )
            finally:
                _http_request(
                    f"{control_root}/control/scenarios/{preflight_id}",
                    method="DELETE",
                    headers=control_headers,
                    expected=(200,),
                )
        self._mark_layer(ledger, "provider-contract")

    def up(self, profile_name: str = DEFAULT_PROFILE_NAME) -> dict[str, Any]:
        self.check_prerequisites()
        sha, dirty = self.inspect_source()
        run_id = new_run_id()
        ledger = self.store.initialize(run_id, profile_name)
        ledger["source_revision"] = sha
        ledger["source_dirty"] = dirty
        ledger["authoritative"] = not dirty
        self.store.persist(ledger)
        if dirty:
            print(
                "[e2e-app] dirty-tree override active: results are non-authoritative",
                flush=True,
            )
        self.create_cluster(ledger)
        self.build_and_import_images(ledger)
        self.create_secrets_and_fixture(ledger)
        self.deploy_chart(ledger)
        self.create_keycloak_users(ledger)
        self.provider_preflight(ledger)
        self._mark_layer(ledger, "ready-for-playwright")
        print(
            f"[e2e-app] owned {ledger['profile']} environment ready "
            f"({ledger['cluster_name']})",
            flush=True,
        )
        return ledger

    def _network_facts(self, ledger: Mapping[str, Any]) -> tuple[str, str, str]:
        cluster_name = validate_cluster_name(str(ledger["cluster_name"]))
        network = f"k3d-{cluster_name}"
        network_raw = self.runner.run(
            ["docker", "network", "inspect", network],
            label="owned Docker network inspection",
        ).stdout
        load_balancer_raw = self.runner.run(
            ["docker", "inspect", f"k3d-{cluster_name}-serverlb"],
            label="owned ingress inspection",
        ).stdout
        try:
            network_document = json.loads(network_raw)[0]
            load_balancer_document = json.loads(load_balancer_raw)[0]
            gateways = [
                item.get("Gateway")
                for item in network_document["IPAM"]["Config"]
                if isinstance(item, dict) and item.get("Gateway")
            ]
            ingress_ip = load_balancer_document["NetworkSettings"]["Networks"][network][
                "IPAddress"
            ]
        except (KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
            raise HarnessError(
                "owned k3d Docker network metadata is incomplete"
            ) from exc
        if len(gateways) != 1:
            raise SafetyError("owned k3d network must have exactly one host gateway")
        gateway = str(gateways[0])
        ingress = str(ingress_ip)
        try:
            if not ipaddress.ip_address(gateway).is_private:
                raise SafetyError("k3d bridge gateway is not private")
            if not ipaddress.ip_address(ingress).is_private:
                raise SafetyError("k3d ingress address is not private")
        except ValueError as exc:
            raise SafetyError("k3d network returned an invalid IP address") from exc
        return network, ingress, gateway

    @staticmethod
    def _prepare_browser_directories(run_dir: Path) -> None:
        for name in ("node_modules", "browser"):
            path = run_dir / name
            if os.path.lexists(path):
                if path.is_symlink() or not path.is_dir():
                    raise SafetyError(f"browser {name} path is not a regular directory")
                if stat.S_IMODE(path.stat().st_mode) & 0o077:
                    raise SafetyError(
                        f"browser {name} directory permissions are too broad"
                    )
            else:
                path.mkdir(mode=0o700)
        # Docker cannot create a nested bind-mount target beneath the read-only
        # repository mount. Pre-create this ignored, empty mountpoint; the real
        # files still land in run_dir through the nested bind.
        canonical_root = REPO_ROOT / "cockpit/test-results/app"
        reject_existing_symlink_components(canonical_root, "browser output root")
        canonical_root.mkdir(parents=True, exist_ok=True)
        canonical_mountpoint = canonical_root / run_dir.name
        if os.path.lexists(canonical_mountpoint):
            if canonical_mountpoint.is_symlink() or not canonical_mountpoint.is_dir():
                raise SafetyError("browser mountpoint is not a regular directory")
        else:
            canonical_mountpoint.mkdir(mode=0o700)

        # Same trap, second mountpoint: ``cockpit/node_modules`` is bind-mounted
        # over the read-only repository mount too. A developer checkout already
        # has the directory from ``npm ci``, so this only ever bites a fresh
        # runner -- which is why it read as a Helm failure for days rather than
        # as what it is. Pre-create it for the identical reason; the real
        # modules still land in run_dir through the nested bind.
        canonical_modules = REPO_ROOT / "cockpit/node_modules"
        reject_existing_symlink_components(canonical_modules, "cockpit node_modules")
        if os.path.lexists(canonical_modules):
            if canonical_modules.is_symlink() or not canonical_modules.is_dir():
                raise SafetyError("cockpit node_modules is not a regular directory")
        else:
            canonical_modules.mkdir(parents=True)

    def _finish_browser_container(self, run_id: str, run_dir: Path) -> None:
        validate_run_id(run_id)
        container_name = f"srw-e2e-browser-{run_id}"
        identity = self.runner.run(
            [
                "docker",
                "inspect",
                "--format",
                '{{.Name}}|{{ index .Config.Labels "srw.io/e2e-owner" }}',
                container_name,
            ],
            check=False,
            label="browser container cleanup inspection",
        )
        if identity.returncode == 0:
            if identity.stdout.strip().split("|") != [f"/{container_name}", run_id]:
                raise SafetyError("refusing to remove a mismatched browser container")
            self.runner.run(
                ["docker", "rm", "--force", container_name],
                label="exact browser container cleanup",
            )
            remaining = self.runner.run(
                ["docker", "inspect", container_name], check=False
            )
            if remaining.returncode == 0:
                raise SafetyError("run-owned browser container remained after removal")
        node_modules = run_dir / "node_modules"
        if node_modules.is_symlink():
            raise SafetyError("refusing to remove a symlinked node_modules path")
        if node_modules.is_dir():
            shutil.rmtree(node_modules)
        mountpoint = REPO_ROOT / "cockpit/test-results/app" / run_id
        if mountpoint.is_symlink():
            raise SafetyError("refusing to remove a symlinked browser mountpoint")
        if mountpoint.is_dir():
            if any(mountpoint.iterdir()):
                raise SafetyError("browser mountpoint unexpectedly contains files")
            mountpoint.rmdir()

    def test_owned(self, ledger: dict[str, Any]) -> None:
        self._assert_owned_cluster(ledger)
        profile = profile_from_ledger(ledger)
        bundle = self._load_secrets(ledger)
        network, ingress_ip, gateway = self._network_facts(ledger)
        run_dir = self._run_dir(ledger)
        self._prepare_browser_directories(run_dir)
        with PortForward(
            kubeconfig=self._kubeconfig(ledger),
            namespace=NAMESPACE,
            resource="deployment/srw-e2e-model-fixture",
            remote_port=8001,
            # Bind only the dedicated, run-owned k3d bridge gateway. The
            # token-protected control API is reachable by the Playwright
            # container but never exposed on 0.0.0.0 or through a Service.
            address=gateway,
        ) as control:
            assert control.local_port is not None
            environment = bundle.browser_environment()
            previous_attempts = ledger.get("browser_attempts")
            if previous_attempts is None:
                previous_attempts = (
                    1 if (run_dir / "playwright-output.txt").is_file() else 0
                )
            if (
                not isinstance(previous_attempts, int)
                or isinstance(previous_attempts, bool)
                or previous_attempts < 0
            ):
                raise SafetyError("browser attempt ledger is invalid")
            browser_attempt = previous_attempts + 1
            ledger["browser_attempts"] = browser_attempt
            self.store.persist(ledger)
            container_run_dir = f"/work/cockpit/test-results/app/{ledger['run_id']}"
            environment.update(
                {
                    "APP_E2E_BASE_URL": BASE_URL,
                    "APP_E2E_ALLOW_REMOTE": "1",
                    "APP_E2E_OWNED_CLUSTER": "1",
                    "APP_E2E_BROWSER_ATTEMPT": str(browser_attempt),
                    "APP_E2E_CONTROL_URL": (
                        f"http://host.docker.internal:{control.local_port}"
                    ),
                    "APP_E2E_PROVIDER_CONTROL_URL": (
                        f"http://host.docker.internal:{control.local_port}"
                    ),
                    "APP_E2E_RUN_DIR": container_run_dir,
                    "APP_E2E_AUTH_STATE": f"{container_run_dir}/.auth/journey.json",
                    "APP_E2E_RESOURCE_LEDGER": (
                        f"{container_run_dir}/browser-resources.json"
                    ),
                    "APP_E2E_REPORT_DIR": f"{container_run_dir}/playwright-report",
                    "APP_E2E_DEFER_FAILED_CLEANUP": "1",
                    "APP_E2E_WORKSPACE_BACKEND": profile.workspace_backend,
                    "APP_E2E_EXPECT_EXECUTION_LANE": profile.execution_lane,
                }
            )
            env_file = run_dir / "browser.env"
            write_private_env(env_file, environment)
            command = playwright_command(
                env_file=env_file,
                state_dir=run_dir,
                network=network,
                ingress_ip=ingress_ip,
                host_gateway=gateway,
                runner_image=str(ledger["images"]["playwright"]),
                run_id=str(ledger["run_id"]),
            )
            print(
                f"[e2e-app] running {profile.name} Chromium journey in pinned "
                "Playwright image",
                flush=True,
            )
            try:
                result = self.runner.run(
                    command,
                    check=False,
                    timeout=1200,
                    label="application Playwright journey",
                )
            finally:
                try:
                    self._finish_browser_container(str(ledger["run_id"]), run_dir)
                finally:
                    if env_file.is_symlink():
                        raise SafetyError("refusing to remove a symlinked browser env")
                    if env_file.is_file():
                        env_file.unlink()
            output = sanitize_diagnostic(
                result.stdout + result.stderr, bundle.secret_values()
            )
            _atomic_private_write(run_dir / "playwright-output.txt", output)
            if result.returncode:
                raise HarnessError(
                    f"application Playwright journey failed with exit code {result.returncode}"
                )
        self._mark_layer(ledger, "playwright-complete")

    def test_attached(self) -> None:
        if os.environ.get("APP_E2E_ALLOW_ATTACH") != "1":
            raise SafetyError("attach mode requires APP_E2E_ALLOW_ATTACH=1")
        required = (
            "APP_E2E_BASE_URL",
            "APP_E2E_USERNAME",
            "APP_E2E_PASSWORD",
            "APP_E2E_ADMIN_USERNAME",
            "APP_E2E_ADMIN_PASSWORD",
            "APP_E2E_CONTROL_URL",
            "APP_E2E_CONTROL_TOKEN",
            "APP_E2E_PROVIDER_BASE_URL",
        )
        missing = [name for name in required if not os.environ.get(name)]
        if missing:
            raise HarnessError(f"attach mode is missing: {', '.join(missing)}")
        base_url = validate_origin(
            os.environ["APP_E2E_BASE_URL"],
            allow_remote=os.environ.get("APP_E2E_ALLOW_REMOTE") == "1",
        )
        run_id = f"attach-{new_run_id()}"
        run_dir = self.store.create_run_directory(run_id)
        self._prepare_browser_directories(run_dir)
        optional = (
            "APP_E2E_ALLOW_REMOTE",
            "APP_E2E_CHAT_MODEL",
            "APP_E2E_EMBEDDING_MODEL",
            "APP_E2E_WORKSPACE_BACKEND",
            "APP_E2E_EXPECT_EXECUTION_LANE",
        )
        environment = {
            name: os.environ[name]
            for name in (*required, *optional)
            if os.environ.get(name)
        }
        environment.update(
            {
                "APP_E2E_BASE_URL": base_url,
                "APP_E2E_ATTACH_MODE": "1",
                "APP_E2E_ALLOW_ATTACH": "1",
                "APP_E2E_RUN_DIR": f"/work/cockpit/test-results/app/{run_id}",
                "APP_E2E_AUTH_STATE": (
                    f"/work/cockpit/test-results/app/{run_id}/.auth/journey.json"
                ),
                "APP_E2E_RESOURCE_LEDGER": (
                    f"/work/cockpit/test-results/app/{run_id}/browser-resources.json"
                ),
                "APP_E2E_REPORT_DIR": (
                    f"/work/cockpit/test-results/app/{run_id}/playwright-report"
                ),
                # Attach has no owning runner that can clean after diagnostics.
                "APP_E2E_DEFER_FAILED_CLEANUP": "0",
            }
        )
        env_file = run_dir / "browser.env"
        write_private_env(env_file, environment)
        network = attach_docker_network(base_url, environment["APP_E2E_CONTROL_URL"])
        runner_image = self.ensure_playwright_runner_image()
        command = playwright_command(
            env_file=env_file,
            state_dir=run_dir,
            network=network,
            ingress_ip="unused",
            host_gateway="unused",
            base_url=base_url,
            attach=True,
            runner_image=runner_image,
            run_id=run_id,
        )
        print("[e2e-app] attach mode: verification-only browser iteration", flush=True)
        try:
            result = self.runner.run(
                command,
                check=False,
                timeout=1200,
                label="attached Playwright journey",
            )
        finally:
            try:
                self._finish_browser_container(run_id, run_dir)
            finally:
                for private_path in (
                    env_file,
                    run_dir / "browser/.auth/journey.json",
                    run_dir / "browser/.auth/journey.json.candidate",
                ):
                    if private_path.is_symlink():
                        raise SafetyError(
                            "refusing to remove a symlinked attach credential"
                        )
                    if private_path.is_file():
                        private_path.unlink()
        secrets_to_redact = [environment[name] for name in required[1:]]
        _atomic_private_write(
            run_dir / "playwright-output.txt",
            sanitize_diagnostic(result.stdout + result.stderr, secrets_to_redact),
        )
        if result.returncode:
            raise HarnessError(
                f"attached Playwright journey failed with exit code {result.returncode}"
            )
        print("[e2e-app] attach pass is non-authoritative", flush=True)

    def _capture_diagnostic(
        self,
        ledger: Mapping[str, Any],
        filename: str,
        command: Sequence[str],
        known_secrets: Iterable[str],
        *,
        logs: bool = False,
    ) -> None:
        try:
            result = self.runner.run(command, check=False, timeout=90)
            content = result.stdout + result.stderr
            if result.returncode:
                content += f"\n[diagnostic command exit={result.returncode}]\n"
        except Exception as exc:
            content = f"[diagnostic command failed: {type(exc).__name__}]\n"
        destination = self._run_dir(ledger) / "diagnostics" / filename
        sanitized = (
            sanitize_log_diagnostic(content, known_secrets)
            if logs
            else sanitize_diagnostic(content, known_secrets)
        )
        _atomic_private_write(destination, bound_diagnostic(sanitized))

    def diagnostics(self, ledger: Mapping[str, Any]) -> Path:
        self.store.validate(ledger)
        run_dir = self._run_dir(ledger)
        diagnostics_dir = run_dir / "diagnostics"
        if os.path.lexists(diagnostics_dir):
            if diagnostics_dir.is_symlink() or not diagnostics_dir.is_dir():
                raise SafetyError("diagnostics path is not a regular directory")
            if stat.S_IMODE(diagnostics_dir.stat().st_mode) & 0o077:
                raise SafetyError("diagnostics directory permissions are too broad")
        else:
            diagnostics_dir.mkdir(mode=0o700)
        try:
            bundle = self._load_secrets(ledger)
            known_secrets = bundle.secret_values()
        except HarnessError:
            bundle = None
            known_secrets = ()
        commands = {
            "resources.txt": self._kubectl(
                ledger,
                "-n",
                NAMESPACE,
                "get",
                "pods,deployments,statefulsets,jobs,pvc,ingress,networkpolicy",
                "-o",
                "wide",
            ),
            "events.txt": self._kubectl(
                ledger,
                "-n",
                NAMESPACE,
                "get",
                "events",
                "--sort-by=.lastTimestamp",
            ),
            "pod-status-images.txt": self._kubectl(
                ledger,
                "-n",
                NAMESPACE,
                "get",
                "pods",
                "-o",
                "custom-columns=NAME:.metadata.name,PHASE:.status.phase,READY:.status.containerStatuses[*].ready,RESTARTS:.status.containerStatuses[*].restartCount,IMAGES:.spec.containers[*].image,IMAGE_IDS:.status.containerStatuses[*].imageID",
            ),
        }
        for filename, command in commands.items():
            self._capture_diagnostic(ledger, filename, command, known_secrets)

        setup_logs = [
            *(
                f"image-build-{component}.txt"
                for component in (
                    "orchestrator",
                    "agent",
                    "cockpit",
                    "provider",
                    "playwright",
                    "workspace",
                )
            ),
            *(
                f"image-import-dependency-{index}.txt"
                for index in range(1, len(DEPENDENCY_IMAGES) + 1)
            ),
            "image-import-application.txt",
            "helm-dependency-build.txt",
        ]
        for filename in setup_logs:
            source = run_dir / filename
            if source.is_symlink():
                raise SafetyError("image import diagnostic is a symlink")
            if source.is_file():
                _atomic_private_write(
                    diagnostics_dir / filename,
                    bound_diagnostic(
                        sanitize_diagnostic(
                            source.read_text(encoding="utf-8"), known_secrets
                        )
                    ),
                )

        try:
            pod_result = self.runner.run(
                self._kubectl(
                    ledger,
                    "-n",
                    NAMESPACE,
                    "get",
                    "pods",
                    "-o",
                    "jsonpath={range .items[*]}{.metadata.name}{'\\n'}{end}",
                ),
                check=False,
                timeout=90,
            )
        except Exception:
            pod_result = CommandResult(1)
        safe_pod = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
        for pod in pod_result.stdout.splitlines():
            if not safe_pod.fullmatch(pod):
                continue
            self._capture_diagnostic(
                ledger,
                f"describe-{pod}.txt",
                self._kubectl(
                    ledger,
                    "-n",
                    NAMESPACE,
                    "describe",
                    "pod",
                    pod,
                ),
                known_secrets,
            )
            self._capture_diagnostic(
                ledger,
                f"logs-{pod}.txt",
                self._kubectl(
                    ledger,
                    "-n",
                    NAMESPACE,
                    "logs",
                    pod,
                    "--all-containers=true",
                    "--tail=300",
                ),
                known_secrets,
                logs=True,
            )
            self._capture_diagnostic(
                ledger,
                f"logs-previous-{pod}.txt",
                self._kubectl(
                    ledger,
                    "-n",
                    NAMESPACE,
                    "logs",
                    pod,
                    "--all-containers=true",
                    "--previous",
                    "--tail=200",
                ),
                known_secrets,
                logs=True,
            )

        if bundle is not None:
            try:
                with PortForward(
                    kubeconfig=self._kubeconfig(ledger),
                    namespace=NAMESPACE,
                    resource="deployment/srw-e2e-model-fixture",
                    remote_port=8001,
                ) as control:
                    assert control.local_port is not None
                    _status, body = _http_request(
                        f"http://127.0.0.1:{control.local_port}/control/scenarios",
                        headers={
                            "Authorization": f"Bearer {bundle.provider_control_token}"
                        },
                    )
                    provider_state = _json_body(body, label="provider diagnostics")
                    write_private_json(
                        diagnostics_dir / "provider-counters.json", provider_state
                    )
            except HarnessError as exc:
                _atomic_private_write(
                    diagnostics_dir / "provider-counters-error.txt",
                    sanitize_diagnostic(str(exc), known_secrets) + "\n",
                )
        summary = {
            "owner": OWNER,
            "run_id": ledger["run_id"],
            "cluster_name": ledger["cluster_name"],
            "source_revision": ledger.get("source_revision", ""),
            "last_completed_layer": ledger.get("last_completed_layer", "none"),
            "layer_timings": ledger.get("layer_timings", {}),
            "captured_at": utc_now(),
        }
        write_private_json(diagnostics_dir / "summary.json", summary)
        print(f"[e2e-app] sanitized diagnostics: {diagnostics_dir}", flush=True)
        return diagnostics_dir

    @staticmethod
    def _cookie_header(storage_state: Mapping[str, Any], hostname: str) -> str:
        cookies = storage_state.get("cookies")
        if not isinstance(cookies, list):
            raise SafetyError("Playwright auth state has no cookie list")
        selected: list[tuple[str, str]] = []
        for cookie in cookies:
            if not isinstance(cookie, dict):
                continue
            name = cookie.get("name")
            value = cookie.get("value")
            domain = str(cookie.get("domain", "")).lstrip(".").lower()
            if (
                isinstance(name, str)
                and isinstance(value, str)
                and (hostname == domain or hostname.endswith(f".{domain}"))
            ):
                if not re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+", name):
                    raise SafetyError(
                        "Playwright auth state contains an invalid cookie name"
                    )
                if any(character in value for character in "\r\n;"):
                    raise SafetyError(
                        "Playwright auth state contains an invalid cookie value"
                    )
                selected.append((name, value))
        if not selected:
            raise SafetyError(
                "Playwright auth state has no cookie for the owned origin"
            )
        return "; ".join(f"{name}={value}" for name, value in selected)

    @staticmethod
    def _resource_thread_ids(resource_ledger: Mapping[str, Any]) -> list[str]:
        if resource_ledger.get("schema") != 1:
            raise SafetyError("browser resource ledger has an unsupported schema")
        resources = resource_ledger.get("resources")
        if not isinstance(resources, list):
            raise SafetyError("browser resource ledger has no resource list")
        result: list[str] = []
        uuid_pattern = re.compile(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
            re.IGNORECASE,
        )
        for resource in resources:
            if not isinstance(resource, dict) or set(resource).difference(
                {"kind", "id", "created_at", "cleaned_at", "cleanup_status"}
            ):
                raise SafetyError(
                    "browser resource ledger contains an unexpected entry"
                )
            if resource.get("kind") != "thread":
                raise SafetyError(
                    "browser resource ledger contains an unsupported kind"
                )
            thread_id = resource.get("id")
            if not isinstance(thread_id, str) or not uuid_pattern.fullmatch(thread_id):
                raise SafetyError(
                    "browser resource ledger contains an invalid thread id"
                )
            if thread_id in result:
                raise SafetyError(
                    "browser resource ledger contains a duplicate thread id"
                )
            result.append(thread_id)
        return result

    @classmethod
    def _mark_resource_cleanup_complete(
        cls,
        resource_ledger: dict[str, Any],
        cleanup_results: Sequence[Mapping[str, str]],
    ) -> None:
        expected_ids = cls._resource_thread_ids(resource_ledger)
        result_ids = [result.get("id") for result in cleanup_results]
        if len(result_ids) != len(set(result_ids)) or set(result_ids) != set(
            expected_ids
        ):
            raise SafetyError(
                "refusing to complete a resource ledger without exact cleanup results"
            )
        completed_at = utc_now()
        resources = resource_ledger["resources"]
        for resource in resources:
            resource["cleaned_at"] = completed_at
            resource["cleanup_status"] = "verified-absent"
        resource_ledger["cleanup_complete"] = True
        resource_ledger["cleanup_completed_at"] = completed_at

    def _cleanup_threads(
        self,
        ledger: Mapping[str, Any],
        thread_ids: Sequence[str],
        cookie_header: str,
    ) -> list[dict[str, str]]:
        if not thread_ids:
            return []
        timeout_seconds = int(os.environ.get("APP_E2E_CLEANUP_TIMEOUT_SECONDS", "180"))
        if not 1 <= timeout_seconds <= 300:
            raise SafetyError("cleanup timeout must be between 1 and 300 seconds")
        force_timeout_seconds = int(
            os.environ.get("APP_E2E_FORCE_CLEANUP_TIMEOUT_SECONDS", "60")
        )
        if not 1 <= force_timeout_seconds <= 120:
            raise SafetyError(
                "forced cleanup timeout must be between 1 and 120 seconds"
            )
        results: list[dict[str, str]] = []
        with PortForward(
            kubeconfig=self._kubeconfig(ledger),
            namespace=NAMESPACE,
            resource="service/srw-e2e-orchestrator",
            remote_port=8085,
        ) as forward:
            assert forward.local_port is not None
            root = f"http://127.0.0.1:{forward.local_port}"
            headers = {
                "Cookie": cookie_header,
                "X-CSRF": "1",
                "Origin": BASE_URL,
                "Host": BASE_HOST,
            }
            for thread_id in reversed(thread_ids):
                path = f"/api/persistent/threads/{urllib.parse.quote(thread_id)}"
                status = 0
                deleted = False
                forced = False
                for forced, phase_budget in (
                    (False, timeout_seconds),
                    (True, force_timeout_seconds),
                ):
                    # Force remains legal only after the whole graceful budget
                    # for this exact validated ledger ID. A 2xx can acknowledge
                    # asynchronous pinned retirement without deleting the row.
                    deadline = time.monotonic() + phase_budget
                    query = "?permanent=true" + ("&force=true" if forced else "")
                    while True:
                        now = time.monotonic()
                        if now >= deadline:
                            break
                        status, _body = _http_request(
                            f"{root}{path}{query}",
                            method="DELETE",
                            headers=headers,
                            expected=(200, 202, 204, 404, 409, 503),
                            # Synchronous stateless retirement retains this
                            # phase's full remaining lifecycle request budget.
                            timeout=max(0.1, deadline - now),
                        )
                        if status == 404:
                            deleted = True
                            break
                        if status not in {409, 503}:
                            verify_status, _body = _http_request(
                                f"{root}{path}",
                                headers=headers,
                                expected=(200, 404),
                                timeout=min(20, max(0.1, deadline - time.monotonic())),
                            )
                            if verify_status == 404:
                                deleted = True
                                break
                        time.sleep(min(2, max(0.0, deadline - time.monotonic())))
                    if deleted:
                        break
                if not deleted:
                    raise HarnessError("bounded exact-id force cleanup did not settle")
                results.append(
                    {
                        "kind": "thread",
                        "id": thread_id,
                        "status": str(status),
                        "forced": str(forced).lower(),
                    }
                )
        return results

    def _cleanup_provider_run(
        self, ledger: Mapping[str, Any], resource_ledger: Mapping[str, Any]
    ) -> None:
        run_id = resource_ledger.get("run_id")
        if not isinstance(run_id, str) or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9_-]{2,127}", run_id
        ):
            return
        bundle = self._load_secrets(ledger)
        with PortForward(
            kubeconfig=self._kubeconfig(ledger),
            namespace=NAMESPACE,
            resource="deployment/srw-e2e-model-fixture",
            remote_port=8001,
        ) as forward:
            assert forward.local_port is not None
            root = f"http://127.0.0.1:{forward.local_port}/control/scenarios/{urllib.parse.quote(run_id)}"
            headers = {"Authorization": f"Bearer {bundle.provider_control_token}"}
            try:
                settle_seconds = float(
                    os.environ.get("APP_E2E_PROVIDER_SETTLE_SECONDS", "5")
                )
                timeout_seconds = float(
                    os.environ.get("APP_E2E_PROVIDER_SETTLE_TIMEOUT_SECONDS", "120")
                )
            except ValueError as exc:
                raise HarnessError(
                    "provider settlement bounds must be numeric"
                ) from exc
            if settle_seconds < 0 or timeout_seconds <= 0:
                raise HarnessError("provider settlement bounds are invalid")

            deadline = time.monotonic() + timeout_seconds
            stable_since: float | None = None
            stable_call_count: int | None = None
            state: dict[str, Any] | None = None
            while time.monotonic() < deadline:
                state_status, state_body = _http_request(
                    root, headers=headers, expected=(200, 404)
                )
                if state_status == 404:
                    return
                candidate = _json_body(state_body, label="provider cleanup state")
                if not isinstance(candidate, dict):
                    raise HarnessError("provider cleanup state is invalid")
                if candidate.get("unexpected_count") != 0:
                    raise HarnessError("provider scenario ended with unexpected calls")
                call_count = len(candidate.get("calls", []))
                settled = (
                    candidate.get("pending_calls") == 0
                    and candidate.get("remaining_required_responses") == 0
                )
                now = time.monotonic()
                if settled and call_count == stable_call_count:
                    stable_since = stable_since if stable_since is not None else now
                    if now - stable_since >= settle_seconds:
                        state = candidate
                        break
                else:
                    stable_since = now if settled else None
                    stable_call_count = call_count if settled else None
                time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
            if state is None:
                raise HarnessError(
                    "provider scenario did not settle with all required calls consumed"
                )

            overview_root = root.rsplit("/", maxsplit=1)[0]
            _overview_status, overview_body = _http_request(
                overview_root, headers=headers
            )
            overview = _json_body(overview_body, label="provider cleanup overview")
            if (
                not isinstance(overview, dict)
                or overview.get("unscoped_unexpected_calls") != 0
                or overview.get("unscoped_calls_truncated") != 0
                or overview.get("unscoped_calls") != []
            ):
                raise HarnessError(
                    "provider cleanup found rejected or unscoped requests"
                )
            write_private_json(
                self._run_dir(ledger) / "provider-cleanup-state.json",
                {"scenario": state, "overview": overview},
            )
            _http_request(root, method="DELETE", headers=headers, expected=(200,))

    def cleanup(self, ledger: Mapping[str, Any]) -> None:
        self._assert_owned_cluster(ledger)
        run_dir = self._run_dir(ledger)
        resource_path = run_dir / "browser/browser-resources.json"
        if not resource_path.exists():
            print(
                "[e2e-app] no browser resource ledger; exact cleanup has nothing to do",
                flush=True,
            )
            return
        resource_ledger = read_private_json(resource_path)
        if not isinstance(resource_ledger, dict):
            raise SafetyError("browser resource ledger must be a JSON object")
        thread_ids = self._resource_thread_ids(resource_ledger)
        auth_path = run_dir / "browser/.auth/journey.json"
        if thread_ids and not auth_path.exists():
            raise HarnessError(
                "cannot clean exact threads without the saved journey auth state"
            )
        results: list[dict[str, str]] = []
        if thread_ids:
            auth_state = read_private_json(auth_path)
            if not isinstance(auth_state, dict):
                raise SafetyError("Playwright auth state must be a JSON object")
            cookie_header = self._cookie_header(auth_state, BASE_HOST)
            results = self._cleanup_threads(ledger, thread_ids, cookie_header)
        self._cleanup_provider_run(ledger, resource_ledger)
        self._mark_resource_cleanup_complete(resource_ledger, results)
        write_private_json(resource_path, resource_ledger)
        write_private_json(
            run_dir / "cleanup.json",
            {
                "run_id": ledger["run_id"],
                "completed_at": utc_now(),
                "resources": results,
            },
        )
        print(
            f"[e2e-app] exact cleanup verified ({len(results)} thread(s))", flush=True
        )

    def _remove_run_images(self, ledger: Mapping[str, Any]) -> None:
        recorded_images = ledger.get("images")
        if recorded_images == {}:
            return
        if not isinstance(recorded_images, dict):
            raise SafetyError("ownership ledger image map is invalid")
        sha = str(ledger.get("source_revision", ""))
        run_id = validate_run_id(str(ledger["run_id"]))
        dirty = ledger.get("source_dirty") is True
        profile = profile_from_ledger(ledger)
        expected_images, _commands = build_image_commands(
            sha,
            run_id,
            dirty=dirty,
            include_workspace=profile.include_workspace_image,
        )
        if recorded_images != expected_images:
            raise SafetyError("ownership ledger image tags are not run-derived")
        recorded_ids = ledger.get("image_ids", {})
        if not isinstance(recorded_ids, dict) or set(recorded_ids).difference(
            expected_images
        ):
            raise SafetyError("ownership ledger image ids are invalid")
        for component, image_ref in expected_images.items():
            identity = self.runner.run(
                [
                    "docker",
                    "image",
                    "inspect",
                    "--format",
                    '{{.Id}}|{{ index .Config.Labels "srw.io/e2e-owner" }}|{{ index .Config.Labels "srw.io/source-revision" }}',
                    image_ref,
                ],
                check=False,
                label=f"{component} teardown image inspection",
            )
            if identity.returncode:
                continue
            parts = identity.stdout.strip().split("|")
            recorded_id = recorded_ids.get(component)
            if (
                len(parts) != 3
                or not re.fullmatch(r"sha256:[0-9a-f]{64}", parts[0])
                or parts[1:] != [run_id, sha]
                or (
                    recorded_id is not None
                    and (not isinstance(recorded_id, str) or parts[0] != recorded_id)
                )
            ):
                raise SafetyError(
                    f"refusing to remove {component} image with mismatched ownership"
                )
            self.runner.run(
                ["docker", "image", "rm", image_ref],
                label=f"exact {component} image removal",
            )
            remaining = self.runner.run(
                ["docker", "image", "inspect", image_ref], check=False
            )
            if remaining.returncode == 0:
                raise SafetyError(f"run-owned {component} image remained after removal")

    def _remove_image_volume(self, ledger: Mapping[str, Any]) -> None:
        cluster_name = validate_cluster_name(str(ledger["cluster_name"]))
        volume_name = f"k3d-{cluster_name}-images"
        recorded_name = ledger.get("image_volume_name")
        if recorded_name not in {None, volume_name}:
            raise SafetyError("ownership ledger image volume name is invalid")
        identity = self.runner.run(
            [
                "docker",
                "volume",
                "inspect",
                "--format",
                '{{.Name}}|{{ index .Labels "k3d.cluster" }}|{{ index .Labels "app" }}',
                volume_name,
            ],
            check=False,
            label="teardown image volume inspection",
        )
        if identity.returncode:
            return
        if identity.stdout.strip().split("|") != [volume_name, cluster_name, "k3d"]:
            raise SafetyError(
                "refusing to remove an image volume with mismatched ownership"
            )
        self.runner.run(
            ["docker", "volume", "rm", volume_name],
            label="exact k3d image volume removal",
        )
        remaining = self.runner.run(
            ["docker", "volume", "inspect", volume_name], check=False
        )
        if remaining.returncode == 0:
            raise SafetyError("run-owned k3d image volume remained after removal")

    def _remove_credentials(self, ledger: Mapping[str, Any]) -> None:
        run_dir = self._run_dir(ledger)
        exact_files = (
            run_dir / "credentials.json",
            run_dir / "kubeconfig.yaml",
            run_dir / "browser.env",
            run_dir / "vm-ssh-key",
            run_dir / "vm-ssh-key.pub",
            run_dir / "browser/.auth/journey.json",
            run_dir / "browser/.auth/journey.json.candidate",
        )
        for path in exact_files:
            resolved = path.resolve()
            if path.is_symlink() or resolved != path:
                raise SafetyError("refusing to remove a symlinked credential path")
            if path.is_file():
                path.unlink()
        node_modules = run_dir / "node_modules"
        if node_modules.is_symlink():
            raise SafetyError("refusing to remove a symlinked node_modules path")
        if node_modules.is_dir():
            shutil.rmtree(node_modules)
        auth_dir = run_dir / "browser/.auth"
        if auth_dir.is_dir() and not any(auth_dir.iterdir()):
            auth_dir.rmdir()
        mountpoint = REPO_ROOT / "cockpit/test-results/app" / str(ledger["run_id"])
        if mountpoint.is_symlink():
            raise SafetyError("refusing to remove a symlinked browser mountpoint")
        if mountpoint.is_dir() and not any(mountpoint.iterdir()):
            mountpoint.rmdir()

    def down(self, ledger: dict[str, Any]) -> None:
        self.store.validate(ledger)
        cluster_name = validate_cluster_name(str(ledger["cluster_name"]))
        container_name = f"k3d-{cluster_name}-server-0"
        live = self.runner.run(["docker", "inspect", container_name], check=False)
        if live.returncode == 0:
            self._assert_owned_cluster(ledger)
            self.runner.run(
                ["k3d", "cluster", "delete", cluster_name],
                timeout=240,
                label="exact owned cluster deletion",
            )
        inventory = self.runner.run(
            ["k3d", "cluster", "list", "-o", "json"],
            label="post-delete k3d cluster inventory",
        ).stdout
        try:
            entries = json.loads(inventory or "[]")
        except json.JSONDecodeError as exc:
            raise SafetyError("cannot prove that the owned cluster is absent") from exc
        if any(
            isinstance(item, dict) and item.get("name") == cluster_name
            for item in entries
        ):
            message = (
                "cluster deletion did not remove the exact owned cluster"
                if live.returncode == 0
                else "cluster name exists but its owned server identity is missing"
            )
            raise SafetyError(message)
        remaining = self.runner.run(["docker", "inspect", container_name], check=False)
        if remaining.returncode == 0:
            raise SafetyError("owned server container remained after cluster deletion")
        self._remove_image_volume(ledger)
        self._remove_run_images(ledger)
        ledger["cluster_deleted_at"] = utc_now()
        self._remove_credentials(ledger)
        self.store.clear_active(ledger)
        print(f"[e2e-app] deleted exact owned cluster {cluster_name}", flush=True)


def _state_root_from_environment() -> Path:
    configured = os.environ.get("APP_E2E_STATE_DIR")
    return Path(configured).expanduser() if configured else DEFAULT_STATE_ROOT


def _safe_error_text(exc: BaseException, harness: ApplicationE2EHarness) -> str:
    secrets_to_redact: Iterable[str] = ()
    try:
        ledger = harness.store.load()
        secrets_to_redact = harness._load_secrets(ledger).secret_values()
    except BaseException:
        pass
    return (
        sanitize_diagnostic(str(exc), secrets_to_redact).strip() or type(exc).__name__
    )


def _best_effort_diagnostics(harness: ApplicationE2EHarness) -> None:
    try:
        ledger = harness.store.load()
        if ledger.get("created_by_run"):
            harness.diagnostics(ledger)
    except BaseException as exc:
        print(
            f"[e2e-app] diagnostics also failed: {_safe_error_text(exc, harness)}",
            file=sys.stderr,
        )


def _best_effort_down(harness: ApplicationE2EHarness) -> BaseException | None:
    try:
        ledger = harness.store.load()
    except BaseException as exc:
        if not os.path.lexists(harness.store.active_path):
            return None
        print(
            f"[e2e-app] teardown ownership load failed: {_safe_error_text(exc, harness)}",
            file=sys.stderr,
        )
        return exc
    try:
        harness.down(ledger)
        return None
    except BaseException as exc:
        print(
            f"[e2e-app] teardown failed safely: {_safe_error_text(exc, harness)}",
            file=sys.stderr,
        )
        return exc


def _run_authoritative(
    harness: ApplicationE2EHarness, profile_name: str = DEFAULT_PROFILE_NAME
) -> int:
    ledger: dict[str, Any] | None = None
    primary_error: BaseException | None = None
    cleanup_error: BaseException | None = None
    down_error: BaseException | None = None
    try:
        ledger = harness.up(profile_name)
        harness.test_owned(ledger)
    except BaseException as exc:  # cleanup/teardown must also run on Ctrl-C
        primary_error = exc
        _best_effort_diagnostics(harness)
    try:
        ledger = harness.store.load()
        if ledger.get("created_by_run"):
            harness.cleanup(ledger)
    except BaseException as exc:
        cleanup_error = exc
        if primary_error is None:
            _best_effort_diagnostics(harness)
    finally:
        down_error = _best_effort_down(harness)
    if primary_error is not None:
        print(
            f"[e2e-app] failed: {_safe_error_text(primary_error, harness)}",
            file=sys.stderr,
        )
        if cleanup_error is not None:
            print(
                f"[e2e-app] cleanup additionally failed: {_safe_error_text(cleanup_error, harness)}",
                file=sys.stderr,
            )
        if down_error is not None:
            print(
                f"[e2e-app] teardown additionally failed: {_safe_error_text(down_error, harness)}",
                file=sys.stderr,
            )
        return 1
    if cleanup_error is not None:
        print(
            f"[e2e-app] cleanup failed: {_safe_error_text(cleanup_error, harness)}",
            file=sys.stderr,
        )
        return 1
    if down_error is not None:
        print(
            f"[e2e-app] teardown failed: {_safe_error_text(down_error, harness)}",
            file=sys.stderr,
        )
        return 1
    evidence = (
        "authoritative"
        if ledger is not None and ledger.get("authoritative") is True
        else "non-authoritative dirty-tree"
    )
    print(
        f"[e2e-app] {evidence} {profile_name} golden journey passed and teardown "
        "completed",
        flush=True,
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    profile_choices = tuple(APPLICATION_E2E_PROFILES)
    run_parser = subparsers.add_parser(
        "run", help="own the complete up/test/cleanup/down lifecycle"
    )
    run_parser.add_argument(
        "--profile", choices=profile_choices, default=DEFAULT_PROFILE_NAME
    )
    up_parser = subparsers.add_parser(
        "up", help="create and preflight a fresh owned environment"
    )
    up_parser.add_argument(
        "--profile", choices=profile_choices, default=DEFAULT_PROFILE_NAME
    )
    test_parser = subparsers.add_parser("test", help="run the browser journey")
    test_parser.add_argument(
        "--attach",
        action="store_true",
        help="explicit non-authoritative verification against an existing stack",
    )
    subparsers.add_parser("diagnostics", help="capture the bounded sanitized bundle")
    subparsers.add_parser("cleanup", help="clean only exact browser-ledger resources")
    subparsers.add_parser("down", help="delete only the exact ledger-owned cluster")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    harness = ApplicationE2EHarness(_state_root_from_environment())
    try:
        if arguments.command == "run":
            return _run_authoritative(harness, arguments.profile)
        if arguments.command == "up":
            try:
                harness.up(arguments.profile)
            except BaseException:
                _best_effort_diagnostics(harness)
                down_error = _best_effort_down(harness)
                if down_error is not None:
                    print(
                        f"[e2e-app] teardown additionally failed: {_safe_error_text(down_error, harness)}",
                        file=sys.stderr,
                    )
                raise
            return 0
        if arguments.command == "test" and arguments.attach:
            harness.check_attach_prerequisites()
            harness.test_attached()
            return 0
        ledger = harness.store.load()
        if arguments.command == "test":
            harness.test_owned(ledger)
        elif arguments.command == "diagnostics":
            harness.diagnostics(ledger)
        elif arguments.command == "cleanup":
            harness.cleanup(ledger)
        elif arguments.command == "down":
            harness.down(ledger)
        else:  # argparse keeps this unreachable.
            raise HarnessError("unsupported command")
        return 0
    except KeyboardInterrupt:
        print("[e2e-app] interrupted", file=sys.stderr)
        return 130
    except BaseException as exc:
        print(f"[e2e-app] error: {_safe_error_text(exc, harness)}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
