"""``credential_file``: credential files (kubeconfig, generic file), in the workspace.

The files go to the workspace the shell runs in, for jobs and sessions
alike, and never to the agent pod. Each file's contents go, with the mode
its connector names, into a private store under ``~/.srw-credentials/``
over the backend's secret stdin channel
(``RemoteBackend.install_credential_files``); its target path becomes a
symlink to it. A snapshot never captures the store, only the links.

The orchestrator stores each target resolved against ``/home/srw``
(:data:`AGENT_HOME`); here it becomes the same path under the workspace
home. A target outside the home, inside SRW's own credential namespaces, or
on a file the shell or sshd runs is refused, and a target that already
exists is left alone (the contents still reach the store).

Every delivery syncs the whole current set: a live detach removes a file,
and a backend swap writes the set again on the new host before the old one
retires. The files live as long as the workspace does, as the environment
file does; an ended execution does not reach back into a workspace whose
shell it has already torn down.

An entry's ``transform`` and ``merge_group`` carry what used to be a
kubeconfig type check: kubeconfig names are prefixed with the connector's
slug, and every kubeconfig is merged into one config that ``KUBECONFIG``
names and ``~/.kube/config`` links to (unless the home has its own). An
entry's ``env_var`` names the file in the shell's environment.
"""

from __future__ import annotations

import hashlib
import logging
import posixpath
import re
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from agent.connectors.base import AGENT_HOME, Delivery, FactsLines, RuntimeContext
from agent.connectors.legacy import unreadable_file_modes
from shared.credential_connectors import normalize_credential_env

logger = logging.getLogger(__name__)

#: Home-relative targets a connector never links: shell and sshd start-up
#: files, and the tmux configuration SRW's shell runs with.
REFUSED_TARGETS: frozenset[str] = frozenset(
    {
        ".bashrc",
        ".bash_profile",
        ".bash_login",
        ".bash_logout",
        ".profile",
        ".tmux.conf",
        ".ssh/rc",
        ".ssh/environment",
        ".ssh/authorized_keys",
        ".ssh/authorized_keys2",
    }
)
#: Home-relative directories SRW owns: the credential store and the
#: managed repositories' ssh-agent namespace.
REFUSED_ROOTS: tuple[str, ...] = (".srw-credentials", ".ssh/srw-managed")

#: The merged kubeconfig, in the store and at its link.
MERGED_KUBECONFIG = "kubeconfig"
MERGED_KUBECONFIG_LINK = ".kube/config"

_SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")


def _ds_slug_hyphen(name: str) -> str:
    """Hyphenated slug for kubeconfig context prefixes.

    Matches ``slugify_datasource_name`` in
    ``src/orchestrator/security/credential_files.py``.
    """
    s = re.sub(r"[^a-z0-9]+", "-", (name or "").lower()).strip("-")
    return s or "unnamed"


def home_target(path: str) -> tuple[str | None, str | None]:
    """``(home-relative target, None)``, or ``(None, why it is refused)``."""
    if path.startswith("~/"):
        relative = path[2:]
    elif path.startswith(AGENT_HOME + "/"):
        relative = path[len(AGENT_HOME) + 1 :]
    else:
        return None, "outside the home"
    relative = posixpath.normpath(relative)
    if relative in ("", ".") or relative == ".." or relative.startswith("../"):
        return None, "outside the home"
    if relative in REFUSED_TARGETS or any(
        relative == root or relative.startswith(root + "/") for root in REFUSED_ROOTS
    ):
        return None, "reserved by the workspace"
    return relative, None


def _store_name(relative: str) -> str:
    """A stable, unique store name for a target: its digest and basename."""
    digest = hashlib.sha256(relative.encode("utf-8")).hexdigest()[:12]
    base = _SAFE_NAME.sub("-", posixpath.basename(relative)).strip("-.") or "file"
    return f"{digest}-{base}"[:96]


def _prefix_kubeconfig_yaml(yaml_str: str, prefix: str) -> str:
    """Prefix every cluster/user/context name in a kubeconfig with ``<prefix>-``.

    Several kubeconfigs merge into one config; without prefixing, two
    uploads with a context named ``default`` would collide. We pre-prefix
    per connector so the agent sees deterministic, collision-free context
    names like ``prod-eu-default``.

    Returns the re-emitted YAML. On a parse failure the original string is
    returned and a warning is logged — the upload may still be usable,
    just with un-prefixed contexts.
    """
    try:
        import yaml
    except ImportError:
        logger.warning("PyYAML not available; skipping kubeconfig prefixing")
        return yaml_str

    try:
        doc = yaml.safe_load(yaml_str)
    except yaml.YAMLError as e:
        logger.warning("Kubeconfig YAML parse failed (%s); writing un-prefixed", e)
        return yaml_str
    if not isinstance(doc, dict):
        return yaml_str

    def _pfx(name: Any) -> Any:
        return f"{prefix}-{name}" if isinstance(name, str) and name else name

    for cluster in doc.get("clusters") or []:
        if isinstance(cluster, dict) and "name" in cluster:
            cluster["name"] = _pfx(cluster["name"])
    for user in doc.get("users") or []:
        if isinstance(user, dict) and "name" in user:
            user["name"] = _pfx(user["name"])
    for ctx in doc.get("contexts") or []:
        if isinstance(ctx, dict):
            if "name" in ctx:
                ctx["name"] = _pfx(ctx["name"])
            inner = ctx.get("context")
            if isinstance(inner, dict):
                if "cluster" in inner:
                    inner["cluster"] = _pfx(inner["cluster"])
                if "user" in inner:
                    inner["user"] = _pfx(inner["user"])
    if doc.get("current-context"):
        doc["current-context"] = _pfx(doc["current-context"])

    return yaml.safe_dump(doc, sort_keys=False)


def merge_kubeconfigs(contents: Sequence[str]) -> str | None:
    """One kubeconfig holding every cluster, user and context, as kubectl
    merges them (the first of a name wins, so does the first
    ``current-context``). ``None`` if any input is not a kubeconfig mapping.
    """
    import yaml

    merged: dict[str, Any] = {
        "apiVersion": "v1",
        "kind": "Config",
        "preferences": {},
        "clusters": [],
        "users": [],
        "contexts": [],
        "current-context": "",
    }
    seen: dict[str, set[Any]] = {"clusters": set(), "users": set(), "contexts": set()}
    for text in contents:
        try:
            doc = yaml.safe_load(text)
        except yaml.YAMLError:
            return None
        if not isinstance(doc, dict):
            return None
        for key in ("clusters", "users", "contexts"):
            for item in doc.get(key) or []:
                if not isinstance(item, dict):
                    continue
                name = item.get("name")
                if name in seen[key]:
                    continue
                seen[key].add(name)
                merged[key].append(item)
        if not merged["current-context"] and doc.get("current-context"):
            merged["current-context"] = doc["current-context"]
    return yaml.safe_dump(merged, sort_keys=False)


@dataclass
class CredentialFilePlan:
    """What one sync sends to the workspace.

    ``files`` is the backend's list (``name``, ``content``, ``mode``,
    ``link``); ``env`` maps a variable to the store names it lists (a
    ``KUBECONFIG`` that could not merge lists every kubeconfig).
    """

    files: list[dict[str, Any]] = field(default_factory=list)
    env: dict[str, tuple[str, ...]] = field(default_factory=dict)


def _usable_env_name(name: str) -> bool:
    try:
        normalize_credential_env({name: ""})
    except ValueError:
        return False
    return True


def plan_credential_files(
    deliveries: Sequence[Delivery], *, quiet: bool = False
) -> CredentialFilePlan:
    """The files and variables the deliveries put in the workspace."""

    def warn(message: str, *args: Any) -> None:
        if not quiet:
            logger.warning(message, *args)

    plan = CredentialFilePlan()
    targets: set[str] = set()
    groups: dict[str, list[tuple[str, str]]] = {}
    for delivery in deliveries:
        name = delivery.name
        bad_modes = unreadable_file_modes(delivery.entry)
        for index, value in enumerate(delivery.values("credential_file")):
            relative, refused = home_target(str(value.get("path") or ""))
            if relative is None:
                warn(
                    "Skipping a credential file for '%s': its target is %s",
                    name,
                    refused,
                )
                continue
            if relative in targets:
                warn(
                    "Skipping a credential file for '%s': another connector "
                    "already delivers ~/%s",
                    name,
                    relative,
                )
                continue
            targets.add(relative)
            contents = str(value.get("content") or "")
            if value.get("transform") == "kubeconfig_prefix":
                contents = _prefix_kubeconfig_yaml(contents, _ds_slug_hyphen(name))
            bad_mode = bad_modes[index] if index < len(bad_modes) else None
            if bad_mode is not None:
                warn("Bad mode %r on '%s'; using 0600", bad_mode, name)
            stored = _store_name(relative)
            plan.files.append(
                {
                    "name": stored,
                    "content": contents,
                    "mode": int(value.get("mode", 0o600)),
                    "link": relative,
                }
            )
            env_var = value.get("env_var")
            if env_var:
                if not _usable_env_name(env_var):
                    warn("Skipping %s for '%s': the name is reserved", env_var, name)
                elif env_var in plan.env:
                    warn("Skipping %s for '%s': already set", env_var, name)
                else:
                    plan.env[env_var] = (stored,)
            group = value.get("merge_group")
            if group:
                groups.setdefault(group, []).append((stored, contents))

    kubeconfigs = groups.get("kubeconfig")
    if kubeconfigs:
        merged = merge_kubeconfigs([contents for _stored, contents in kubeconfigs])
        if "KUBECONFIG" in plan.env:
            warn("KUBECONFIG names the merged kubeconfig, not a connector file")
        if merged is None:
            warn("Kubeconfigs could not be merged; KUBECONFIG lists each file")
            plan.env["KUBECONFIG"] = tuple(stored for stored, _ in kubeconfigs)
        else:
            plan.files.append(
                {
                    "name": MERGED_KUBECONFIG,
                    "content": merged,
                    "mode": 0o600,
                    "link": MERGED_KUBECONFIG_LINK,
                }
            )
            plan.env["KUBECONFIG"] = (MERGED_KUBECONFIG,)
    return plan


def sync_credential_files(
    deliveries: Sequence[Delivery],
    backend: Any,
    *,
    retired_env: Sequence[str] = (),
) -> None:
    """Make the workspace hold exactly the deliveries' files.

    ``retired_env`` names variables an earlier delivery set and this one no
    longer does: they are emptied, since the environment file only merges.
    """
    plan = plan_credential_files(deliveries)
    store = backend.install_credential_files(plan.files)
    env = {
        name: ":".join(posixpath.join(store, stored) for stored in names)
        for name, names in plan.env.items()
    }
    env.update({name: "" for name in retired_env if name not in env})
    if env:
        backend.install_credential_environment(env)
    logger.info(
        "Credential files in the workspace: %d file(s), variables %s",
        len(plan.files),
        sorted(plan.env) or "none",
    )


def _deliver(
    deliveries: Sequence[Delivery],
    backend: Any,
    *,
    retired_env: Sequence[str] = (),
) -> None:
    """Best effort: a file that cannot be delivered never fails the work."""
    if backend is None or not getattr(backend, "supports_shell", False):
        if deliveries:
            logger.warning(
                "Credential files need a sandbox or VM workspace; not delivered: %s",
                ", ".join(repr(delivery.name) for delivery in deliveries),
            )
        return
    try:
        sync_credential_files(deliveries, backend, retired_env=retired_env)
    except Exception as e:
        logger.warning("Failed to deliver credential files to the workspace: %s", e)


class CredentialFileMaterializer:
    form = "credential_file"

    def materialize(self, deliveries: Sequence[Delivery], rt: RuntimeContext) -> None:
        if deliveries:
            _deliver(deliveries, rt.workspace_backend)

    def replace(
        self,
        old: Sequence[Delivery],
        new: Sequence[Delivery],
        rt: RuntimeContext,
    ) -> None:
        if not old and not new:
            return
        before = plan_credential_files(old, quiet=True).env
        after = plan_credential_files(new, quiet=True).env
        _deliver(
            new,
            rt.workspace_backend,
            retired_env=[name for name in before if name not in after],
        )

    def on_backend_swap(self, deliveries: Sequence[Delivery], backend: Any) -> None:
        if deliveries:
            _deliver(deliveries, backend)

    def facts(
        self, deliveries: Sequence[Delivery], rt: RuntimeContext
    ) -> list[FactsLines]:
        out: list[FactsLines] = []
        for delivery in deliveries:
            name = delivery.name
            values = delivery.values("credential_file")
            if _files_slot_kind(delivery) == "kubeconfig":
                slug = _ds_slug_hyphen(name)
                line = (
                    f"- **{name}** (kubeconfig) — merged into `~/.kube/config`; "
                    f"contexts prefixed `{slug}-*`. Try `kubectl config get-contexts`."
                )
            else:
                paths = ", ".join(_fact_path(value) for value in values) or "<none>"
                line = f"- **{name}** (file) — {paths}"
            out.append(FactsLines("Credential Files", delivery.index, [line]))
        return out


def _files_slot_kind(delivery: Delivery) -> str | None:
    if delivery.spec is None:
        return None
    return next(
        (slot.kind for slot in delivery.spec.credential_slots if slot.name == "files"),
        None,
    )


def _fact_path(value: Any) -> str:
    path = str(value.get("path") or "")
    relative, refused = home_target(path)
    if relative is None:
        return f"`{path}` (not delivered: {refused})"
    env_var = value.get("env_var")
    named = bool(env_var) and _usable_env_name(env_var)
    return f"`~/{relative}`" + (f" (`${env_var}`)" if named else "")
