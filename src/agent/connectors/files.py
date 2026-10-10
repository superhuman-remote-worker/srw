"""``credential_file``: credential files (kubeconfig, generic file), in the workspace.

The files go to the workspace the shell runs in, for jobs and sessions
alike, and never to the agent pod. Each file's contents go, with the mode
its connector names (never an execute bit), into a private store under
``~/.srw-credentials/`` over the backend's secret stdin channel
(``RemoteBackend.install_credential_files``); its target path becomes a
symlink to it. A snapshot never captures the store, only the links.

The orchestrator stores each target resolved against ``/home/srw``; here it
becomes the same path under the workspace home, if the credential-file
allowlist (:mod:`shared.connectors.file_targets`) permits it. The
orchestrator refuses anything else when a connector is saved; a row saved
before that rule is skipped here and the README says why. So is a file's
``env_var`` that no connector may set
(``shared.connectors.env_names.connector_env_problem``). A target that
already holds a file of the user's is left alone (the contents still reach
the store).

Every delivery on a shell workspace syncs the whole current set, empty or
not: a detach, live or between attaches, removes the file and unsets the
variable that named it, and a backend swap writes the set again on the new
host before the old one retires. The terminal shell retirement removes them
with the work item (``RemoteBackend.shell_cleanup``): a session's End, a
job's completed, failed or cancelled status while its agent runs, the
retired side of a tier swap.

Known gap (slice D1d): a job that ends without a live agent never runs that
retirement. Paused or pending_review, then cancelled (or approved) from the
cockpit, the job's store and links stay in its workspace volume until the
workspace is torn down, at the latest when the job is deleted. No snapshot
ever holds them (``.srw-credentials`` is excluded), and no later work item
reads them (each has its own store and sources only its own environment
file). There is no claim-fenced path to the workspace at that moment: the
orchestrator's cancel does not reach it, and the teardown deletes the
volume itself.

An entry's ``transform`` and ``merge_group`` carry what used to be a
kubeconfig type check: kubeconfig names are prefixed with the connector's
slug, and every kubeconfig is merged into one config that ``KUBECONFIG``
names (after the user's own ``~/.kube/config``, if there is one, so the
user's current context stays the default) and ``~/.kube/config`` links to
otherwise. Another connector's file at ``~/.kube/config`` is treated as the
user's: never linked over, listed first in ``KUBECONFIG``, and the README
says so. A kubeconfig whose own target another connector's file claims still
joins the merge, stored without a link. An entry's ``env_var`` names the file
in the shell's environment, unless another connector already set it.
"""

from __future__ import annotations

import hashlib
import logging
import posixpath
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, NamedTuple

from agent.connectors.base import (
    Delivery,
    FactsLines,
    RuntimeContext,
    read_only_note,
)
from agent.connectors.legacy import unreadable_file_modes
from shared.connectors.env_names import connector_env_problem
from shared.connectors.file_targets import mode_problem, safe_mode, target_problem

logger = logging.getLogger(__name__)

#: The merged kubeconfig, in the store and at its link.
MERGED_KUBECONFIG = "kubeconfig"
MERGED_KUBECONFIG_LINK = ".kube/config"
#: The variable the kubeconfig merge owns.
KUBECONFIG_VAR = "KUBECONFIG"

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
    return target_problem(path)


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
    ``link``); ``env`` its variables (``name``, ``files``: the store names
    the variable lists, ``prepend``: a home path listed before them if it
    holds a file of the user's). A ``KUBECONFIG`` whose kubeconfigs could not
    merge lists every kubeconfig.
    """

    files: list[dict[str, Any]] = field(default_factory=list)
    env: list[dict[str, Any]] = field(default_factory=list)
    #: The connector whose own file holds ``~/.kube/config``: the merged
    #: kubeconfig is listed after it in ``KUBECONFIG``, never linked over it.
    kubeconfig_after: str | None = None

    @property
    def env_names(self) -> list[str]:
        return [item["name"] for item in self.env]


class _Owner(NamedTuple):
    """Who delivers a home path: its stored file, its connector, and whether
    it is one of the kubeconfigs the merge holds."""

    stored: str
    connector: str
    merged: bool


def plan_credential_files(
    deliveries: Sequence[Delivery], *, quiet: bool = False
) -> CredentialFilePlan:
    """The files and variables the deliveries put in the workspace."""

    def warn(message: str, *args: Any) -> None:
        if not quiet:
            logger.warning(message, *args)

    plan = CredentialFilePlan()
    owners: dict[str, _Owner] = {}
    named: set[str] = set()
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
            group = value.get("merge_group")
            link: str | None = relative
            stored = _store_name(relative)
            if relative in owners:
                if not group:
                    warn(
                        "Skipping a credential file for '%s': another connector "
                        "already delivers ~/%s",
                        name,
                        relative,
                    )
                    continue
                # Another connector's file claims this kubeconfig's own
                # target: the kubeconfig still joins the merge, stored
                # without a link of its own.
                warn(
                    "~/%s is another connector's file; '%s''s kubeconfig is "
                    "merged without a link of its own",
                    relative,
                    name,
                )
                link = None
                stored = _store_name(f"{relative}#{delivery.index}.{index}")
            else:
                owners[relative] = _Owner(stored, name, bool(group))
            contents = str(value.get("content") or "")
            if value.get("transform") == "kubeconfig_prefix":
                contents = _prefix_kubeconfig_yaml(contents, _ds_slug_hyphen(name))
            bad_mode = bad_modes[index] if index < len(bad_modes) else None
            if bad_mode is not None:
                warn("Bad mode %r on '%s'; using 0600", bad_mode, name)
            mode = int(value.get("mode", 0o600))
            if mode_problem(mode) is not None:
                warn(
                    "Mode %04o on '%s' grants more than read and write; using %04o",
                    mode,
                    name,
                    safe_mode(mode),
                )
            plan.files.append(
                {
                    "name": stored,
                    "content": contents,
                    "mode": safe_mode(mode),
                    "link": link,
                }
            )
            env_var = value.get("env_var")
            if env_var:
                refused = connector_env_problem(env_var)
                if refused is not None:
                    warn("Skipping %s for '%s': %s", env_var, name, refused)
                elif env_var in named:
                    warn("Skipping %s for '%s': already set", env_var, name)
                else:
                    named.add(env_var)
                    plan.env.append({"name": env_var, "files": [stored]})
            if group:
                groups.setdefault(group, []).append((stored, contents))

    kubeconfigs = groups.get("kubeconfig")
    if kubeconfigs:
        # Another connector's own file at ~/.kube/config (not one of the
        # merged kubeconfigs) keeps its place.
        claimed = owners.get(MERGED_KUBECONFIG_LINK)
        other = claimed if claimed is not None and not claimed.merged else None
        merged = merge_kubeconfigs([contents for _stored, contents in kubeconfigs])
        if merged is None:
            warn("Kubeconfigs could not be merged; KUBECONFIG lists each file")
            files = [stored for stored, _ in kubeconfigs]
        else:
            plan.files.append(
                {
                    "name": MERGED_KUBECONFIG,
                    "content": merged,
                    "mode": 0o600,
                    "link": None if other is not None else MERGED_KUBECONFIG_LINK,
                }
            )
            files = [MERGED_KUBECONFIG]
        if other is not None:
            warn(
                "'%s' delivers ~/%s; the merged kubeconfig is listed after it "
                "in KUBECONFIG",
                other.connector,
                MERGED_KUBECONFIG_LINK,
            )
            files = [other.stored, *files]
            plan.kubeconfig_after = other.connector
        # The user's own ~/.kube/config, if the merged one could not take its
        # place, comes first: its current-context stays the default and a
        # shared connector's never replaces it. Another connector's file
        # there is listed first the same way (above).
        plan.env.append(
            {"name": KUBECONFIG_VAR, "files": files, "prepend": MERGED_KUBECONFIG_LINK}
        )
    return plan


def sync_credential_files(
    deliveries: Sequence[Delivery], backend: Any
) -> dict[str, Any] | None:
    """Make the workspace hold exactly the deliveries' files.

    The workspace program empties a variable an earlier sync set and this
    one does not, from its own record, and never overwrites one another
    connector set. Returns its report.
    """
    plan = plan_credential_files(deliveries)
    report = backend.install_credential_files(plan.files, plan.env)
    report = report if isinstance(report, Mapping) else {}
    skipped = report.get("skipped") or {}
    env_skipped = report.get("env_skipped") or {}
    for link, reason in skipped.items():
        logger.warning("Credential file not linked at ~/%s: %s", link, reason)
    for name, reason in env_skipped.items():
        logger.warning("Credential file variable %s not set: %s", name, reason)
    logger.info(
        "Credential files in the workspace: %d file(s), %d linked, variables %s",
        len(plan.files),
        len(report.get("linked") or ()),
        sorted(report.get("env") or ()) or "none",
    )
    return dict(report)


def _deliver(deliveries: Sequence[Delivery], backend: Any) -> None:
    """Best effort: a file that cannot be delivered never fails the work.

    Runs on every shell workspace, with or without deliveries, so what an
    earlier attach left behind goes once its connector is gone.
    """
    if backend is None or not getattr(backend, "supports_shell", False):
        if deliveries:
            logger.warning(
                "Credential files need a sandbox or VM workspace; not delivered: %s",
                ", ".join(repr(delivery.name) for delivery in deliveries),
            )
        return
    try:
        sync_credential_files(deliveries, backend)
    except Exception as e:
        logger.warning("Failed to deliver credential files to the workspace: %s", e)


class CredentialFileMaterializer:
    form = "credential_file"

    def materialize(self, deliveries: Sequence[Delivery], rt: RuntimeContext) -> None:
        _deliver(deliveries, rt.workspace_backend)

    def replace(
        self,
        old: Sequence[Delivery],
        new: Sequence[Delivery],
        rt: RuntimeContext,
    ) -> None:
        _deliver(new, rt.workspace_backend)

    def on_backend_swap(self, deliveries: Sequence[Delivery], backend: Any) -> None:
        _deliver(deliveries, backend)

    def facts(
        self, deliveries: Sequence[Delivery], rt: RuntimeContext
    ) -> list[FactsLines]:
        report = _report(rt)
        after = plan_credential_files(deliveries, quiet=True).kubeconfig_after
        out: list[FactsLines] = []
        for delivery in deliveries:
            name = delivery.name
            values = delivery.values("credential_file")
            if _files_slot_kind(delivery) == "kubeconfig":
                line = _kubeconfig_line(name, values, report, after=after)
            else:
                paths = (
                    ", ".join(_fact_path(value, report) for value in values) or "<none>"
                )
                line = f"- **{name}** (file) — {paths}"
            # The same file is delivered either way: read-only is this note.
            line += read_only_note(delivery.entry)
            out.append(FactsLines("Credential Files", delivery.index, [line]))
        return out


def _report(rt: RuntimeContext) -> Mapping[str, Any]:
    """The last sync's report on this workspace (empty before any sync)."""
    report = getattr(rt.workspace_backend, "credential_files_report", None)
    return report if isinstance(report, Mapping) else {}


def _files_slot_kind(delivery: Delivery) -> str | None:
    if delivery.spec is None:
        return None
    return next(
        (slot.kind for slot in delivery.spec.credential_slots if slot.name == "files"),
        None,
    )


def _kubeconfig_line(
    name: str,
    values: Sequence[Mapping[str, Any]],
    report: Mapping[str, Any],
    *,
    after: str | None = None,
) -> str:
    refused = [
        (str(value.get("path") or ""), problem)
        for value in values
        for _relative, problem in [home_target(str(value.get("path") or ""))]
        if problem is not None
    ]
    if values and len(refused) == len(values):
        path, problem = refused[0]
        return f"- **{name}** (kubeconfig) — `{path}` not delivered: {problem}"
    skipped = report.get("skipped") or {}
    env_skipped = report.get("env_skipped") or {}
    if KUBECONFIG_VAR in env_skipped:
        where = (
            "merged into `~/.kube/config` (`$KUBECONFIG` is another connector's)"
            if MERGED_KUBECONFIG_LINK not in skipped
            else "merged, but `~/.kube/config` and `$KUBECONFIG` are taken"
        )
    elif after is not None:
        where = (
            f"merged into `$KUBECONFIG` after **{after}**'s `~/.kube/config`, "
            "whose current context stays the default"
        )
    elif MERGED_KUBECONFIG_LINK in skipped:
        where = (
            "merged into `$KUBECONFIG` after your own `~/.kube/config`, whose "
            "current context stays the default"
        )
    else:
        where = "merged into `~/.kube/config` (`$KUBECONFIG`)"
    slug = _ds_slug_hyphen(name)
    return (
        f"- **{name}** (kubeconfig) — {where}; contexts prefixed `{slug}-*`. "
        "Where kubectl is installed, try `kubectl config get-contexts`."
    )


def _fact_path(value: Mapping[str, Any], report: Mapping[str, Any]) -> str:
    path = str(value.get("path") or "")
    relative, refused = home_target(path)
    if relative is None:
        return f"`{path}` (not delivered: {refused})"
    env_var = value.get("env_var")
    unnamed = connector_env_problem(env_var) if env_var else None
    named = bool(env_var) and unnamed is None
    taken = named and env_var in (report.get("env_skipped") or {})
    skipped = (report.get("skipped") or {}).get(relative)
    notes: list[str] = []
    if skipped:
        notes.append(f"not linked: {skipped}")
    if unnamed is not None:
        notes.append(f"variable not delivered by SRW: {unnamed}")
    elif named and not taken:
        notes.append(f"`${env_var}`")
    elif taken:
        notes.append(f"`${env_var}` is another connector's")
    return f"`~/{relative}`" + (f" ({'; '.join(notes)})" if notes else "")
