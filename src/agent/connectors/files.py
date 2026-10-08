"""``credential_file``: credential files (kubeconfig, generic file).

Each entry's ``recipient`` says where the file goes. Today that is only the
agent pod, for worker jobs (sessions never materialize credential files);
slice D1d moves them to the workspace for jobs and sessions alike, and an
entry addressed elsewhere is skipped until then. Writes never clobber a
file this materializer did not write, and a manifest records everything
written so :func:`cleanup_credential_files` can undo it at job end.

An entry's ``transform`` and ``merge_group`` carry what used to be a
kubeconfig type check: kubeconfig names are prefixed with the connector's
slug, and every kubeconfig is merged into ``~/.kube/config``.
"""

from __future__ import annotations

import logging
import os
import re
import subprocess
from collections.abc import Sequence
from typing import Any, Callable, Dict, List, Optional

from agent.connectors.base import AGENT_HOME, Delivery, FactsLines, RuntimeContext
from agent.connectors.legacy import deliveries_from_payload, unreadable_file_modes

logger = logging.getLogger(__name__)


def _ds_slug_hyphen(name: str) -> str:
    """Hyphenated slug for filenames and kubeconfig context prefixes.

    Matches ``slugify_datasource_name`` in
    ``src/orchestrator/security/credential_files.py``.
    """
    s = re.sub(r"[^a-z0-9]+", "-", (name or "").lower()).strip("-")
    return s or "unnamed"


def _retarget(path: str, home_dir: str) -> str:
    """Swap ``/home/srw`` for ``home_dir`` so tests can use a tmp directory.

    The orchestrator's validator already resolved ``~`` against
    ``/home/srw``; in production no swap is needed. In tests we pass a
    tmp ``home_dir`` and rewrite the prefix at write time.
    """
    if not path or home_dir == AGENT_HOME:
        return path
    if path == AGENT_HOME:
        return home_dir
    if path.startswith(AGENT_HOME + "/"):
        return home_dir + path[len(AGENT_HOME) :]
    return path


def _mkdir_tracking(path: str, created_dirs: List[str]) -> None:
    """``mkdir -p`` while recording each directory we (not the OS image) created.

    Cleanup uses this list to ``rmdir`` only the directories we made, leaving
    pre-existing ones like ``~/.ssh`` (which may have ``known_hosts``) intact.
    """
    if not path or path == "/" or os.path.isdir(path):
        return
    parent = os.path.dirname(path)
    if parent and parent != path:
        _mkdir_tracking(parent, created_dirs)
    try:
        os.mkdir(path)
        created_dirs.append(path)
    except FileExistsError:
        pass


def _prefix_kubeconfig_yaml(yaml_str: str, prefix: str) -> str:
    """Prefix every cluster/user/context name in a kubeconfig with ``<prefix>-``.

    Multi-cluster jobs merge several kubeconfigs into ``~/.kube/config``;
    without prefixing, two uploads with a context named ``default`` would
    collide. We pre-prefix per-datasource so the agent sees deterministic,
    collision-free context names like ``prod-eu-default``.

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


def _merge_kubeconfigs(
    kubeconfig_paths: List[str],
    home_dir: str,
    manifest: Dict[str, Any],
) -> Optional[str]:
    """Merge per-datasource kubeconfigs into ``~/.kube/config`` using ``kubectl``.

    Returns the merged absolute path on success. On failure (no kubectl,
    bad input) returns ``None`` and the per-datasource files remain
    available individually — the caller falls back to a colon-separated
    ``KUBECONFIG`` so kubectl can still find them.
    """
    if not kubeconfig_paths:
        return None
    merged_path = os.path.join(home_dir, ".kube", "config")
    if os.path.exists(merged_path):
        logger.warning(
            "Refusing to overwrite existing %s; agent will use per-ds KUBECONFIG list",
            merged_path,
        )
        return None
    _mkdir_tracking(os.path.dirname(merged_path), manifest["dirs"])
    env = {**os.environ, "KUBECONFIG": ":".join(kubeconfig_paths)}
    try:
        result = subprocess.run(
            ["kubectl", "config", "view", "--flatten", "--merge"],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        )
    except FileNotFoundError:
        logger.warning("kubectl not installed; falling back to KUBECONFIG=<colon-list>")
        return None
    except subprocess.CalledProcessError as e:
        logger.warning("kubectl config view failed: %s", e.stderr.strip())
        return None
    except subprocess.TimeoutExpired:
        logger.warning("kubectl config view timed out")
        return None

    try:
        with open(merged_path, "w") as f:
            f.write(result.stdout)
        os.chmod(merged_path, 0o600)
    except OSError as e:
        logger.warning("Failed to write merged kubeconfig %s: %s", merged_path, e)
        return None
    manifest["files"].append(merged_path)
    return merged_path


def _merge_kubeconfig_group(
    paths: List[str], home_dir: str, manifest: Dict[str, Any]
) -> None:
    """Point ``KUBECONFIG`` at the merged config, or at every file."""
    merged = _merge_kubeconfigs(paths, home_dir, manifest)
    os.environ["KUBECONFIG"] = merged if merged else ":".join(paths)
    manifest["env_vars"].append("KUBECONFIG")


#: How each merge group combines its files once all are written.
_MERGE_GROUPS: Dict[str, Callable[[List[str], str, Dict[str, Any]], None]] = {
    "kubeconfig": _merge_kubeconfig_group,
}


def _write_files(
    deliveries: Sequence[Delivery], home_dir: str, manifest: Dict[str, Any]
) -> None:
    groups: Dict[str, List[str]] = {}
    for delivery in deliveries:
        ds_name = delivery.name
        ds_slug = _ds_slug_hyphen(ds_name)
        entries = [
            entry
            for entry in (delivery.binding.entries if delivery.binding else ())
            if entry.form == "credential_file"
        ]
        bad_modes = unreadable_file_modes(delivery.entry)
        for index, entry in enumerate(entries):
            if entry.recipient != "agent_pod":
                logger.warning(
                    "Skipping a credential file for '%s': delivery to %s is "
                    "not supported yet",
                    ds_name,
                    entry.recipient,
                )
                continue
            value = entry.value
            absolute = _retarget(value["path"], home_dir)
            if not absolute:
                logger.warning(
                    "Skipping file entry with empty target_path on '%s'", ds_name
                )
                continue
            if os.path.exists(absolute):
                logger.warning(
                    "Refusing to overwrite existing file at %s (datasource '%s')",
                    absolute,
                    ds_name,
                )
                continue
            contents = value["content"]
            if value.get("transform") == "kubeconfig_prefix":
                contents = _prefix_kubeconfig_yaml(contents, ds_slug)
            bad_mode = bad_modes[index] if index < len(bad_modes) else None
            if bad_mode is not None:
                logger.warning("Bad mode %r on '%s'; using 0600", bad_mode, ds_name)
            mode = int(value.get("mode", 0o600))

            parent = os.path.dirname(absolute)
            if parent:
                _mkdir_tracking(parent, manifest["dirs"])
            try:
                with open(absolute, "w") as f:
                    f.write(contents)
                os.chmod(absolute, mode)
            except OSError as e:
                logger.warning(
                    "Failed to write credential file %s for '%s': %s",
                    absolute,
                    ds_name,
                    e,
                )
                continue
            manifest["files"].append(absolute)
            logger.info(
                "Materialized credential file for '%s' at %s (mode %04o)",
                ds_name,
                absolute,
                mode,
            )

            env_var = value.get("env_var")
            if env_var:
                os.environ[env_var] = absolute
                manifest["env_vars"].append(env_var)

            group = value.get("merge_group")
            if group:
                groups.setdefault(group, []).append(absolute)

    for group, paths in groups.items():
        merge = _MERGE_GROUPS.get(group)
        if merge is not None:
            merge(paths, home_dir, manifest)


def materialize_credential_files(
    deliveries: Sequence[Delivery], home_dir: str = AGENT_HOME
) -> Dict[str, Any]:
    """Write the deliveries' credential files and return their manifest::

        {
            "files":    [abs paths written],
            "dirs":     [abs dirs we created],
            "env_vars": [env var names we set],
        }

    Each file's parent is created (and recorded), a kubeconfig's names are
    prefixed, the file is written with its mode, and its ``env_var`` (if
    any) points at it. A path that already exists is skipped with a
    warning. Once every file is written, the kubeconfigs are merged with
    ``kubectl config view --flatten --merge`` into ``~/.kube/config`` and
    ``KUBECONFIG`` points there (or at every file, if the merge fails).
    """
    manifest: Dict[str, Any] = {"files": [], "dirs": [], "env_vars": []}
    _write_files(deliveries, home_dir, manifest)
    return manifest


def process_credential_files(
    ds_configs: List[Dict[str, Any]],
    home_dir: str = AGENT_HOME,
) -> Dict[str, Any]:
    """:func:`materialize_credential_files` for payload entries.

    ``home_dir`` overrides the ``/home/srw`` prefix of the stored target
    paths; production leaves the default and tests pass a tmp directory.
    """
    deliveries = [
        delivery
        for delivery in deliveries_from_payload(ds_configs)
        if delivery.routes_to("credential_file")
    ]
    return materialize_credential_files(deliveries, home_dir)


def cleanup_credential_files(manifest: Optional[Dict[str, Any]]) -> None:
    """Undo a credential-file manifest. Best-effort, never raises.

    Removes materialized files, unsets env vars, and ``rmdir``s only the
    directories the materialization step created (pre-existing dirs like
    ``~/.ssh`` are left alone).
    """
    if not manifest:
        return

    for env_var in manifest.get("env_vars", []) or []:
        os.environ.pop(env_var, None)

    for path in manifest.get("files", []) or []:
        try:
            os.unlink(path)
            logger.debug("Removed credential file %s", path)
        except FileNotFoundError:
            pass
        except OSError as e:
            logger.warning("Failed to remove credential file %s: %s", path, e)

    # Deepest first so children come out before their parents.
    for d in sorted(manifest.get("dirs", []) or [], key=lambda p: -len(p)):
        try:
            os.rmdir(d)
        except OSError:
            # Non-empty or already gone; either way, nothing to do.
            pass


def _files_slot_kind(delivery: Delivery) -> str | None:
    if delivery.spec is None:
        return None
    return next(
        (slot.kind for slot in delivery.spec.credential_slots if slot.name == "files"),
        None,
    )


class CredentialFileMaterializer:
    form = "credential_file"

    def materialize(self, deliveries: Sequence[Delivery], rt: RuntimeContext) -> None:
        # Best effort: a file that cannot be written never fails the job.
        try:
            rt.files_manifest = materialize_credential_files(deliveries, rt.home_dir)
        except Exception as e:
            logger.warning("Failed to materialize credential files: %s", e)
            rt.files_manifest = None

    def release(self, rt: RuntimeContext) -> None:
        if rt.files_manifest:
            try:
                cleanup_credential_files(rt.files_manifest)
            except Exception as e:
                logger.warning("Error cleaning up credential files: %s", e)
            rt.files_manifest = None

    def facts(
        self, deliveries: Sequence[Delivery], rt: RuntimeContext
    ) -> list[FactsLines]:
        out: list[FactsLines] = []
        for delivery in deliveries:
            ds = delivery.entry
            name = ds.get("name", "Unnamed")
            if _files_slot_kind(delivery) == "kubeconfig":
                slug = _ds_slug_hyphen(name)
                line = (
                    f"- **{name}** (kubeconfig) — merged into `~/.kube/config`; "
                    f"contexts prefixed `{slug}-*`. Try `kubectl config get-contexts`."
                )
            else:
                files = (ds.get("credentials") or {}).get("files") or []
                paths = (
                    ", ".join(f"`{f.get('target_path')}`" for f in files) or "<none>"
                )
                line = f"- **{name}** (file) — {paths}"
            out.append(FactsLines("Credential Files", delivery.index, [line]))
        return out
