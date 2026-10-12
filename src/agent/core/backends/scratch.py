"""Scratch workspace backend — disposable agent-local tmpdir for ``none`` mode.

The ``none`` tier (``knowledge-base/knowledge/features/no_workspace_agent_mode.md`` §6) registers
**no file tools at all**, but the graph's internal consumers (PlanManager, the
TodoManager archive, optional ``task_brief.md`` reads, the ``README.md``
facts block)
still expect *a* backend. Rather than auditing every call site for
None-safety, ``none`` mode constructs this ephemeral backend rooted in a
private tmpdir on the agent pod.

This is the ``tests/_fs_backend.py`` pattern promoted into production, with the
backend owning and cleaning up its own tmpdir. It does **not** violate the
"agent never uses its own filesystem as a workspace" rule in spirit: nothing
agent-driven can touch it — there are no file tools over it, and durable
outputs in ``none`` mode are ``freeze_data``, KB writes, and datasource writes.
Scratch state is disposable by design (the LangGraph checkpoint already lives
on agent-local disk; phase archives are mirrored to the Postgres audit trail).

``supports_shell`` is False and there is no ``$HOME`` concept, so shell, git,
and repository-datasource clones are unavailable by construction (§7).
"""

import logging
import shutil
import tempfile
from pathlib import Path

from shared.runtime.core.workspace_backend import WorkspaceBackend, search_excludes

logger = logging.getLogger(__name__)


class ScratchBackend(WorkspaceBackend):
    """Filesystem WorkspaceBackend rooted in an owned, disposable tmpdir.

    Args:
        job_id: Used only to label the tmpdir for debuggability.
        base_dir: Parent directory for the tmpdir (defaults to the system temp
            location). The tmpdir itself is created here and removed on
            ``disconnect()``.
    """

    def __init__(self, job_id: str = "", base_dir: str | None = None):
        safe_id = "".join(c for c in job_id if c.isalnum() or c in "-_")[:24]
        self._root_path = Path(
            tempfile.mkdtemp(prefix=f"srw-scratch-{safe_id}-", dir=base_dir)
        )
        self._connected = True
        logger.info("ScratchBackend created at %s (none mode)", self._root_path)

    @property
    def root(self) -> str:
        return str(self._root_path)

    @property
    def root_path(self) -> Path:
        return self._root_path

    @property
    def supports_file_tools(self) -> bool:
        # `none` mode registers no file tools — ScratchBackend is internal-only
        # (no_workspace_agent_mode.md §6). supports_shell stays the ABC default
        # (False), so shell/browser/git are gated off too.
        return False

    def _resolve(self, relative_path: str) -> Path:
        if not relative_path:
            return self._root_path.resolve()

        full_path = (self._root_path / relative_path).resolve()
        workspace_resolved = self._root_path.resolve()

        try:
            full_path.relative_to(workspace_resolved)
        except ValueError:
            raise ValueError(f"Path '{relative_path}' escapes workspace boundary")

        return full_path

    # --- File operations ---

    def read_file(self, path: str, binary: bool = False) -> str | bytes:
        full_path = self._resolve(path)
        if not full_path.exists():
            raise FileNotFoundError(f"File not found: {path}")
        if not full_path.is_file():
            raise ValueError(f"Not a file: {path}")
        if binary:
            return full_path.read_bytes()
        return full_path.read_text(encoding="utf-8")

    def write_file(self, path: str, content: str | bytes) -> None:
        full_path = self._resolve(path)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, bytes):
            full_path.write_bytes(content)
        else:
            full_path.write_text(content, encoding="utf-8")

    def append_file(self, path: str, content: str) -> None:
        full_path = self._resolve(path)
        full_path.parent.mkdir(parents=True, exist_ok=True)
        with open(full_path, "a", encoding="utf-8") as f:
            f.write(content)

    def exists(self, path: str) -> bool:
        return self._resolve(path).exists()

    def is_file(self, path: str) -> bool:
        full_path = self._resolve(path)
        return full_path.exists() and full_path.is_file()

    def is_dir(self, path: str) -> bool:
        full_path = self._resolve(path)
        return full_path.exists() and full_path.is_dir()

    def list_dir(self, path: str = "", pattern: str = "*") -> list[str]:
        dir_path = self._resolve(path)
        if not dir_path.exists():
            return []
        if not dir_path.is_dir():
            return [path]

        results = []
        workspace_resolved = self._root_path.resolve()
        for item in dir_path.glob(pattern):
            rel = item.relative_to(workspace_resolved)
            if item.is_dir():
                results.append(str(rel) + "/")
            else:
                results.append(str(rel))
        return sorted(results)

    def search_files(
        self,
        query: str,
        path: str = "",
        case_sensitive: bool = False,
        exclude_dirs: list[str] | None = None,
    ) -> list[dict]:
        search_path = self._resolve(path)
        results = []
        workspace_resolved = self._root_path.resolve()

        if not case_sensitive:
            query = query.lower()

        if search_path.is_file():
            files_to_search = [search_path]
        else:
            files_to_search = search_path.rglob("*")

        for file_path in files_to_search:
            if not file_path.is_file():
                continue
            if file_path.suffix in [".pdf", ".docx", ".png", ".jpg", ".gif", ".zip"]:
                continue
            if search_excludes(
                file_path.relative_to(search_path).as_posix(), exclude_dirs
            ):
                continue
            try:
                content = file_path.read_text(encoding="utf-8")
                for i, line in enumerate(content.splitlines(), 1):
                    search_line = line if case_sensitive else line.lower()
                    if query in search_line:
                        rel_path = str(file_path.relative_to(workspace_resolved))
                        results.append(
                            {
                                "path": rel_path,
                                "line_number": i,
                                "line": line.strip(),
                            }
                        )
            except (UnicodeDecodeError, IOError):
                continue

        return results

    def mkdir(self, path: str) -> None:
        self._resolve(path).mkdir(parents=True, exist_ok=True)

    def delete_file(self, path: str) -> bool:
        file_path = self._resolve(path)
        if not file_path.exists():
            return False
        if file_path.is_file():
            file_path.unlink()
            return True
        if file_path.is_dir():
            if any(file_path.iterdir()):
                raise ValueError(f"Cannot delete non-empty directory: {path}")
            file_path.rmdir()
            return True
        return False

    def delete_directory(self, path: str) -> bool:
        dir_path = self._resolve(path)
        if dir_path == self._root_path.resolve():
            raise ValueError("Cannot delete workspace root directory")
        if not dir_path.exists():
            return False
        if not dir_path.is_dir():
            raise ValueError(f"Not a directory: {path}")
        shutil.rmtree(dir_path)
        return True

    def move(self, src: str, dst: str) -> None:
        source_path = self._resolve(src)
        dest_path = self._resolve(dst)
        if not source_path.exists():
            raise FileNotFoundError(f"Source not found: {src}")
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source_path), str(dest_path))

    def copy(self, src: str, dst: str) -> None:
        source_path = self._resolve(src)
        dest_path = self._resolve(dst)
        if not source_path.exists():
            raise FileNotFoundError(f"Source not found: {src}")
        if source_path.is_dir():
            raise ValueError(f"Cannot copy directory: {src}. Use move for directories.")
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(source_path), str(dest_path))

    def stat(self, path: str) -> int:
        target_path = self._resolve(path)
        if not target_path.exists():
            return 0
        if target_path.is_file():
            return target_path.stat().st_size
        total = 0
        for file_path in target_path.rglob("*"):
            if file_path.is_file():
                total += file_path.stat().st_size
        return total

    def resolve_path(self, relative_path: str) -> str:
        return str(self._resolve(relative_path))

    # --- Lifecycle ---

    def connect(self) -> None:
        # tmpdir is created in __init__; (re)ensure it exists for idempotency.
        self._root_path.mkdir(parents=True, exist_ok=True)
        self._connected = True

    def disconnect(self) -> None:
        # Scratch state is disposable — remove the tmpdir on teardown.
        try:
            shutil.rmtree(self._root_path, ignore_errors=True)
        finally:
            self._connected = False

    def is_connected(self) -> bool:
        return self._connected
