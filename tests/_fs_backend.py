"""Filesystem-backed WorkspaceBackend for tests only.

Production code must never import this module. The real system uses
RemoteBackend exclusively — the agent process never operates on its own
filesystem as the workspace. This helper exists purely so unit tests can
exercise WorkspaceManager and its dependents against a tmp_path without
standing up an SSH-accessible workspace container.
"""

import shutil
from pathlib import Path

from shared.runtime.core.workspace_backend import WorkspaceBackend, search_excludes


class FilesystemTestBackend(WorkspaceBackend):
    """pathlib-backed WorkspaceBackend for pytest fixtures."""

    def __init__(self, workspace_path: Path):
        self._root_path = Path(workspace_path)

    @property
    def root(self) -> str:
        return str(self._root_path)

    @property
    def root_path(self) -> Path:
        return self._root_path

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
                lines = content.splitlines()

                for i, line in enumerate(lines, 1):
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
        dir_path = self._resolve(path)
        dir_path.mkdir(parents=True, exist_ok=True)

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

    def connect(self) -> None:
        pass

    def disconnect(self) -> None:
        pass

    def is_connected(self) -> bool:
        return True
