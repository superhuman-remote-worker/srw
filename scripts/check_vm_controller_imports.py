#!/usr/bin/env python3
"""Import the installed controller, including deferred shared imports, offline.

This build probe runs against the final image's locked dependencies. It never
starts the controller, reads Kubernetes credentials, or creates a client.
"""

import ast
import importlib
from pathlib import Path
import sys


def check_imports():
    import vm_controller

    package = Path(vm_controller.__file__).parent
    pending = {
        f"vm_controller.{path.stem}"
        for path in package.glob("*.py")
        if path.stem not in {"__init__", "__main__"}
    }
    seen = set()
    while pending:
        name = min(pending)
        pending.remove(name)
        if name in seen:
            continue
        if name == "shared.runtime" or name.startswith("shared.runtime."):
            raise RuntimeError("VM controller must not import shared.runtime")
        seen.add(name)
        module = importlib.import_module(name)
        # AST inspection includes imports inside startup and request handlers,
        # which merely importing the entrypoint cannot exercise.
        for node in ast.walk(ast.parse(Path(module.__file__).read_text())):
            if isinstance(node, ast.ImportFrom) and node.module:
                if node.module.startswith("shared."):
                    pending.add(node.module)
            elif isinstance(node, ast.Import):
                pending.update(
                    alias.name for alias in node.names
                    if alias.name.startswith("shared.")
                )
    if "shared.runtime" in sys.modules:
        raise RuntimeError("VM controller imported shared.runtime transitively")
    return seen


if __name__ == "__main__":
    check_imports()
