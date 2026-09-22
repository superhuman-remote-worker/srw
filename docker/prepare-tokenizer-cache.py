"""Prepare image tokenizer assets; verify final images without network or fallbacks.

Builds run ``prepare`` as root before copying application source, then ``verify``
after USER srw with Docker's RUN --network=none. The installed tiktoken library
owns asset URLs, download hash checks, and model selection. Runtime callers and
their unknown-model fallbacks are not changed by this build-only contract.

``--component`` names the image's application package. The agent image checks
every token counter; the orchestrator image has no agent package, so it checks
the shared ones it does run: KB chunking and the LLM request preflight.

The explicit seven-name set covers the installed library's built-in model map,
including legacy names accepted by SRW's open model-ID configuration. This does
not promise offline assets for third-party tokenizer plugins or future built-ins;
new mapped encodings require review. The cache manifest records only public
tokenizer assets: installed library version, encoding names, sizes, and hashes.
"""

from __future__ import annotations

import argparse
from contextlib import ExitStack, contextmanager
import hashlib
import importlib
from importlib.metadata import version
import json
import os
from pathlib import Path
import re
import sys
from unittest.mock import patch

import tiktoken
import tiktoken.load
import tiktoken.model
import tiktoken.registry


# Configured model IDs are open-ended. Keep every current built-in reachable
# through encoding_for_model, including legacy IDs. Arbitrary plugin encodings
# are outside this asset contract.
ENCODINGS = (
    "cl100k_base",
    "o200k_base",
    "o200k_harmony",
    "p50k_base",
    "p50k_edit",
    "r50k_base",
    "gpt2",
)
MANIFEST = "srw-tokenizer-assets.json"
COMPONENTS = ("agent", "orchestrator")
TEXT = "Guten Tag, Welt! 日本語の文章。\ndef answer(value):\n    return value + 42\n"
# Expected context/shared selections intentionally preserve the existing prefix
# difference. Unknown model IDs still use cl100k_base in all three helpers.
MODEL_CASES = (
    ("gpt-4", "cl100k_base", "cl100k_base"),
    ("gpt-4o", "o200k_base", "o200k_base"),
    ("gpt-5.6-sol", "o200k_base", "o200k_base"),
    ("gpt-5.3-codex", "o200k_base", "o200k_base"),
    ("gpt-oss-120b", "o200k_harmony", "o200k_harmony"),
    ("openai/gpt-4o", "o200k_base", "cl100k_base"),
    ("openai/gpt-oss-120b", "o200k_harmony", "cl100k_base"),
    ("codex/gpt-5.4", "o200k_base", "cl100k_base"),
    ("fixture/unknown-model", "cl100k_base", "cl100k_base"),
    ("text-davinci-003", "p50k_base", "p50k_base"),
    ("text-davinci-edit-001", "p50k_edit", "p50k_edit"),
    ("davinci", "r50k_base", "r50k_base"),
    ("gpt2", "gpt2", "gpt2"),
)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def cache_directory() -> Path:
    configured = os.environ.get("TIKTOKEN_CACHE_DIR", "")
    path = Path(configured)
    require(
        bool(configured) and path.is_absolute(), "Set an absolute TIKTOKEN_CACHE_DIR"
    )
    return path


def check_encoding_contract() -> None:
    mapped = set(tiktoken.model.MODEL_TO_ENCODING.values()) | set(
        tiktoken.model.MODEL_PREFIX_TO_ENCODING.values()
    )
    require(
        mapped <= set(ENCODINGS),
        f"Review new tiktoken model encodings: {sorted(mapped - set(ENCODINGS))}",
    )


def asset_inventory(cache: Path) -> dict:
    assets = {}
    for path in sorted(cache.iterdir()):
        if path.name == MANIFEST:
            continue
        require(
            re.fullmatch(r"[0-9a-f]{40}", path.name) is not None
            and path.is_file()
            and not path.is_symlink(),
            "Unexpected file in managed tokenizer cache",
        )
        contents = path.read_bytes()
        assets[path.name] = {
            "bytes": len(contents),
            "sha256": hashlib.sha256(contents).hexdigest(),
        }
    require(bool(assets), "Tokenizer cache has no assets")
    return assets


def cold_encoding(name: str):
    # In-memory encoders must never make an absent final-image asset pass.
    tiktoken.registry.ENCODINGS.clear()
    encoding = tiktoken.get_encoding(name)
    tokens = encoding.encode(TEXT, disallowed_special=())
    require(
        bool(tokens) and encoding.decode(tokens) == TEXT, f"Round trip failed: {name}"
    )
    return encoding


@contextmanager
def forbid_downloads():
    attempts = []

    def reject_read(_path):
        attempts.append(True)
        raise RuntimeError("Tokenizer tried to download an uncached asset")

    with patch.object(tiktoken.load, "read_file", reject_read):
        try:
            yield
        finally:
            # Some callers catch the exception and return a positive approximate
            # count. Inspect attempts even when the caller appears successful.
            require(not attempts, "Tokenizer tried to download an uncached asset")


def prepare(cache: Path) -> dict:
    check_encoding_contract()
    cache.mkdir(parents=True, exist_ok=True)
    for name in ENCODINGS:
        cold_encoding(name)
    assets = asset_inventory(cache)
    manifest = {
        "schema": 1,
        "tiktoken_version": version("tiktoken"),
        "encodings": list(ENCODINGS),
        "assets": assets,
    }
    (cache / MANIFEST).write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )
    cache.chmod(0o755)
    for path in cache.iterdir():
        path.chmod(0o644)
    return manifest


def verify_assets(cache: Path) -> dict:
    check_encoding_contract()
    manifest = json.loads((cache / MANIFEST).read_text(encoding="utf-8"))
    require(manifest.get("schema") == 1, "Unknown tokenizer manifest schema")
    require(
        manifest.get("tiktoken_version") == version("tiktoken"),
        "Tokenizer version drift",
    )
    require(
        manifest.get("encodings") == list(ENCODINGS),
        "Tokenizer encoding contract drift",
    )
    require(
        manifest.get("assets") == asset_inventory(cache),
        "Tokenizer asset content drift",
    )
    with forbid_downloads():
        for name in ENCODINGS:
            cold_encoding(name)
    require(
        manifest["assets"] == asset_inventory(cache),
        "Offline check changed tokenizer assets",
    )
    return manifest


def verify_helpers(component: str = "agent") -> int:
    from shared.runtime.core import chunk_planner
    from shared.runtime.llm import reasoning_chat

    # The orchestrator image ships no agent package, so no context counter.
    context = None
    if component == "agent":
        from langchain_core.messages import HumanMessage

        from agent.core import context

    require(
        (context is None or context.TIKTOKEN_AVAILABLE)
        and chunk_planner.TIKTOKEN_AVAILABLE
        and reasoning_chat.TIKTOKEN_AVAILABLE,
        "Token counting uses an unavailable-dependency fallback",
    )
    body = {"messages": [{"role": "user", "content": TEXT}]}
    checks = 0
    with ExitStack() as stack:
        stack.enter_context(forbid_downloads())
        if context is not None:
            stack.enter_context(
                patch.object(
                    context,
                    "count_tokens_approximate",
                    side_effect=RuntimeError(
                        "Context counting used its approximate fallback"
                    ),
                )
            )
        for model, context_name, shared_name in MODEL_CASES:
            shared_encoding = cold_encoding(shared_name)
            expected_text = len(shared_encoding.encode(TEXT, disallowed_special=()))
            expected_request = expected_text + len(shared_encoding.encode("user")) + 14

            if context is not None:
                context_encoding = cold_encoding(context_name)
                expected_context = (
                    len(context_encoding.encode(TEXT, disallowed_special=())) + 4
                )
                tiktoken.registry.ENCODINGS.clear()
                actual = context.count_tokens_tiktoken(
                    [HumanMessage(content=TEXT)], model
                )
                require(
                    actual == expected_context, f"Context count changed for {model}"
                )
                checks += 1
            tiktoken.registry.ENCODINGS.clear()
            chunk_planner._ENCODING_CACHE.clear()
            actual = chunk_planner.count_text_tokens(TEXT, model)
            require(actual == expected_text, f"Chunk count changed for {model}")
            tiktoken.registry.ENCODINGS.clear()
            actual = reasoning_chat.count_request_tokens(body, model)
            require(actual == expected_request, f"Request count changed for {model}")
            checks += 2

        expected = len(cold_encoding("cl100k_base").encode(TEXT, disallowed_special=()))
        tiktoken.registry.ENCODINGS.clear()
        chunk_planner._ENCODING_CACHE.clear()
        require(
            chunk_planner.count_text_tokens(TEXT) == expected,
            "Default chunk count changed",
        )
    return checks + 1


def verify(cache: Path, component: str = "agent") -> dict:
    require(component in COMPONENTS, f"Unknown image component: {component}")
    require(
        os.getuid() != 0, "Run offline verification as the final non-root image user"
    )
    require(bool(sys.flags.safe_path), "Offline verification requires PYTHONSAFEPATH")
    require(
        not os.environ.get("PYTHONPATH"),
        "Offline verification must use installed packages",
    )
    package = importlib.import_module(component)

    require(
        Path(package.__file__).resolve() == Path(f"/app/src/{component}/__init__.py"),
        f"{component.capitalize()} import does not resolve to the image's "
        "canonical source package",
    )
    require(
        cache.stat().st_uid == 0 and cache.stat().st_mode & 0o777 == 0o755,
        "Cache directory permissions changed",
    )
    for path in cache.iterdir():
        require(
            path.stat().st_uid == 0 and path.stat().st_mode & 0o777 == 0o644,
            "Cache file permissions changed",
        )
    manifest = verify_assets(cache)
    checks = verify_helpers(component)
    require(
        manifest["assets"] == asset_inventory(cache),
        "Helper check changed tokenizer assets",
    )
    return {
        "tiktoken_version": manifest["tiktoken_version"],
        "encodings": manifest["encodings"],
        "asset_files": len(manifest["assets"]),
        "helper_checks": checks,
        "uid": os.getuid(),
        "offline": True,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "verify"))
    parser.add_argument("--component", choices=COMPONENTS, default="agent")
    args = parser.parse_args()
    cache = cache_directory()
    result = prepare(cache) if args.mode == "prepare" else verify(cache, args.component)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
