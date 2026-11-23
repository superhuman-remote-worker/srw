"""Every file that names a dispatch ``{PREFIX}_BASE_URL`` also names its pin.

A new call site that reads or writes ``EMBEDDING_BASE_URL`` (or a sibling) but
forgets ``{PREFIX}_TLS_PINNED_CERT`` would talk to a pinned endpoint without its
pin. This guard fails the build for such a file.
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

PREFIXES = (
    "EMBEDDING",
    "KB_EMBEDDING",
    "RERANK",
    "VISION",
    "WHISPER",
    "TTS",
    "CITATION_LLM",
)
# Files that derive the pin name from prefix + suffix instead of spelling it.
BUILDS_NAMES = {
    "src/shared/runtime/core/transport_resolution.py",  # _DISPATCH_ENV_SUFFIXES
    "src/orchestrator/services/dispatch_credentials.py",  # f"{prefix}_TLS_PINNED_CERT"
    "src/orchestrator/services/job_dispatch_credentials.py",
}


def test_every_file_naming_a_dispatch_base_url_names_its_pin():
    for path in (ROOT / "src").rglob("*.py"):
        rel, text = path.relative_to(ROOT).as_posix(), path.read_text(encoding="utf-8")
        for p in PREFIXES:
            if f'"{p}_BASE_URL"' not in text:
                continue
            if rel in BUILDS_NAMES:
                assert "TLS_PINNED_CERT" in text, rel
            else:
                assert (
                    f'"{p}_TLS_PINNED_CERT"' in text or f'TlsPin.from_env("{p}"' in text
                ), f"{rel} names {p}_BASE_URL but not its pin"
