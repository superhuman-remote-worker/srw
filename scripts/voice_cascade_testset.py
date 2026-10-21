"""The WP0 German test set as the cascade sees it; local_voice_gateway.md §6.1 (WP3).

Shared by ``voice_cascade_try.py`` and ``voice_cascade_eval.py``: the
fictional hotel's facts as accept ``instructions`` (persona, facts, then
SRW's live rules, read from the orchestrator's source without importing
it), the greeting, the utterances and canned back-office answers, and a
loader for a ``cascade`` profile.
"""

from __future__ import annotations

import ast
import os
import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
TESTSET_DIR = REPO_ROOT / "tests" / "voice_bench" / "testset" / "de"
AUDIO_DIR = REPO_ROOT / "tests" / "voice_bench" / ".cache" / "audio" / "testset"
INSTRUCTIONS_PY = (
    REPO_ROOT / "src" / "orchestrator" / "services" / "live" / "instructions.py"
)

PERSONA = (
    "Du bist die Telefonstimme des Hotel Beispielhof. Du sprichst freundlich, "
    "knapp und in ganzen Sätzen, wie eine gute Rezeptionistin."
)
GREETING = (
    "Guten Tag, hier ist die digitale Assistenz vom Hotel Beispielhof. "
    "Ich bin eine KI. Wie kann ich Ihnen helfen?"
)
#: What the back office answers per delegated utterance (the harness's
#: stand-in for a job's ``message_agent(reply_to=…)``).
CANNED_ANSWERS = {
    "d01-availability": "Vom 12. bis 14. März ist ein Doppelzimmer frei, 129 Euro pro Nacht mit Frühstück.",
    "d02-booking-change": "Die Buchung ist um eine Nacht bis Samstag verlängert, die Bestätigung kommt per E-Mail.",
    "d03-invoice-copy": "Die Rechnung vom letzten Aufenthalt geht heute noch per E-Mail raus.",
    "d07-table": "Am Samstag um 19 Uhr ist ein Tisch für sechs Personen reserviert.",
    "d09-shuttle": "Die Abholung am Donnerstag um 18 Uhr am Bahnhof ist eingetragen, sie kostet 8 Euro.",
    "t03-complaint": "Die Rezeption ruft innerhalb von zehn Minuten zurück.",
}
DEFAULT_ANSWER = "Das ist erledigt; die Bestätigung kommt per E-Mail."


def live_voice_rules() -> str:
    """``LIVE_VOICE_RULES`` from the orchestrator's source (no import: the
    gateway's venv has none of the orchestrator's dependencies)."""
    tree = ast.parse(INSTRUCTIONS_PY.read_text(encoding="utf-8"))
    for node in tree.body:
        if (
            isinstance(node, ast.Assign)
            and any(
                isinstance(t, ast.Name) and t.id == "LIVE_VOICE_RULES"
                for t in node.targets
            )
            and isinstance(node.value, ast.Constant)
        ):
            return node.value.value
    raise RuntimeError("LIVE_VOICE_RULES not found in instructions.py")


def facts() -> str:
    text = (TESTSET_DIR / "facts.md").read_text(encoding="utf-8")
    # Drop the "fictional" note for the voice; it stays in the file.
    return re.sub(r"(?m)^>.*\n", "", text).strip()


def instructions() -> str:
    """As ``live/instructions.py::session_instructions`` builds them."""
    return f"{PERSONA}\n\n## Facts\n{facts()}\n\n{live_voice_rules()}"


def greeting_instruction(greeting: str = GREETING) -> str:
    """As ``live/instructions.py::greeting_instruction`` builds it."""
    return f'Greet the caller now with exactly these words, then listen: "{greeting}"'


def utterances() -> list[dict]:
    data = yaml.safe_load((TESTSET_DIR / "utterances.yaml").read_text(encoding="utf-8"))
    return list(data["utterances"])


def clip(utterance_id: str) -> Path:
    """The phone-quality rendering (8 kHz A-law) from ``voice-bench.sh audio``."""
    path = AUDIO_DIR / f"{utterance_id}.8k-alaw.wav"
    if not path.exists():
        raise SystemExit(f"{path} is missing; run scripts/voice-bench.sh audio first")
    return path


def models_dir() -> Path:
    """Where ``docker/voice-models/models.py fetch`` put the models."""
    return Path(
        os.environ.get("VOICE_MODELS_DIR", REPO_ROOT / ".cache" / "voice-models")
    )


def load_profile(path: Path, name: str) -> dict:
    """A profile mapping from a gateway config (``profiles:``), a file of
    profiles, or a bare profile. Without a ``turn.vad_model_file``, the
    fetched Silero v6.2.3 is used when present."""
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    profiles = raw.get("profiles", raw)
    profile = profiles.get(name) if name in profiles else raw
    if not isinstance(profile, dict) or "stt" not in profile:
        raise SystemExit(f"no profile {name} in {path}")
    profile = dict(profile)
    if profile.pop("engine", "cascade") != "cascade":
        raise SystemExit(f"profile {name} is not a cascade profile")
    turn = dict(profile.get("turn") or {})
    vad = models_dir() / "vad" / "silero_vad-v6.2.3.onnx"
    if "vad_model_file" not in turn and vad.exists():
        turn["vad_model_file"] = str(vad.resolve())
        profile["turn"] = turn
    return profile


def normalise(text: str) -> str:
    return re.sub(r"[^\wäöüß]+", "", text.lower())


def matches(text: str, keywords: list[str]) -> bool:
    """utterances.yaml matching: every keyword, ``a|b`` for either."""
    said = normalise(text)
    return all(
        any(normalise(k) in said for k in group.split("|")) for group in keywords
    )
