"""A run's summary as JSON and Markdown, the stored baseline, a diff against
it, and several runs or profiles side by side.

A run directory holds ``run.json`` (what ran), ``stt.json``, ``file.json``
and ``call.json`` (what was measured, each with its own ``summary``);
``summarise`` folds them into ``summary.json`` and ``summary.md``. A baseline
is a ``summary.json`` kept under ``baselines/<profile>.json``.
"""

from __future__ import annotations

import json
from pathlib import Path

from tests.voice_bench.evalset.spec import EVALSET_DIR

BASELINE_DIR = EVALSET_DIR / "baselines"
LEVELS = ("stt", "file", "call")

#: Below these a category is a weak spot in the summary.
WEAK_EXACT = 0.8
WEAK_DECISION = 0.8
WEAK_WER = 0.15

#: Keys whose growth is bad; everything with "rate"/"ok"/"perfect"/"pass" is
#: good when it grows; other counts are shown, not judged.
_LOWER_BETTER = (
    "wer",
    "_ms",
    "cut_off",
    "missed",
    "phantoms",
    "phantom",
    "taken",
    "false_barge_in",
    "delegated_early",
    "wording",
    "faults",
    "names_back_office",
    "promises_handover",
    "two_holding_sentences",
    "holds_again",
    "silent",
)
_HIGHER_BETTER = ("rate", "_ok", "perfect", "pass")


def load_json(path: Path) -> dict | None:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def summarise(run_dir: Path) -> dict:
    """``summary.json`` of a run directory (and ``summary.md`` beside it)."""
    meta = load_json(run_dir / "run.json") or {}
    summary: dict = {"meta": meta}
    for level in LEVELS:
        data = load_json(run_dir / f"{level}.json")
        if data is None:
            continue
        if level == "stt" and data.get("items"):
            from tests.voice_bench.evalset.scoring import aggregate_stt

            summary[level] = aggregate_stt(data["items"])
        else:
            summary[level] = data.get("summary", {})
    (run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    (run_dir / "summary.md").write_text(markdown(summary), encoding="utf-8")
    return summary


# -- Markdown ------------------------------------------------------------------------


def _fmt(value, digits: int = 2) -> str:
    if value is None:
        return "–"
    if isinstance(value, float):
        if (value.is_integer() and abs(value) >= 2) or abs(value) >= 100:
            return f"{value:.0f}"
        return f"{value:.{digits}f}"
    return str(value)


def _table(header: list[str], rows: list[list]) -> list[str]:
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    lines += ["| " + " | ".join(_fmt(c) for c in row) + " |" for row in rows]
    return lines + [""]


_CONDITION_ORDER = (
    "clean",
    "g711a",
    "g711u",
    "g722",
    "mobile",
    "loss-3",
    "loss-8",
    "street",
    "cafe",
    "office",
    "quiet",
    "clipped",
)


def _condition_order(name: str) -> tuple[int, str]:
    order = _CONDITION_ORDER.index(name) if name in _CONDITION_ORDER else 99
    return (order, name)


def _cell(row: dict | None) -> str:
    if not row:
        return ""
    if row.get("exact_rate") is not None:
        return _fmt(row["exact_rate"])
    if "phantoms" in row:
        return f"{row['phantoms']} with text"
    return f"WER {_fmt(row.get('wer_mean'))}" if row.get("wer_mean") is not None else ""


def weak_spots(summary: dict) -> list[str]:
    spots = []
    stt = summary.get("stt") or {}
    for cat, row in (stt.get("by_category") or {}).items():
        if row.get("exact_rate") is not None and row["exact_rate"] < WEAK_EXACT:
            spots.append(
                f"STT {cat}: {row['values_ok']}/{row['values']} values exact "
                f"(WER {_fmt(row.get('wer_mean'))})"
            )
        elif row.get("wer_mean") is not None and row["wer_mean"] > WEAK_WER:
            spots.append(f"STT {cat}: WER {_fmt(row['wer_mean'])}")
        if row.get("phantoms"):
            spots.append(
                f"STT {cat}: {row['phantoms']}/{row['silent_clips']} silent clips got text"
            )
    for cond, row in (stt.get("by_condition") or {}).items():
        if row.get("exact_rate") is not None and row["exact_rate"] < WEAK_EXACT:
            spots.append(
                f"STT under {cond}: {row['values_ok']}/{row['values']} values exact, "
                f"WER {_fmt(row.get('wer_mean'))}"
            )
    for level in ("file", "call"):
        data = summary.get(level) or {}
        for cat, row in (data.get("by_category") or {}).items():
            if (
                row.get("decision_rate") is not None
                and row["decision_rate"] < WEAK_DECISION
            ):
                spots.append(
                    f"{level} {cat}: decision right in {row['decisions_ok']}/{row['decisions']}"
                )
            if row.get("exact_rate") is not None and row["exact_rate"] < WEAK_EXACT:
                spots.append(
                    f"{level} {cat}: {row['values_ok']}/{row['values']} values reached the voice or back office"
                )
        taking = data.get("turn_taking") or {}
        for key in ("cut_off", "delegated_early", "missed", "false_barge_in"):
            if taking.get(key):
                spots.append(
                    f"{level} turn taking: {key.replace('_', ' ')} {taking[key]}x"
                )
        if taking.get("backchannel_taken"):
            spots.append(
                f"{level} turn taking: backchannel taken as a turn "
                f"{taking['backchannel_taken']}/{taking['backchannel_turns']}"
            )
        if (
            taking.get("barge_in_turns")
            and taking.get("barge_in_ok", 0) < taking["barge_in_turns"]
        ):
            spots.append(
                f"{level} turn taking: barge-in honoured {taking.get('barge_in_ok', 0)}/{taking['barge_in_turns']}"
            )
        if data.get("phantom_transcripts"):
            spots.append(
                f"{level}: {data['phantom_transcripts']} caller transcripts without caller speech"
            )
        if data.get("wording"):
            faults = ", ".join(f"{k} {v}" for k, v in data["wording"].items())
            spots.append(f"{level} wording: {faults}")
    return spots


def markdown(summary: dict) -> str:
    meta = summary.get("meta") or {}
    lines = [
        f"# Voice eval: {meta.get('profile', '?')}",
        "",
        f"- when: {meta.get('started', '?')}; source: {meta.get('git', '?')}",
        f"- levels: {', '.join(meta.get('levels', [])) or '?'}",
    ]
    for note in meta.get("notes", []):
        lines.append(f"- {note}")
    lines.append("")
    spots = weak_spots(summary)
    lines += ["## Weak spots", ""]
    lines += [f"- {s}" for s in spots] or ["- none found by the thresholds"]
    lines.append("")

    stt = summary.get("stt")
    if stt:
        o = stt["overall"]
        lines += [
            "## STT (file level, every clip under every condition)",
            "",
            f"{o['clips']} clips; WER mean {_fmt(o['wer_mean'])}, median {_fmt(o['wer_median'])}; "
            f"values exact {o['values_ok']}/{o['values']} ({_fmt(o['exact_rate'])}); "
            f"silent clips with text {o.get('phantoms', 0)}/{o.get('silent_clips', 0)}.",
            "",
        ]
        for title, key in (
            ("By category", "by_category"),
            ("By condition", "by_condition"),
            ("By voice", "by_voice"),
            ("By style", "by_style"),
        ):
            rows = [
                [
                    name,
                    r["clips"],
                    r["wer_mean"],
                    r["perfect"],
                    f"{r['values_ok']}/{r['values']}",
                    r["exact_rate"],
                ]
                for name, r in stt[key].items()
            ]
            lines += [f"### {title}", ""]
            lines += _table(
                [
                    title.split()[-1],
                    "clips",
                    "WER",
                    "word for word",
                    "values exact",
                    "rate",
                ],
                rows,
            )
        rows = [
            [k, f"{r['ok']}/{r['n']}", r["rate"]]
            for k, r in stt["by_value_kind"].items()
        ]
        lines += ["### Values by kind", ""] + _table(["kind", "exact", "rate"], rows)
        matrix = stt.get("by_category_condition") or {}
        conds = sorted(
            {c for row in matrix.values() for c in row}, key=_condition_order
        )
        if matrix:
            rows = [
                [cat] + [_cell(matrix[cat].get(c)) for c in conds] for cat in matrix
            ]
            lines += [
                "### Category by condition (values exact; WER where a category has no values)",
                "",
            ] + _table(["category", *conds], rows)

    for level, title in (
        ("file", "file level (cascade harness)"),
        ("call", "call level (bench PBX)"),
    ):
        data = summary.get(level)
        if not data:
            continue
        o = data["overall"]
        lines += [
            f"## Scenarios, {title}",
            "",
            f"{o['scenarios_ok']}/{o['scenarios']} scenarios and {o['turns_ok']}/{o['turns']} turns "
            f"fully right; values {o['values_ok']}/{o['values']}.",
            "",
        ]
        rows = [
            [
                cat,
                r.get("turns"),
                f"{r['decisions_ok']}/{r['decisions']}" if r.get("decisions") else "–",
                f"{r['values_ok']}/{r['values']}" if r.get("values") else "–",
            ]
            for cat, r in data["by_category"].items()
        ]
        lines += ["### By category", ""] + _table(
            ["category", "turns", "decision right", "values reached"], rows
        )
        rows = [[k, f"{r['ok']}/{r['n']}"] for k, r in data["values_by_where"].items()]
        lines += ["### Values by place", ""] + _table(["where", "found"], rows)
        taking = data.get("turn_taking") or {}
        lines += [
            "### Turn taking",
            "",
            f"caller cut off {taking.get('cut_off', 0)}, delegated before the caller finished "
            f"{taking.get('delegated_early', 0)}, turn missed {taking.get('missed', 0)}, "
            f"backchannel taken {taking.get('backchannel_taken', 0)}/{taking.get('backchannel_turns', 0)}, "
            f"barge-in honoured {taking.get('barge_in_ok', 0)}/{taking.get('barge_in_turns', 0)}, "
            f"false barge-in {taking.get('false_barge_in', 0) if level == 'file' else 'not observable'}, "
            f"caller transcripts without speech {data.get('phantom_transcripts', 0)}; "
            f"lines meant for over the voice that came while it was silent (not scored) "
            f"{taking.get('not_over_voice', 0)}.",
            "",
        ]
        timings = data.get("timings") or {}
        if timings:
            rows = [
                [
                    label,
                    t["turns"],
                    t["turn_wait_ms"],
                    t["stt_ms"],
                    t["llm_first_token_ms"],
                    t["llm_first_text_ms"],
                    t["tts_first_audio_ms"],
                    t["first_audio_ms"],
                    t["first_audio_ms_p90"],
                ]
                for label, t in timings.items()
            ]
            lines += ["### Latency (engine, medians, ms)", ""]
            lines += _table(
                [
                    "turns",
                    "n",
                    "turn wait",
                    "STT",
                    "LLM 1st token",
                    "LLM to TTS text",
                    "TTS 1st audio",
                    "end of speech to audio",
                    "p90",
                ],
                rows,
            )
        fa = data.get("first_audio_ms") or {}
        if fa.get("n"):
            lines += [
                f"At the caller: end of speech to the voice's first audio, median {_fmt(fa['median'])} ms, "
                f"p90 {_fmt(fa['p90'])} ms over {fa['n']} turns; longest silence while the back office "
                f"worked {_fmt((data.get('wait_silence_ms') or {}).get('max'))} ms.",
                "",
            ]
        wording = data.get("wording") or {}
        lines += [
            "### Wording",
            "",
            (", ".join(f"{k} {v}" for k, v in wording.items()) or "no faults") + ".",
            "",
        ]
        intel = data.get("intelligibility") or {}
        if intel:
            lines += [
                "### What the caller heard",
                "",
                f"{intel.get('judge', '?')}: WER mean {_fmt(intel.get('wer_mean'))} over "
                f"{intel.get('lines', 0)} voice utterances; word for word {intel.get('perfect', 0)}.",
                "",
            ]
        rows = [
            [
                sid,
                "pass" if r["pass"] else "FAIL",
                f"{r['turns_ok']}/{r['turns']}",
                "; ".join(r["failures"][:4]),
            ]
            for sid, r in data["by_scenario"].items()
        ]
        lines += ["### Per scenario", ""] + _table(
            ["scenario", "", "turns", "what failed"], rows
        )
    return "\n".join(lines).rstrip() + "\n"


# -- baseline, diff, compare ---------------------------------------------------------


def flatten(data, prefix: str = "") -> dict[str, float]:
    out: dict[str, float] = {}
    if isinstance(data, dict):
        for key, value in data.items():
            if key in ("meta", "failures"):
                continue
            out.update(flatten(value, f"{prefix}.{key}" if prefix else str(key)))
    elif isinstance(data, bool):
        out[prefix] = float(data)
    elif isinstance(data, int | float):
        out[prefix] = float(data)
    return out


def direction(path: str) -> int:
    """+1 when bigger is better, -1 when smaller is better, 0 when neutral."""
    leaf = path.rsplit(".", 1)[-1]
    if any(k in leaf for k in _HIGHER_BETTER) and "taken" not in leaf:
        return 1
    if any(k in leaf for k in _LOWER_BETTER) or any(k in path for k in (".wording.",)):
        return -1
    return 0


def _tolerance(path: str, old: float) -> float:
    leaf = path.rsplit(".", 1)[-1]
    if "_ms" in leaf:
        return max(50.0, abs(old) * 0.1)
    if "wer" in leaf or "rate" in leaf:
        return 0.02
    return 0.0


def diff(current: dict, baseline: dict) -> list[dict]:
    """Metrics that moved beyond their tolerance, worst first."""
    now, before = flatten(current), flatten(baseline)
    rows = []
    for path in sorted(set(now) | set(before)):
        old, new = before.get(path), now.get(path)
        if old is None or new is None:
            if old != new:
                rows.append(
                    {
                        "metric": path,
                        "baseline": old,
                        "now": new,
                        "verdict": "new" if old is None else "gone",
                    }
                )
            continue
        if abs(new - old) <= _tolerance(path, old):
            continue
        sign = direction(path)
        verdict = (
            "changed"
            if sign == 0
            else ("better" if (new - old) * sign > 0 else "worse")
        )
        rows.append({"metric": path, "baseline": old, "now": new, "verdict": verdict})
    order = {"worse": 0, "better": 1, "changed": 2, "new": 3, "gone": 4}
    return sorted(rows, key=lambda r: (order[r["verdict"]], r["metric"]))


def diff_markdown(rows: list[dict], *, limit: int = 80) -> str:
    if not rows:
        return "No metric moved beyond its tolerance.\n"
    counts = {
        v: sum(r["verdict"] == v for r in rows)
        for v in ("worse", "better", "changed", "new", "gone")
    }
    head = ", ".join(f"{k} {v}" for k, v in counts.items() if v)
    lines = [f"Against the baseline: {head}.", ""]
    lines += _table(
        ["metric", "baseline", "now", ""],
        [[r["metric"], r["baseline"], r["now"], r["verdict"]] for r in rows[:limit]],
    )
    if len(rows) > limit:
        lines.append(f"… and {len(rows) - limit} more (diff.json has them all).")
    return "\n".join(lines).rstrip() + "\n"


#: The metrics ``compare`` puts side by side.
COMPARE_KEYS = (
    "stt.overall.wer_mean",
    "stt.overall.exact_rate",
    "stt.by_value_kind.name.rate",
    "stt.by_value_kind.phone.rate",
    "stt.by_value_kind.date.rate",
    "stt.by_value_kind.time.rate",
    "stt.by_value_kind.email.rate",
    "stt.by_value_kind.address.rate",
    "stt.by_value_kind.number.rate",
    "stt.by_condition.clean.wer_mean",
    "stt.by_condition.g711a.wer_mean",
    "stt.by_condition.mobile.wer_mean",
    "stt.by_condition.cafe.wer_mean",
    "stt.by_condition.loss-8.wer_mean",
    "stt.overall.phantoms",
    "{level}.overall.turns_ok",
    "{level}.overall.values_ok",
    "{level}.decisions.answer.rate",
    "{level}.decisions.delegate.rate",
    "{level}.decisions.transfer.rate",
    "{level}.decisions.message.rate",
    "{level}.decisions.ignore.rate",
    "{level}.values_by_where.request.rate",
    "{level}.values_by_where.reply.rate",
    "{level}.turn_taking.cut_off",
    "{level}.turn_taking.missed",
    "{level}.turn_taking.backchannel_taken",
    "{level}.turn_taking.barge_in_ok",
    "{level}.turn_taking.false_barge_in",
    "{level}.timings.answered.first_audio_ms",
    "{level}.timings.delegated.first_audio_ms",
    "{level}.timings.all.stt_ms",
    "{level}.timings.all.llm_first_token_ms",
    "{level}.timings.all.tts_first_audio_ms",
    "{level}.first_audio_ms.median",
    "{level}.wait_silence_ms.max",
    "{level}.intelligibility.wer_mean",
)


def compare(summaries: dict[str, dict]) -> str:
    """Several summaries (runs, baselines, profiles) side by side."""
    flat = {name: flatten(s) for name, s in summaries.items()}
    keys = []
    for key in COMPARE_KEYS:
        keys += (
            [key.format(level=lvl) for lvl in ("file", "call")]
            if "{level}" in key
            else [key]
        )
    rows = []
    for key in keys:
        cells = [flat[name].get(key) for name in summaries]
        if all(c is None for c in cells):
            continue
        rows.append([key, *cells])
    return "\n".join(_table(["metric", *summaries], rows)).rstrip() + "\n"


def baseline_path(profile: str) -> Path:
    return BASELINE_DIR / f"{profile}.json"
