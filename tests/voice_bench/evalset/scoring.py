"""Scoring a run: STT per clip, each scenario turn, and the totals.

The runner (``scripts/voice_eval.py``) records what happened (transcripts,
the voice's words, delegations, flushes, times); this module decides what
that means against the test set's expectations, without audio or servers:

- **STT** per clip: word error rate (the bench's own ``wer``, passed in) and
  whether each value survived exactly; a silent clip must give no text.
- **Turns**: the voice's decision (answered / delegated / stayed quiet) against
  the expected outcome; values in the reply, the delegated request or STT;
  turn taking (caller cut off, delegated before the caller finished, turn
  missed, backchannel taken as a turn, barge-in honoured, false barge-in);
  the voice's wording rules (``voice_cascade_eval.py``'s checks, passed in).
- **Totals** per category, condition, voice and scenario.

Every time is in milliseconds on the call's clock (0 = the call answered).
"""

from __future__ import annotations

import datetime as dt
import statistics
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable

from tests.voice_bench.evalset import values as V
from tests.voice_bench.evalset.spec import (
    CATEGORIES,
    Expect,
    Scenario,
    Turn,
    Utterance,
    Value,
    has_pause,
)

#: How long after the caller stopped the voice must react before a turn
#: counts as missed.
MISSED_AFTER_MS = 8_000
#: A flush this soon after the caller started talking over the voice is the
#: barge-in working.
BARGE_IN_WITHIN_MS = 2_000
#: A caller transcript this long after the caller's last words is still theirs
#: (VAD stop, turn wait and STT come first).
TRANSCRIPT_LAG_MS = 4_000

#: Expected outcome → the category its decision counts towards.
DECISION_CATEGORY = {
    "answer": "facts",
    "delegate": "back_office",
    "transfer": "transfer",
    "message": "messages",
}


def _mean(values: Iterable[float]) -> float | None:
    items = [v for v in values if v is not None]
    return round(statistics.mean(items), 3) if items else None


def _median(values: Iterable[float]) -> float | None:
    items = [v for v in values if v is not None]
    return round(statistics.median(items), 1) if items else None


def _p90(values: Iterable[float]) -> float | None:
    items = sorted(v for v in values if v is not None)
    if not items:
        return None
    return round(items[min(len(items) - 1, int(round(0.9 * (len(items) - 1))))], 1)


def _rate(ok: int, n: int) -> float | None:
    return round(ok / n, 3) if n else None


# -- STT --------------------------------------------------------------------------


def score_stt(
    utt: Utterance,
    heard: str,
    *,
    wer: Callable[[str, str], float],
    today: dt.date | None = None,
    values: Iterable[Value] | None = None,
) -> dict:
    """One clip: what STT wrote against what the caller said."""
    if utt.silence:
        return {
            "wer": None,
            "phantom": bool(heard.strip()),
            "values": [],
        }
    reference = V.strip_fillers(utt.reference)
    hypothesis = V.strip_fillers(heard)
    checks = []
    for value in utt.values if values is None else values:
        found = V.match(value, heard, today)
        checks.append(
            {
                "value": value.label(),
                "kind": value.kind,
                "category": value.category,
                "ok": found.ok,
                "how": found.how,
            }
        )
    return {
        "wer": round(wer(reference, hypothesis), 3) if reference else None,
        "phantom": False,
        "values": checks,
    }


def _stt_group(items: list[dict]) -> dict:
    spoken = [i for i in items if not i.get("silence")]
    silent = [i for i in items if i.get("silence")]
    checks = [c for i in spoken for c in i["values"]]
    out = {
        "clips": len(items),
        "wer_mean": _mean(i["wer"] for i in spoken),
        "wer_median": _median(i["wer"] for i in spoken),
        "perfect": sum(1 for i in spoken if i["wer"] == 0),
        "values": len(checks),
        "values_ok": sum(c["ok"] for c in checks),
        "exact_rate": _rate(sum(c["ok"] for c in checks), len(checks)),
    }
    if silent:
        out["silent_clips"] = len(silent)
        out["phantoms"] = sum(i["phantom"] for i in silent)
    return out


def aggregate_stt(items: list[dict]) -> dict:
    """Totals of the STT pass, by category, condition, voice, style and value kind."""

    def grouped(key) -> dict:
        groups: dict[str, list[dict]] = defaultdict(list)
        for item in items:
            for name in key(item):
                groups[name].append(item)
        return {name: _stt_group(group) for name, group in sorted(groups.items())}

    kinds: dict[str, Counter] = defaultdict(Counter)
    for item in items:
        for check in item["values"]:
            kinds[check["kind"]]["n"] += 1
            kinds[check["kind"]]["ok"] += check["ok"]
    matrix: dict[str, dict[str, list[dict]]] = defaultdict(lambda: defaultdict(list))
    for item in items:
        matrix[item["category"]][item["condition"]].append(item)
    return {
        "overall": _stt_group(items),
        "by_category_condition": {
            cat: {
                cond: {
                    k: v
                    for k, v in _stt_group(group).items()
                    if k in ("wer_mean", "exact_rate", "phantoms")
                }
                for cond, group in sorted(conds.items())
            }
            for cat, conds in sorted(matrix.items())
        },
        "by_category": grouped(lambda i: [i["category"]]),
        "by_condition": grouped(lambda i: [i["condition"]]),
        "by_voice": grouped(lambda i: [i["voice"]]),
        "by_style": grouped(lambda i: i.get("styles") or ["plain"]),
        "by_value_kind": {
            k: {"n": c["n"], "ok": c["ok"], "rate": _rate(c["ok"], c["n"])}
            for k, c in sorted(kinds.items())
        },
    }


# -- one scenario turn -----------------------------------------------------------------


def where_for(value: Value, expect: Expect) -> str:
    if value.where:
        return value.where
    if expect.outcome in ("delegate", "transfer", "message"):
        return "request"
    if expect.outcome == "answer":
        return "reply"
    return "stt"


def _texts(obs: dict) -> dict[str, list[tuple[str, str]]]:
    """The texts a value may be looked for in, by place: (source, text)."""
    requests = [
        ("transcript", d.get("request") or "") for d in obs.get("delegations", [])
    ]
    requests += [
        ("llm", d.get("request_llm") or "")
        for d in obs.get("delegations", [])
        if d.get("request_llm")
    ]
    reply = [("reply", obs.get("reply") or "")]
    return {
        "stt": [("stt", obs.get("stt") or "")],
        "reply": reply,
        "request": requests,
        "either": requests + reply,
    }


def check_value(
    value: Value,
    expect: Expect,
    obs: dict,
    today: dt.date | None,
    judge: Callable[[Value, str, str], dict] | None = None,
) -> dict:
    where = where_for(value, expect)
    found_in = []
    how = ""
    texts = _texts(obs)[where]
    for source, text in texts:
        match = V.match(value, text, today)
        if match.ok:
            found_in.append(source)
            how = how or match.how
    result = {
        "value": value.label(),
        "kind": value.kind,
        "category": value.category,
        "where": where,
        "ok": bool(found_in),
        "found_in": found_in,
        "how": how,
    }
    # The LLM judge only where exact rules cannot decide (``meaning``).
    if not found_in and value.kind == "meaning" and judge is not None:
        joined = "\n".join(t for _, t in texts if t)
        verdict = judge(value, joined, where)
        result.update(ok=bool(verdict.get("pass")), how="llm_judge", judge=verdict)
    return result


def got_outcome(obs: dict) -> str:
    if obs.get("delegations"):
        return "delegate"
    if (obs.get("reply") or "").strip():
        return "answer"
    return "silent"


def score_turn(
    turn: Turn,
    obs: dict,
    *,
    today: dt.date | None = None,
    wording: Callable[[str, bool], list[str]] | None = None,
    relay_wording: Callable[[str], list[str]] | None = None,
    judge: Callable[[Value, str, str], dict] | None = None,
) -> dict:
    """One turn's verdict from what was observed in its window.

    ``obs`` keys (absent = not observed at this level): ``caller_start_ms``,
    ``caller_end_ms`` (end of the caller's speech), ``window_end_ms``,
    ``stt`` (caller transcripts in the window), ``stt_events`` (their times),
    ``reply`` (the voice's words in the window), ``holding`` (its words before
    the back office answered), ``relay`` (after), ``delegations``
    (``at_ms``, ``request``, ``request_llm``, ``answered_at_ms``),
    ``voice_starts``, ``voice_sentences`` (when the voice began each
    sentence), ``flushes``, ``voice_was_talking``, ``first_audio_ms``.
    """
    expect = turn.expect
    got = got_outcome(obs)
    start = obs.get("caller_start_ms")
    end = obs.get("caller_end_ms")
    delegations = obs.get("delegations", [])
    flushes = obs.get("flushes")
    stt_events = obs.get("stt_events", [])
    verdict: dict = {"outcome_got": got}

    # A line meant to be said over the voice that came while the voice was
    # silent (the call level's fixed schedule met a slow reply) tests
    # nothing about backchannels or barge-in: it is not scored as such.
    over_voice = (
        turn.when != "during_voice" or obs.get("voice_was_talking") is not False
    )
    if not over_voice:
        verdict["not_over_voice"] = True

    # -- the decision
    if not over_voice and (expect.outcome == "ignore" or expect.barge_in):
        verdict["outcome_got"] = "not over the voice"
        verdict["outcome_ok"] = True
    elif expect.outcome == "ignore":
        taken = bool(stt_events) or bool(delegations) or bool(flushes)
        if turn.silence:
            # Only the line: any transcript or reply is a phantom turn.
            taken = taken or bool(
                (obs.get("reply") or "").strip() and not obs.get("voice_was_talking")
            )
        verdict["outcome_got"] = "taken" if taken else "ignored"
        verdict["outcome_ok"] = not taken
        if turn.when == "during_voice":
            verdict["backchannel_taken"] = taken
    elif expect.outcome == "any":
        verdict["outcome_ok"] = True
    elif expect.outcome == "answer":
        verdict["outcome_ok"] = got == "answer"
    else:
        verdict["outcome_ok"] = got == "delegate"

    # -- values
    verdict["values"] = [
        check_value(v, expect, obs, today, judge) for v in expect.values
    ]
    absent = []
    for value in expect.absent:
        hit = check_value(value, expect, obs, today)
        absent.append({**hit, "ok": not hit["ok"]})
    verdict["absent"] = absent

    # -- turn taking
    starts = obs.get("voice_starts") or []
    if start is not None and end is not None:
        # The voice began a new utterance (audio, or a sentence it started
        # to say) while the caller was still talking.
        spoken = starts + (obs.get("voice_sentences") or [])
        cut = [s for s in spoken if start + 300 < s < end - 200]
        # Only when the voice was silent as the caller began: a caller who
        # starts over the voice is not cut off by it.
        verdict["cut_off"] = (
            bool(cut)
            and turn.when != "during_voice"
            and expect.outcome != "ignore"
            and obs.get("voice_was_talking") is not True
        )
        verdict["delegated_early"] = any(
            d.get("at_ms") is not None and d["at_ms"] < end - 200 for d in delegations
        )
        if expect.outcome in ("answer", "delegate", "transfer", "message"):
            reacted = [s for s in starts if s >= end - 200] + [
                d["at_ms"] for d in delegations if d.get("at_ms") is not None
            ]
            verdict["missed"] = not any(r <= end + MISSED_AFTER_MS for r in reacted)
        if expect.barge_in and over_voice:
            if flushes is not None:
                verdict["barge_in_ok"] = any(
                    start <= f <= start + BARGE_IN_WITHIN_MS for f in flushes
                )
            elif obs.get("voice_stopped_ms") is not None:
                verdict["barge_in_ok"] = (
                    obs["voice_stopped_ms"] <= start + BARGE_IN_WITHIN_MS
                )
    if flushes is not None and not expect.barge_in and expect.outcome != "ignore":
        # (A flush during a backchannel counts as the backchannel taken.)
        verdict["false_barge_in"] = len(flushes)
    verdict["first_audio_ms"] = obs.get("first_audio_ms")
    if obs.get("wait_silence_ms") is not None:
        verdict["wait_silence_ms"] = obs["wait_silence_ms"]

    # -- wording
    faults: list[str] = []
    if wording is not None and expect.outcome != "ignore":
        if delegations:
            faults += wording(obs.get("holding") or "", True)
            if relay_wording is not None and any(
                d.get("answered_at_ms") for d in delegations
            ):
                faults += ["relay_" + f for f in relay_wording(obs.get("relay") or "")]
        elif (obs.get("reply") or "").strip():
            faults += wording(obs.get("reply") or "", False)
    verdict["wording"] = faults

    verdict["ok"] = turn_ok(verdict)
    return verdict


def turn_ok(verdict: dict) -> bool:
    return bool(
        verdict.get("outcome_ok")
        and all(v["ok"] for v in verdict.get("values", []))
        and all(v["ok"] for v in verdict.get("absent", []))
        and not verdict.get("cut_off")
        and not verdict.get("missed")
        and verdict.get("barge_in_ok", True) is not False
        and not verdict.get("false_barge_in")
    )


def reply_windows(turns: list[Turn], starts: list[int], end: int) -> list[int]:
    """Where each turn's reply window ends: at the next caller line, except
    that a backchannel said over the voice (``during_voice`` + ``ignore``)
    does not end the reply it was said over."""
    out = []
    for i in range(len(turns)):
        j = i + 1
        while (
            j < len(turns)
            and turns[j].when == "during_voice"
            and turns[j].expect.outcome == "ignore"
        ):
            j += 1
        out.append(starts[j] if j < len(turns) else end)
    return out


def phantom_transcripts(
    transcripts: list[tuple[int, str]], speech: list[tuple[int, int]]
) -> list[tuple[int, str]]:
    """Caller transcripts that no caller speech explains (noise taken as words)."""
    return [
        (at, text)
        for at, text in transcripts
        if not any(a <= at <= b + TRANSCRIPT_LAG_MS for a, b in speech)
    ]


# -- totals over the scenarios ------------------------------------------------------------


def turn_categories(turn: Turn, scenario: Scenario) -> list[str]:
    cats = []
    if turn.expect.outcome in DECISION_CATEGORY:
        cats.append(DECISION_CATEGORY[turn.expect.outcome])
    if turn.expect.outcome == "ignore":
        cats.append("silence" if turn.silence else "backchannels")
    if turn.expect.barge_in:
        cats.append("barge_in")
    if turn.text and has_pause(turn.text):
        cats.append("pauses")
    if turn.text and len(turn.text.split()) <= 3 and turn.expect.outcome != "ignore":
        cats.append("short_answers")
    return cats


def aggregate_turns(results: list[dict], scenarios: dict[str, Scenario]) -> dict:
    """Totals over every scored turn (``results``: dicts with ``scenario``,
    ``turn`` index and ``verdict``)."""
    by_cat: dict[str, Counter] = defaultdict(Counter)
    decisions: dict[str, Counter] = defaultdict(Counter)
    where: dict[str, Counter] = defaultdict(Counter)
    kinds: dict[str, Counter] = defaultdict(Counter)
    taking = Counter()
    wording = Counter()
    first_audio = []
    waits = []
    per_scenario: dict[str, Counter] = defaultdict(Counter)
    failures: dict[str, list[str]] = defaultdict(list)
    for item in results:
        scenario = scenarios[item["scenario"]]
        turn = scenario.turns[item["turn"]]
        verdict = item["verdict"]
        sc = per_scenario[scenario.id]
        sc["turns"] += 1
        sc["ok"] += verdict["ok"]
        cats = turn_categories(turn, scenario)
        for cat in cats:
            by_cat[cat]["turns"] += 1
            by_cat[cat]["turns_ok"] += verdict["ok"]
        if turn.expect.outcome in DECISION_CATEGORY or turn.expect.outcome == "ignore":
            decisions[turn.expect.outcome]["n"] += 1
            decisions[turn.expect.outcome]["ok"] += bool(verdict.get("outcome_ok"))
            for cat in cats[:1]:
                by_cat[cat]["decisions"] += 1
                by_cat[cat]["decisions_ok"] += bool(verdict.get("outcome_ok"))
        for check in verdict.get("values", []) + verdict.get("absent", []):
            by_cat[check["category"]]["values"] += 1
            by_cat[check["category"]]["values_ok"] += check["ok"]
            where[check["where"]]["n"] += 1
            where[check["where"]]["ok"] += check["ok"]
            kinds[check["kind"]]["n"] += 1
            kinds[check["kind"]]["ok"] += check["ok"]
            sc["values"] += 1
            sc["values_ok"] += check["ok"]
            if not check["ok"]:
                failures[scenario.id].append(
                    f"{turn.label} {check['value']} not in {check['where']}"
                )
        if not verdict.get("outcome_ok"):
            failures[scenario.id].append(
                f"{turn.label} expected {turn.expect.outcome}, got {verdict['outcome_got']}"
            )
        for flag in ("cut_off", "delegated_early", "missed"):
            if verdict.get(flag):
                taking[flag] += 1
                sc[flag] += 1
                failures[scenario.id].append(f"{turn.label} {flag.replace('_', ' ')}")
        if verdict.get("not_over_voice"):
            taking["not_over_voice"] += 1
        if "backchannel_taken" in verdict:
            taking["backchannel_turns"] += 1
            taking["backchannel_taken"] += bool(verdict["backchannel_taken"])
        if "barge_in_ok" in verdict:
            taking["barge_in_turns"] += 1
            taking["barge_in_ok"] += bool(verdict["barge_in_ok"])
            if not verdict["barge_in_ok"]:
                failures[scenario.id].append(f"{turn.label} barge-in not honoured")
        if verdict.get("false_barge_in"):
            taking["false_barge_in"] += verdict["false_barge_in"]
            failures[scenario.id].append(f"{turn.label} false barge-in")
        for fault in verdict.get("wording", []):
            wording[fault] += 1
            sc["wording_faults"] += 1
        if verdict.get("first_audio_ms") is not None:
            first_audio.append(verdict["first_audio_ms"])
        if verdict.get("wait_silence_ms") is not None:
            waits.append(verdict["wait_silence_ms"])

    def cat_row(c: Counter) -> dict:
        row = {"turns": c["turns"], "turns_ok": c["turns_ok"]}
        for key in ("decisions", "values"):
            if c[key]:
                row[key], row[f"{key}_ok"] = c[key], c[f"{key}_ok"]
        if c["decisions"]:
            row["decision_rate"] = _rate(c["decisions_ok"], c["decisions"])
        if c["values"]:
            row["exact_rate"] = _rate(c["values_ok"], c["values"])
        return row

    order = list(CATEGORIES) + ["keywords", "meaning", "pauses"]
    turns_total = sum(c["turns"] for c in per_scenario.values())
    return {
        "overall": {
            "scenarios": len(per_scenario),
            "scenarios_ok": sum(c["ok"] == c["turns"] for c in per_scenario.values()),
            "turns": turns_total,
            "turns_ok": sum(c["ok"] for c in per_scenario.values()),
            "values": sum(c["values"] for c in per_scenario.values()),
            "values_ok": sum(c["values_ok"] for c in per_scenario.values()),
        },
        "by_category": {k: cat_row(by_cat[k]) for k in order if k in by_cat},
        "decisions": {
            k: {"n": c["n"], "ok": c["ok"], "rate": _rate(c["ok"], c["n"])}
            for k, c in sorted(decisions.items())
        },
        "values_by_where": {
            k: {"n": c["n"], "ok": c["ok"], "rate": _rate(c["ok"], c["n"])}
            for k, c in sorted(where.items())
        },
        "values_by_kind": {
            k: {"n": c["n"], "ok": c["ok"], "rate": _rate(c["ok"], c["n"])}
            for k, c in sorted(kinds.items())
        },
        "turn_taking": dict(sorted(taking.items())),
        "wording": dict(sorted(wording.items())),
        "first_audio_ms": {
            "median": _median(first_audio),
            "p90": _p90(first_audio),
            "n": len(first_audio),
        },
        "wait_silence_ms": {
            "median": _median(waits),
            "max": max(waits) if waits else None,
        },
        "by_scenario": {
            sid: {
                "turns": c["turns"],
                "turns_ok": c["ok"],
                "values": c["values"],
                "values_ok": c["values_ok"],
                "pass": c["ok"] == c["turns"],
                "failures": failures.get(sid, []),
                **{
                    k: c[k]
                    for k in ("cut_off", "delegated_early", "missed", "wording_faults")
                    if c[k]
                },
            }
            for sid, c in sorted(per_scenario.items())
        },
    }


def aggregate_timings(timings: list[dict]) -> dict:
    """Medians of the engine's per-turn stage times (``Timings.as_dict()``),
    for answered and delegated turns apart."""
    stages = (
        "turn_wait_ms",
        "stt_ms",
        "llm_first_token_ms",
        "llm_first_text_ms",
        "tts_first_audio_ms",
        "first_audio_ms",
    )
    out = {}
    for label, rows in (
        ("answered", [t for t in timings if not t.get("delegated")]),
        ("delegated", [t for t in timings if t.get("delegated")]),
        ("all", timings),
    ):
        out[label] = {"turns": len(rows)}
        for stage in stages:
            out[label][stage] = _median(t.get(stage) for t in rows)
        out[label]["first_audio_ms_p90"] = _p90(t.get("first_audio_ms") for t in rows)
    return out
