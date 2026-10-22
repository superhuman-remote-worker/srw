"""The voice test set's parsing, value matching, phone conditions, scoring and
reports (tests/voice_bench/evalset/); the full run needs the GPU servers and
stays manual (scripts/voice-eval.sh)."""

from __future__ import annotations

import datetime as dt
import re
import shutil

import numpy as np
import pytest

from tests.voice_bench import call
from tests.voice_bench.evalset import conditions as C
from tests.voice_bench.evalset import report as Rep
from tests.voice_bench.evalset import scoring as S
from tests.voice_bench.evalset import spec
from tests.voice_bench.evalset import values as V
from tests.voice_bench.evalset.spec import SpecError, Value
from tests.voice_bench.srw_driver import match_answer

TODAY = dt.date(2026, 10, 22)  # a Thursday


# -- the shipped test set ------------------------------------------------------------


@pytest.fixture(scope="module")
def evalset():
    return spec.load()


class TestShippedSet:
    def test_loads_with_every_reference_resolved(self, evalset):
        assert len(evalset.voices.voices) >= 7
        assert set(evalset.corpus.conditions) <= set(evalset.conditions)
        assert len(evalset.scenarios) >= 15
        cats = {u.category for u in evalset.corpus.utterances.values()}
        assert cats == set(spec.CATEGORIES)

    def test_voices_cover_gender_age_and_pace(self, evalset):
        designed = [
            v for v in evalset.voices.voices.values() if v.engine == "qwen-design"
        ]
        assert {v.gender for v in designed} == {"female", "male"}
        assert {"young", "older"} <= {v.age for v in designed}
        assert {"fast", "slow"} <= {v.pace for v in designed}
        assert any(
            v.engine == "piper" and v.hard for v in evalset.voices.voices.values()
        )

    def _all_values(self, evalset):
        for utt in evalset.corpus.utterances.values():
            yield utt.id, utt.values
        for scenario in evalset.scenarios.values():
            for turn in scenario.turns:
                yield (
                    f"{scenario.id}.{turn.label}",
                    turn.expect.values + turn.expect.absent,
                )

    def test_phone_numbers_are_drama_numbers_only(self, evalset):
        drama = re.compile(
            r"^(030 23125 \d{3}|069 90009 \d{3}|040 66969 \d{3}|0221 4710 \d{3}|089 99998 \d{3}"
            r"|\+49 30 23125 \d{3}|\+49 69 90009 \d{3}|\+49 40 66969 \d{3}"
            r"|0152 28817386|0152 28895456|0152 54599371|0171 39200\d\d|0172 9925904"
            r"|0172 9968532|0172 99731(85|86)|0172 9980752|0174 9091317|0174 9464308|0176 040690\d\d)$"
        )
        phones = [
            (where, v.value)
            for where, vals in self._all_values(evalset)
            for v in vals
            if v.kind == "phone"
        ]
        assert phones
        for where, number in phones:
            assert drama.match(number), (
                f"{where}: {number} is not a reserved fictional number"
            )

    def test_email_domains_are_reserved(self, evalset):
        mails = [
            v.value
            for _, vals in self._all_values(evalset)
            for v in vals
            if v.kind == "email"
        ]
        assert mails and all(m.endswith(".example") for m in mails)

    def test_every_value_is_decidable(self, evalset):
        for where, vals in self._all_values(evalset):
            for value in vals:
                if value.kind == "date":
                    assert V.is_known_date(value.value), f"{where}: {value.value}"
                if value.kind == "weekday":
                    assert V.resolve_relative(value.value, TODAY), (
                        f"{where}: {value.value}"
                    )
                if value.kind == "time":
                    assert re.fullmatch(r"\d{1,2}:\d{2}", value.value), where

    def test_corpus_values_match_their_own_text(self, evalset):
        """A perfect transcript (the text itself) passes every exact check."""
        for utt in evalset.corpus.utterances.values():
            for value in utt.values:
                assert V.match(value, utt.reference, TODAY).ok, (
                    f"{utt.id}: {value.label()}"
                )

    def test_scenario_request_values_match_the_callers_words(self, evalset):
        for scenario in evalset.scenarios.values():
            for turn in scenario.turns:
                for value in turn.expect.values:
                    if value.where in ("request", "stt"):
                        assert V.match(value, spec.reference(turn.text), TODAY).ok, (
                            f"{scenario.id}.{turn.label}: {value.label()}"
                        )

    def test_back_office_answers_hold_the_reply_values(self, evalset):
        """What the voice must relay is in the canned answer it gets."""
        for scenario in evalset.scenarios.values():
            for turn in scenario.turns:
                if turn.back_office is None:
                    continue
                for value in turn.expect.values:
                    if value.where == "reply":
                        assert V.match(value, turn.back_office.answer, TODAY).ok, (
                            f"{scenario.id}.{turn.label}: {value.label()}"
                        )


# -- parsing -------------------------------------------------------------------------


class TestParsing:
    def test_pauses_split_the_text(self):
        pieces = spec.segments("von Montag <pause 2.0> bis <pause 1.5>  Mittwoch.")
        assert [(p.text, p.pause_after) for p in pieces] == [
            ("von Montag", 2.0),
            ("bis", 1.5),
            ("Mittwoch.", 0.0),
        ]
        assert (
            spec.reference("von Montag <pause 2> bis Mittwoch.")
            == "von Montag bis Mittwoch."
        )
        assert spec.has_pause("a <pause 1.5> b") and not spec.has_pause(
            "a <pause 0.5> b"
        )

    def test_unknown_markup_is_refused(self):
        with pytest.raises(SpecError, match="markup"):
            spec.segments("hallo <laugh> du")

    def test_values(self):
        assert spec.parse_value({"name": "Müller", "in": "request"}, "v") == Value(
            "name", "Müller", "request"
        )
        with pytest.raises(SpecError, match="exactly one"):
            spec.parse_value({"name": "A", "phone": "1"}, "v")
        with pytest.raises(SpecError, match="in must be"):
            spec.parse_value({"name": "A", "in": "nowhere"}, "v")
        with pytest.raises(SpecError, match="needs a value"):
            spec.parse_value({"keyword": ""}, "v")

    def test_conditions(self):
        conds = spec.parse_conditions(
            {
                "conditions": [
                    {
                        "id": "x",
                        "ops": [
                            {"op": "noise", "kind": "cafe", "snr_db": 10},
                            {"op": "codec", "name": "g711a"},
                        ],
                    }
                ]
            }
        )
        assert conds["x"].codec == "g711a"
        assert [o.op for o in conds["x"].stage("acoustic").ops] == ["noise"]
        for bad in (
            {"op": "codec", "name": "opus"},
            {"op": "noise", "kind": "rain", "snr_db": 3},
            {"op": "loss", "rate": 0.7},
            {"op": "band", "low": 3400, "high": 300},
            {"op": "level"},
        ):
            with pytest.raises(SpecError):
                spec.parse_conditions({"conditions": [{"id": "y", "ops": [bad]}]})

    def _scenario(self, **turn):
        return {
            "id": "s99-test",
            "voice": "v",
            "condition": "c",
            "turns": [{"say": "Hallo.", **turn}],
        }

    def test_scenario(self):
        s = spec.parse_scenario(
            self._scenario(
                expect={"outcome": "delegate", "values": [{"name": "Hall"}]},
                back_office={"answer": "Ja.", "after": 6, "match": ["Zimmer|Raum"]},
            )
        )
        turn = s.turns[0]
        assert turn.expect.outcome == "delegate" and turn.back_office.after == 6.0
        assert s.back_office()[0].as_driver_entry() == {
            "keywords": ["zimmer|raum"],
            "answer": "Ja.",
            "delay": 6.0,
        }
        assert s.default_answer == spec.DEFAULT_ANSWER

    def test_scenario_errors(self):
        with pytest.raises(SpecError, match="barge_in"):
            spec.parse_scenario(self._scenario(expect={"barge_in": True}))
        with pytest.raises(SpecError, match="exactly one"):
            spec.parse_scenario(self._scenario(silence=2.0))
        with pytest.raises(SpecError, match="outcome"):
            spec.parse_scenario(self._scenario(expect={"outcome": "maybe"}))
        with pytest.raises(SpecError, match="unknown"):
            spec.parse_scenario(self._scenario(colour="red"))
        bad = self._scenario()
        bad["id"] = "test"
        with pytest.raises(SpecError, match="s01-short-name"):
            spec.parse_scenario(bad)

    def test_corpus_style_and_pause_agree(self):
        with pytest.raises(SpecError, match="pause"):
            spec.parse_utterance(
                {"id": "x", "category": "names", "text": "a <pause 2> b"}, "u"
            )
        utt = spec.parse_utterance(
            {"id": "x", "category": "names", "style": "pause", "text": "a <pause 2> b"},
            "u",
        )
        assert utt.reference == "a b"

    def test_human_template_parses(self):
        import yaml

        raw = yaml.safe_load(
            (spec.EVALSET_DIR / "human.example.yaml").read_text("utf-8")
        )
        assert spec.parse_human(raw, spec.HUMAN_DIR)[0].speaker == "s01"

    def test_human_manifest(self, tmp_path):
        raw = {
            "recordings": [
                {
                    "id": "a1",
                    "file": "a1.wav",
                    "text": "Hallo.",
                    "speaker": "s1",
                    "consent": "signed 2026-10-22",
                }
            ]
        }
        recs = spec.parse_human(raw, tmp_path)
        assert recs[0].id == "human-a1" and recs[0].file == tmp_path / "a1.wav"
        del raw["recordings"][0]["consent"]
        with pytest.raises(SpecError, match="consent"):
            spec.parse_human(raw, tmp_path)


# -- exact value matching ----------------------------------------------------------------


class TestValues:
    @pytest.mark.parametrize(
        ("word", "number"),
        [
            ("dreiundzwanzig", 23),
            ("einhundertneunundzwanzig", 129),
            ("achthundertzwölf", 812),
            ("zweitausendsechsundzwanzig", 2026),
            ("dreißig", 30),
            ("siebzehn", 17),
            ("Stunde", None),
            ("Grund", None),
        ],
    )
    def test_cardinals(self, word, number):
        assert V.cardinal(word) == number

    @pytest.mark.parametrize(
        ("word", "number"),
        [
            ("dreizehnten", 13),
            ("ersten", 1),
            ("achten", 8),
            ("sechste", 6),
            ("einunddreißigste", 31),
            ("fünft", 5),
            ("erst", None),
            ("Nacht", None),
        ],
    )
    def test_ordinals(self, word, number):
        assert V.ordinal(word) == number

    def test_digit_streams(self):
        assert (
            V.digit_stream("null drei null, zwei drei eins zwei fünf, vier siebzehn")
            == "03023125417"
        )
        assert V.digit_stream("Rückruf unter 030 23125-417 bitte") == "03023125417"
        assert V.digit_stream("vier sieben, nein, vier acht") == "48"

    def test_phone_forms(self):
        assert V.match(Value("phone", "030 23125 417"), "unter 03023125417").ok
        assert V.match(
            Value("phone", "030 23125 417"),
            "plus vier neun drei null zwei drei eins zwei fünf vier eins sieben",
        ).ok
        assert V.match(Value("phone", "+49 40 66969 300"), "040 66969300").ok
        assert not V.match(Value("phone", "030 23125 417"), "Hall1234").ok

    def test_dates(self):
        assert V.dates("am dreizehnten November und am 3.12. oder 24. Dezember") == {
            (13, 11),
            (3, 12),
            (24, 12),
        }
        assert V.match(Value("date", "13.11."), "für den 13. November").ok
        assert not V.match(Value("date", "13.11."), "für den 12. November").ok

    def test_relative_dates(self):
        assert V.resolve_relative("nächste Woche Montag", TODAY) == dt.date(
            2026, 10, 26
        )
        assert V.resolve_relative("übermorgen", TODAY) == dt.date(2026, 10, 24)
        assert V.resolve_relative("in zwei Wochen am Dienstag", TODAY) == dt.date(
            2026, 11, 3
        )
        assert V.resolve_relative("kommenden Freitag", TODAY) == dt.date(2026, 10, 23)
        value = Value("date", "nächste Woche Montag")
        assert V.match(value, "Ist nächste Woche Montag frei?", TODAY).how == "phrase"
        assert V.match(value, "am Montag, den 26. Oktober", TODAY).how == "resolved"
        assert not V.match(Value("date", "übermorgen"), "Ich komme morgen", TODAY).ok

    def test_weekday(self):
        value = Value("weekday", "übermorgen")
        assert V.match(value, "Übermorgen ist Samstag.", TODAY).ok
        assert not V.match(value, "Übermorgen ist Freitag.", TODAY).ok
        assert V.match(value, "Samstag", None).how == "undecided"

    def test_times(self):
        assert V.times(
            "um halb acht, 19:30, viertel nach drei, neunzehn Uhr dreißig, 14 Uhr"
        ) == {
            (7, 30),
            (19, 30),
            (3, 15),
            (14, 0),
        }
        assert V.match(Value("time", "19:30"), "um halb acht abends").ok
        assert V.match(Value("time", "6:45"), "um Viertel vor sieben").ok

    def test_names(self):
        assert V.match(Value("name", "Müller"), "Mein Name ist Mueller").ok
        spelled = V.match(
            Value("name", "Kowalczyk"), "Kovalski, K, O, W, A, L, C, Z, Y, K"
        )
        assert spelled.ok and spelled.how == "spelled"
        assert V.match(
            Value("name", "Leuschner"),
            "L wie Leipzig, E wie Essen, U wie Unna, S wie Salzwedel, C wie Chemnitz, H wie Hamburg, N wie Nürnberg, E wie Essen, R wie Rostock",
        ).ok
        assert V.match(Value("name", "Quandt-Reiter"), "Quandt Reiter").ok
        assert V.match(
            Value("name", "Oelschlägel"), "O, E, L, S, C, H, L, Ä, G, E, L"
        ).ok
        assert not V.match(Value("name", "Przybilla"), "Mein Name ist Zubilla").ok
        assert V.spelled_runs("er es de") == []  # letter names alone are words

    def test_email_and_address(self):
        assert V.match(
            Value("email", "jana.probe@beispiel.example"),
            "Jana Punkt Probe at Beispiel Punkt Example",
        ).ok
        assert V.match(
            Value("email", "reisen_2026@beispiel.example"),
            "reisen unterstrich zwanzig sechsundzwanzig at beispiel punkt example",
        ).ok
        assert not V.match(
            Value("email", "jana.probe@beispiel.example"), "Jana Probe bei Beispiel"
        ).ok
        jana = Value("email", "jana.probe@beispiel.example")
        assert V.match(jana, "ist jana.probe-at-Beispiel.example.").ok
        assert not V.match(jana, "ist jana.probeartbeispiel.example").ok
        info = Value("email", "info@musterfirma-bau.example")
        assert V.match(info, "info-at-musterfirma-bau.example").ok
        address = Value("address", "Lindenallee 23, 99999 Beispielstadt")
        assert V.match(
            address, "an die Linden-Allee 23 in neun neun neun neun neun Beispielstadt"
        ).ok
        assert not V.match(address, "an die Lindenallee 32, 99999 Beispielstadt").ok

    def test_numbers_and_keywords(self):
        assert V.match(Value("number", "5"), "Wir sind zu fünft.").ok
        assert not V.match(Value("number", "1"), "ein Zimmer").ok
        assert V.match(Value("keyword", "10:30|halb elf"), "bis halb elf").ok
        assert (
            V.match(Value("meaning", "x", keywords=("feier",)), "die Feier").how
            == "keywords"
        )
        assert V.match(Value("meaning", "x"), "die Feier").how == "undecided"

    def test_fillers(self):
        assert V.strip_fillers("Ähm, also ich, äh, möchte") == "also ich, möchte"


# -- phone conditions -------------------------------------------------------------------


def _tone(seconds=1.0, freq=440.0, amp=0.3):
    t = np.arange(int(seconds * C.RATE)) / C.RATE
    return C.to_int16(amp * np.sin(2 * np.pi * freq * t))


def _cond(*ops, cid="c"):
    return spec.Condition(cid, "", tuple(spec.parse_op(o, "op") for o in ops))


class TestConditions:
    def test_g711_reference_points(self):
        assert C.alaw_encode(np.array([0], np.int16))[0] == 0xD5
        assert C.ulaw_encode(np.array([0], np.int16))[0] == 0xFF
        ramp = np.linspace(-32000, 32000, 2001).astype(np.int16)
        for enc, dec in (
            (C.alaw_encode, C.alaw_decode),
            (C.ulaw_encode, C.ulaw_decode),
        ):
            back = dec(enc(ramp)).astype(float)
            assert np.all(np.diff(back) >= 0)  # monotonic
            assert np.all(np.abs(back - ramp) <= 0.07 * np.abs(ramp) + 16)

    def test_g711_keeps_speech_band_and_length(self):
        tone = _tone()
        out = C.apply(tone, _cond({"op": "codec", "name": "g711a"}), "x")
        assert len(out) == len(tone)
        assert abs(np.corrcoef(out.astype(float), tone.astype(float))[0, 1]) > 0.99
        high = _tone(freq=6000)
        assert np.std(
            C.apply(high, _cond({"op": "codec", "name": "g711u"}), "x")
        ) < 0.05 * np.std(high)

    def test_band_limit(self):
        low = C.to_float(_tone(freq=100))
        assert np.std(C.band_limit(low, 300, 3400)) < 0.2 * np.std(low)

    def test_level_and_clip(self):
        tone = _tone(amp=0.5)
        quiet = C.apply(tone, _cond({"op": "level", "db": -20}), "x")
        assert np.std(quiet) == pytest.approx(np.std(tone) / 10, rel=0.02)
        info = {}
        loud = C.apply(tone, _cond({"op": "clip", "gain_db": 18}), "x", info=info)
        assert np.max(np.abs(loud)) <= 32767 and info["clipped_share"] > 0.5

    def test_packet_loss_rate_and_bursts(self):
        rng = np.random.default_rng(1)
        lost = C.loss_pattern(20000, 0.08, 3.0, rng)
        assert lost.mean() == pytest.approx(0.08, abs=0.015)
        runs = np.diff(np.flatnonzero(np.diff(np.r_[0, lost.astype(int), 0])))[::2]
        assert runs.mean() == pytest.approx(3.0, rel=0.25)
        info = {}
        out = C.apply(
            _tone(5), _cond({"op": "loss", "rate": 0.1, "burst": 2}), "x", info=info
        )
        assert 0.03 < info["lost_share"] < 0.2
        frames = out[: len(out) // 320 * 320].reshape(-1, 320)
        assert np.sum(np.all(frames == 0, axis=1)) > 10

    @pytest.mark.parametrize("kind", ["street", "cafe", "office", "white"])
    def test_noise_at_snr(self, kind):
        speech = C.to_float(_tone(3))
        rng = np.random.default_rng(3)
        background = C.noise(kind, len(speech), rng)
        mixed = C.mix_at_snr(speech, background, 10)
        noise_rms = np.sqrt(np.mean((mixed - speech) ** 2))
        snr = 20 * np.log10(C.active_level(speech) / noise_rms)
        assert snr == pytest.approx(10, abs=0.5)

    def test_noise_on_silence_uses_a_reference_level(self):
        silence = np.zeros(C.RATE, np.int16)
        out = C.apply(
            silence, _cond({"op": "noise", "kind": "street", "snr_db": 10}), "x"
        )
        level = 20 * np.log10(np.sqrt(np.mean(C.to_float(out) ** 2)))
        assert level == pytest.approx(C.SILENT_REFERENCE_DBFS - 10, abs=1.0)

    def test_same_clip_same_audio(self):
        cond = _cond(
            {"op": "noise", "kind": "cafe", "snr_db": 10}, {"op": "loss", "rate": 0.05}
        )
        tone = _tone(2)
        a, b = C.apply(tone, cond, "clip-1"), C.apply(tone, cond, "clip-1")
        assert np.array_equal(a, b)
        assert not np.array_equal(a, C.apply(tone, cond, "clip-2"))

    def test_stages(self):
        cond = _cond({"op": "level", "db": -6}, {"op": "codec", "name": "g711a"})
        acoustic = C.apply(_tone(), cond, "x", stage="acoustic")
        assert np.std(acoustic) == pytest.approx(np.std(_tone()) / 2, rel=0.02)

    @pytest.mark.skipif(shutil.which("ffmpeg") is None, reason="ffmpeg not installed")
    @pytest.mark.parametrize("codec", ["g722", "amr-nb"])
    def test_ffmpeg_codecs(self, codec):
        tone = _tone(freq=440)
        out = C.apply(tone, _cond({"op": "codec", "name": codec}), "x")
        assert len(out) == len(tone)
        assert np.std(out) > 0.3 * np.std(tone)


# -- scoring -----------------------------------------------------------------------------


def _wer(ref: str, hyp: str) -> float:
    return 0.0 if ref.lower() == hyp.lower() else 1.0


def _turn(**kw):
    raw = {
        "id": "s99-test",
        "voice": "v",
        "condition": "c",
        "turns": [{"say": "Hallo du da.", **kw}],
    }
    return spec.parse_scenario(raw)


class TestScoring:
    def test_stt_item(self):
        utt = spec.parse_utterance(
            {
                "id": "p",
                "category": "phone_numbers",
                "text": "Ähm, null drei null, zwei drei eins zwei fünf, vier eins sieben.",
                "style": "hesitation",
                "values": [{"phone": "030 23125 417"}],
            },
            "u",
        )
        good = S.score_stt(
            utt, "Null drei null, zwei drei eins zwei fünf, vier eins sieben.", wer=_wer
        )
        assert good["wer"] == 0.0 and good["values"][0]["ok"]
        bad = S.score_stt(utt, "Hall1234", wer=_wer)
        assert not bad["values"][0]["ok"]
        silent = spec.parse_utterance(
            {"id": "s", "category": "silence", "silence": 2.0}, "u"
        )
        assert S.score_stt(silent, "Untertitel im Auftrag des ZDF", wer=_wer)["phantom"]

    def test_answer_turn(self):
        scenario = _turn(
            expect={
                "outcome": "answer",
                "values": [{"keyword": "11|elf", "in": "reply"}],
            }
        )
        obs = {
            "caller_start_ms": 1000,
            "caller_end_ms": 3000,
            "reply": "Der Check-out ist bis 11 Uhr möglich.",
            "voice_starts": [4200],
            "flushes": [],
            "delegations": [],
            "first_audio_ms": 1200,
        }
        verdict = S.score_turn(scenario.turns[0], obs, wording=lambda r, d: [])
        assert verdict["ok"] and verdict["outcome_got"] == "answer"
        late = {**obs, "voice_starts": [12000]}
        assert S.score_turn(scenario.turns[0], late)["missed"]
        early = {**obs, "voice_starts": [2000, 4200]}
        assert S.score_turn(scenario.turns[0], early)["cut_off"]
        holding = {**obs, "voice_sentences": [2100]}
        assert S.score_turn(scenario.turns[0], holding)["cut_off"]

    def test_delegated_turn_values_in_the_request(self):
        scenario = _turn(
            expect={
                "outcome": "message",
                "values": [{"phone": "030 23125 417"}, {"name": "Hall"}],
            }
        )
        obs = {
            "caller_start_ms": 0,
            "caller_end_ms": 5000,
            "reply": "Einen Moment, ich frage nach.",
            "holding": "Einen Moment, ich frage nach.",
            "delegations": [
                {
                    "at_ms": 5600,
                    "request": "Hall eins zwei drei vier",
                    "request_llm": "Rückruf an Hall unter 030 23125 417",
                }
            ],
            "voice_starts": [5600],
        }
        verdict = S.score_turn(scenario.turns[0], obs)
        phone, name = verdict["values"]
        assert (
            phone["where"] == "request" and phone["found_in"] == ["llm"] and phone["ok"]
        )
        assert name["found_in"] == ["transcript", "llm"]
        early = {**obs, "delegations": [{"at_ms": 3000, "request": "Hall"}]}
        assert S.score_turn(scenario.turns[0], early)["delegated_early"]

    def test_backchannel_and_barge_in(self):
        back = _turn(when="during_voice", expect={"outcome": "ignore"}).turns[0]
        quiet = {
            "caller_start_ms": 0,
            "caller_end_ms": 600,
            "stt_events": [],
            "delegations": [],
            "flushes": [],
        }
        assert S.score_turn(back, quiet)["backchannel_taken"] is False
        taken = S.score_turn(back, {**quiet, "stt_events": [900]})
        assert taken["backchannel_taken"] and not taken["ok"]
        barge = _turn(
            when="during_voice", expect={"outcome": "answer", "barge_in": True}
        ).turns[0]
        obs = {
            "caller_start_ms": 1000,
            "caller_end_ms": 3000,
            "reply": "Dienstag ab 17:30.",
            "flushes": [1500],
            "voice_starts": [500, 3800],
        }
        assert S.score_turn(barge, obs)["barge_in_ok"]
        assert S.score_turn(barge, {**obs, "flushes": []})["barge_in_ok"] is False
        assert (
            S.score_turn(barge, {**obs, "flushes": None, "voice_stopped_ms": 5000})[
                "barge_in_ok"
            ]
            is False
        )

    def test_lines_meant_for_over_the_voice_need_the_voice(self):
        back = _turn(when="during_voice", expect={"outcome": "ignore"}).turns[0]
        silent = {
            "caller_start_ms": 0,
            "caller_end_ms": 600,
            "stt_events": [900],
            "voice_was_talking": False,
        }
        verdict = S.score_turn(back, silent)
        assert verdict["not_over_voice"] and verdict["outcome_ok"]
        assert "backchannel_taken" not in verdict
        barge = _turn(
            when="during_voice", expect={"outcome": "answer", "barge_in": True}
        ).turns[0]
        verdict = S.score_turn(barge, {**silent, "caller_end_ms": 2000, "flushes": []})
        assert "barge_in_ok" not in verdict and verdict["not_over_voice"]

    def test_false_barge_in_and_absent_values(self):
        turn = _turn(
            expect={
                "outcome": "delegate",
                "absent": [{"date": "12.11.", "in": "reply"}],
            }
        ).turns[0]
        obs = {
            "caller_start_ms": 0,
            "caller_end_ms": 1000,
            "reply": "Am 12. November ist frei.",
            "delegations": [{"at_ms": 1500, "request": "x"}],
            "flushes": [2000],
            "voice_starts": [1500],
        }
        verdict = S.score_turn(turn, obs)
        assert (
            verdict["false_barge_in"] == 1
            and not verdict["absent"][0]["ok"]
            and not verdict["ok"]
        )

    def test_meaning_goes_to_the_judge_only_when_exact_fails(self):
        turn = _turn(
            expect={
                "outcome": "delegate",
                "values": [{"meaning": "die Firmenfeier", "keywords": ["feier"]}],
            }
        ).turns[0]
        calls = []

        def judge(value, text, where):
            calls.append(text)
            return {"pass": True, "model": "fake"}

        obs = {"delegations": [{"at_ms": 0, "request": "es geht um die Feier"}]}
        assert S.score_turn(turn, obs, judge=judge)["values"][0]["how"] == "keywords"
        obs = {"delegations": [{"at_ms": 0, "request": "es geht um das Betriebsfest"}]}
        verdict = S.score_turn(turn, obs, judge=judge)
        assert verdict["values"][0]["how"] == "llm_judge" and verdict["values"][0]["ok"]
        assert calls == ["es geht um das Betriebsfest"]

    def test_reply_windows_span_backchannels(self):
        raw = {
            "id": "s99-test",
            "voice": "v",
            "condition": "c",
            "turns": [
                {"say": "Frage."},
                {
                    "say": "Mhm.",
                    "when": "during_voice",
                    "expect": {"outcome": "ignore"},
                },
                {
                    "say": "Stopp, Dienstag.",
                    "when": "during_voice",
                    "expect": {"barge_in": True, "outcome": "answer"},
                },
                {"say": "Danke."},
            ],
        }
        turns = list(spec.parse_scenario(raw).turns)
        assert S.reply_windows(turns, [0, 10, 20, 30], 99) == [20, 20, 30, 99]

    def test_phantoms(self):
        found = S.phantom_transcripts(
            [(1000, "ja"), (20000, "Untertitel")], [(500, 2000)]
        )
        assert found == [(20000, "Untertitel")]

    def test_aggregates(self):
        scenario = _turn(
            expect={"outcome": "answer", "values": [{"keyword": "x", "in": "reply"}]}
        )
        results = [
            {
                "scenario": scenario.id,
                "turn": 0,
                "verdict": S.score_turn(scenario.turns[0], {"reply": "x"}),
            }
        ]
        summary = S.aggregate_turns(results, {scenario.id: scenario})
        assert summary["overall"]["turns_ok"] == 1
        assert summary["decisions"]["answer"]["rate"] == 1.0
        assert summary["by_category"]["facts"]["decision_rate"] == 1.0
        failed = S.score_turn(scenario.turns[0], {"reply": "y"})
        none_ok = S.aggregate_turns(
            [{"scenario": scenario.id, "turn": 0, "verdict": failed}],
            {scenario.id: scenario},
        )
        assert none_ok["by_category"]["keywords"] == {
            "turns": 0,
            "turns_ok": 0,
            "values": 1,
            "values_ok": 0,
            "exact_rate": 0.0,
        }
        assert Rep.weak_spots({"file": none_ok})
        items = [
            {
                "category": "names",
                "condition": "g711a",
                "voice": "v",
                "styles": ["plain"],
                "wer": 0.2,
                "values": [{"kind": "name", "ok": False}],
                "phantom": False,
            },
            {
                "category": "silence",
                "condition": "g711a",
                "voice": "v",
                "silence": True,
                "wer": None,
                "values": [],
                "phantom": True,
            },
        ]
        stt = S.aggregate_stt(items)
        assert stt["by_category"]["names"]["exact_rate"] == 0.0
        assert stt["overall"]["phantoms"] == 1
        timings = S.aggregate_timings(
            [
                {"stt_ms": 300, "first_audio_ms": 1200, "delegated": False},
                {"stt_ms": 500, "first_audio_ms": 1500, "delegated": True},
            ]
        )
        assert timings["all"]["stt_ms"] == 400.0 and timings["delegated"]["turns"] == 1


# -- reports -----------------------------------------------------------------------------


class TestReport:
    def test_diff_directions(self):
        base = {
            "stt": {"overall": {"wer_mean": 0.10, "exact_rate": 0.8, "clips": 10}},
            "file": {"timings": {"all": {"stt_ms": 300.0}}},
        }
        now = {
            "stt": {"overall": {"wer_mean": 0.20, "exact_rate": 0.9, "clips": 12}},
            "file": {"timings": {"all": {"stt_ms": 310.0}}},
        }
        rows = {r["metric"]: r["verdict"] for r in Rep.diff(now, base)}
        assert rows == {
            "stt.overall.wer_mean": "worse",
            "stt.overall.exact_rate": "better",
            "stt.overall.clips": "changed",
        }
        assert "worse 1" in Rep.diff_markdown(Rep.diff(now, base))
        assert Rep.direction("file.turn_taking.backchannel_taken") == -1
        assert Rep.direction("file.decisions.answer.rate") == 1

    def test_summary_markdown_and_weak_spots(self, tmp_path):
        (tmp_path / "run.json").write_text('{"profile": "p", "levels": ["stt"]}')
        stt = {
            "overall": {
                "clips": 2,
                "wer_mean": 0.3,
                "wer_median": 0.3,
                "perfect": 0,
                "values": 2,
                "values_ok": 1,
                "exact_rate": 0.5,
            },
            "by_category": {
                "names": {
                    "clips": 2,
                    "wer_mean": 0.3,
                    "perfect": 0,
                    "values": 2,
                    "values_ok": 1,
                    "exact_rate": 0.5,
                }
            },
            "by_condition": {},
            "by_voice": {},
            "by_style": {},
            "by_value_kind": {"name": {"n": 2, "ok": 1, "rate": 0.5}},
        }
        (tmp_path / "stt.json").write_text(__import__("json").dumps({"summary": stt}))
        summary = Rep.summarise(tmp_path)
        assert summary["meta"]["profile"] == "p"
        text = (tmp_path / "summary.md").read_text()
        assert "STT names: 1/2 values exact" in text
        assert "## STT" in text

    def test_compare(self):
        out = Rep.compare(
            {
                "a": {"stt": {"overall": {"wer_mean": 0.1}}},
                "b": {"stt": {"overall": {"wer_mean": 0.2}}},
            }
        )
        assert "| stt.overall.wer_mean | 0.10 | 0.20 |" in out


# -- the bench pieces the call level uses ------------------------------------------------


def _runner():
    import importlib.util

    path = spec.EVALSET_DIR.parents[2] / "scripts" / "voice_eval.py"
    loaded = importlib.util.spec_from_file_location("voice_eval_under_test", path)
    module = importlib.util.module_from_spec(loaded)
    loaded.loader.exec_module(module)
    return module


class TestRunner:
    def test_call_schedule_leaves_room_for_each_reply(self):
        runner = _runner()
        raw = {
            "id": "s99-test",
            "voice": "v",
            "condition": "c",
            "turns": [
                {"say": "Frage.", "expect": {"outcome": "answer"}},
                {
                    "say": "Mhm.",
                    "when": "during_voice",
                    "offset": 0.5,
                    "expect": {"outcome": "ignore"},
                },
                {
                    "say": "Buchen.",
                    "expect": {"outcome": "delegate"},
                    "back_office": {"answer": "Ja.", "after": 6},
                },
                {"silence": 3.0, "when": "after_caller"},
            ],
        }
        turns = spec.parse_scenario(raw).turns
        one_second = bytes(2 * C.RATE)
        lines = [(t, one_second, 300, 700) for t in turns]
        times, hangup = runner.call_schedule(lines)
        # The question at 14 s; "Mhm" 1.6 s + 0.5 s after it; the next line
        # waits for the answer's room (8 s after the question), then the
        # delegation's room (6 + 10 s) before the hangup.
        assert times == [14.0, 17.1, 23.0, 24.0]
        assert hangup == 23.0 + 1 + 16 + 4
        assert runner.gateway_timings(
            "x INFO call live_1 cascade turns: [{'stt_ms': 300, 'delegated': False}]",
            "live_1",
        ) == [{"stt_ms": 300, "delegated": False}]

    def test_rescore_rematches_stored_stt_items(self, tmp_path):
        import argparse
        import json

        runner = _runner()
        (tmp_path / "run.json").write_text('{"profile": "p", "today": "2026-10-22"}')
        item = {
            "id": "eml-01",
            "category": "emails",
            "voice": "v",
            "condition": "g711a",
            "styles": ["plain"],
            "wer": 0.4,
            "heard": "Meine E-Mail-Adresse ist jana.probe-at-Beispiel.example.",
            "values": [{"kind": "email", "ok": False}],
        }
        (tmp_path / "stt.json").write_text(json.dumps({"items": [item]}))
        runner.rescore(tmp_path, argparse.Namespace(no_llm_judge=True))
        data = json.loads((tmp_path / "stt.json").read_text())
        assert data["items"][0]["values"][0]["ok"] is True
        assert data["summary"]["by_value_kind"]["email"]["rate"] == 1.0


class TestBenchHooks:
    def test_canned_back_office_matching(self):
        answers = [
            {"keywords": ["zimmer|raum"], "answer": "A"},
            {"keywords": [], "answer": "B"},
        ]
        assert match_answer(answers, "Ein Zimmer bitte")[0] == 0
        assert match_answer(answers, "Ein Zimmer bitte", used={0})[0] == 1
        assert match_answer(answers[:1], "Rechnung") is None
        assert match_answer(answers[:1], "Rechnung", default="D") == (
            -1,
            {"answer": "D"},
        )

    def test_call_script_plays_a_rendered_line(self, tmp_path):
        script = call.parse_script(
            {
                "number": "107",
                "hangup_after": 30,
                "lines": [{"at": 5, "audio": str(tmp_path / "t1.wav")}],
            }
        )
        assert call.line_audio(script.lines[0]) == tmp_path / "t1.wav"
        assert call.render_jobs(script) == []
        with pytest.raises(call.ScriptError, match="either"):
            call.parse_script(
                {
                    "number": "107",
                    "hangup_after": 30,
                    "lines": [{"at": 5, "audio": "a.wav", "text": "x"}],
                }
            )
