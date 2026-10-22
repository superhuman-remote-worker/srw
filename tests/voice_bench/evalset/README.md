# The voice system's end-to-end test set

A German test set for SRW's local voice path (knowledge-base
`local_voice_gateway.md` §5, §9.9), so that a change to a model, a prompt or the
turn taking is measured instead of guessed. It holds a synthetic caller corpus
that is rendered from text (several voices, phone conditions, speaking styles,
names, numbers and dates), multi-turn scenarios with expected outcomes, and a
runner at three levels with a stored baseline.

```
 text (this folder)           render (CPU, cached)         run
 voices.yaml  ─┐              Qwen3-TTS VoiceDesign ─┐     stt   every clip × condition → profile STT
 corpus.yaml  ─┼─ voice-eval ─ Qwen3-TTS Base clone  ─┼──── file  scenarios → gateway Session + CascadeEngine
 scenarios/   ─┤   render     Piper (hard voice)    ─┘     call  scenarios → bench PBX → addon → gateway
 conditions   ─┘                                             └─ summary.json/.md, diff against baselines/
```

Everything here is text and code. Audio lives in the gitignored
`tests/voice_bench/.cache/evalset/` and results in `tests/voice_bench/results/`.

## Run it

Needs the voice environment (Pipecat; `.venv-voice` at the repository root or
`SRW_VOICE_PYTHON`), ffmpeg (G.722, AMR-NB, tempo), Docker, and a cascade
profile's model servers running (for example `cascade-gpu-rocm`,
`docker/voice-models/README.md`). The runner never starts, stops or rebuilds
those servers.

```bash
export SRW_VOICE_PYTHON=$PWD/.venv-voice/bin/python VOICE_MODELS_DIR=~/.cache/srw-voice-models
python docker/voice-models/models.py fetch --set rocm,design,judge   # once: the CPU renderer's models
scripts/voice-eval.sh judge up               # Qwen3-ASR judge on the CPU, 127.0.0.1:18495
scripts/voice-eval.sh render --judge-url http://127.0.0.1:18495/v1   # voices, pieces, clips (cached)
scripts/voice-eval.sh run --profile-file docker/voice-models/cascade-gpu.yaml \
    --profile cascade-gpu-rocm --levels stt,file,call \
    --judge-url http://127.0.0.1:18495/v1 --judge-label "Qwen3-ASR 0.6B on the CPU"
scripts/voice-eval.sh judge down
```

`run` writes `tests/voice_bench/results/eval-<UTC>-<profile>/` with
`run.json` (what ran: source revision, stages, judge, GPU memory), `stt.json`,
`file.json`, `call.json` (every measurement), `summary.json`, `summary.md` (per
category, condition, voice and scenario, with the weak spots on top) and,
when `baselines/<profile>.json` exists, `diff.md`. `file/<scenario>/call.wav`
is the stereo recording of each harness call (caller left, voice right); the
call level keeps the bench's recordings per scenario. Options: `--levels`
(default `stt,file`), `--scenarios s03,s11` (ids or prefixes), `--conditions`,
`--categories`, `--voices`, `--limit` (STT level), `--out`, `--keep-up` (call
level), `--judge-label`. `report RUN --rescore` scores a stored run again with
the current matchers and expectations without placing a call (relative dates
against the day of the run).

Other commands:

| command | what it does |
|---|---|
| `list` | voices, conditions, corpus counts per category, scenarios |
| `render --try-seeds 101,111,121 [--voices f-middle]` | candidate takes for choosing a voice's seed (pitch, pace, judge) |
| `report RUN` | rebuild `summary.md` (and `diff.md`) of a run |
| `diff RUN [--baseline FILE] [--strict]` | what moved against the baseline, worst first (`--strict`: exit 1 when worse) |
| `baseline RUN` | store a run's summary as `baselines/<profile>.json` |
| `compare RUN_A RUN_B …` | runs, baselines or profiles side by side |
| `judge up\|down` | the CPU judge (its own compose project `srw-voice-eval-judge`) |
| `bench-down` | stop a call-level bench left up with `--keep-up` |

Comparing profiles: run the same levels with each `--profile` (and its
`--profile-file`), then `compare` the run directories. Timings are only
comparable between runs on the same hardware with the same other load; the
summary says that model servers shared with other clients make them noisy.

## The levels

**`stt` (fast, no PBX):** every corpus clip under every phone condition (the
sweep utterances in every voice under `sweep_conditions`), sent through the
engine's own STT code (`TranscriptionSTT`, including its repetition cut and
phantom filter) to the profile's STT server. Scores: word error rate (the
model bench's number-aware `wer`, fillers removed from both sides), words
perfect, and whether each value survived exactly; for silent clips, whether STT
wrote anything at all (a phantom). Reported per category, condition, voice,
speaking style and value kind.

**`file` (the cascade without a PBX):** each scenario as one call on the
gateway's real `Session` and `CascadeEngine` (Silero VAD, Smart Turn, STT,
LLM, TTS of the profile) over a file-backed phone line, as
`scripts/voice_cascade_try.py` does. The caller's lines get the scenario's
condition; between them the line carries the condition's background noise.
The canned back office answers each delegation after the scenario's delay;
the caller waits for the voice to finish (`after_voice`), talks over it
(`during_voice`, for backchannels and barge-in) or goes on regardless
(`after_caller`). Exact clock, so it sees every flush (barge-in).

**`call` (real turn taking through the phone path):** the bench PBX and SRW's
Asterisk addon (`voice-bench.sh addon` mode) in compose project
`srw-voice-eval` on `172.30.98.0/24` (never another bench), a gateway on the
host with the profile, and `srw_driver.py` as SRW's driver with the
scenario's canned back office (`log_content` writes the transcripts and each
delegation's request to the run's driver log). The bench caller plays each
scenario's lines with the condition's acoustic part (noise, level, clipping,
band); the line's codec is the real one the PBX and addon negotiate, and the
bench applies no packet loss. The bench caller cannot listen, so its lines
are scheduled: the room after a line is 8 s for an answer, the back office's
delay plus 10 s for a delegation, 1.6 s plus the offset into the voice for a
line said over it. What the caller heard is judged per voiced stretch;
stage timings come from the gateway's own log line; flushes are not
observable, so a barge-in counts as honoured when the voice the caller heard
stopped within 2 s.

## What is scored per scenario turn

| metric | how |
|---|---|
| decision | answered (voice spoke, no delegation), delegated, or quiet; `transfer` and `message` count as right when delegated (the back office transfers and takes messages); `ignore` turns (backchannels, silence) are right when no caller transcript, flush or delegation came of them |
| values | each expected value in the voice's reply (`in: reply`), the delegated request (`request`: the caller's words since the last delegation, as SRW's driver forwards them, or the voice LLM's own tool argument at file level), STT (`stt`) or `either`; `absent` values must not appear |
| caller cut off | the voice began audio or a sentence (a holding sentence included) while the caller was still talking, e.g. in a pause mid-sentence; only when the voice was silent as the caller began |
| delegated early | a delegation went out before the caller finished (the §9.9 "Mittwoch" bug) |
| turn missed | no audio and no delegation within 8 s of the caller's last word |
| backchannel taken | "mhm" over the voice produced a transcript, a flush or a delegation; a line meant for over the voice that met silence (the call level's fixed schedule against a slow reply) is reported as "not over the voice" and not scored |
| barge-in | a real interruption flushed the voice within 2 s (file) or the voice stopped within 2 s (call) |
| false barge-in | a flush that cut the voice outside a barge-in turn (file level) |
| phantom transcripts | caller transcripts with no caller speech near them (noise taken as words) |
| wording | `voice_cascade_eval.py`'s rules: no back office named, at most one holding sentence, no transfer promised before the answer, no new holding sentence when relaying |
| latency | the engine's per-turn stage times (turn wait, STT, LLM first token, LLM to first TTS text, TTS first audio, end of speech to first audio) and, at the caller, end of speech to the voice's first audio; the longest silence while the back office worked |
| intelligibility | what the judge ASR understood of the voice after the phone line against what the voice said (WER) |

**Exact first, an LLM only where exact rules cannot decide.** Names (also
spelled, with or without "K wie Köln"), phone numbers (any grouping, number
words or digits, "+49", a corrected digit group dropped), dates (numeric,
ordinal words, "vom zwölften bis vierzehnten März", relative phrases such as
"nächste Woche Montag" or the date they resolve to on the day of the run),
times ("halb acht", "viertel nach drei", "19:30"), e-mail addresses ("punkt",
"at", "minus", number words), postal addresses, counts and keywords are
matched by `values.py`. Only a `meaning` value (a free paraphrase) that its
keywords do not match goes to an LLM judge with this rubric (also in
`scripts/voice_eval.py`): *PASS only if the text carries the same fact, date
(relative dates resolved against today), number, name or request as the
value; wording, order and politeness do not matter; FAIL if the value is
missing, changed or only partly there.* The judge is the profile's own LLM
unless `--llm-judge-url`/`--llm-judge-model` name another; the run records
which. The shipped scenarios use no `meaning` value, so the baseline needed
no LLM judge.

**What the caller heard** is judged by Qwen3-ASR 0.6B (another model family
than the whisper STT it judges). With the GPU full of a profile's servers it
runs on the CPU (`judge-cpu.yaml`, about 1 s per stretch); the run records the
judge it used.

## The corpus

`de/corpus.yaml`: 123 single caller lines in 14 categories, scored under
12 conditions; 20 of them (`sweep: true`) also in every voice.

| category | lines | what |
|---|---|---|
| facts | 12 | questions the facts sheet answers |
| back_office | 10 | availability, bookings, invoices, lost items, groups |
| transfer | 6 | the caller wants a person |
| messages | 6 | something to pass on |
| names | 18 | common and rare German names, spelled (letters, DIN 5009 town words), double names, a self-correction, "Hall" |
| phone_numbers | 12 | German formats in groups, pairs ("neunundneunzig"), "+49", "zwo", a corrected group |
| dates_times | 18 | absolute and relative dates ("übermorgen", "nächste Woche Montag", "in zwei Wochen am Dienstag"), times ("halb acht", "Viertel vor sieben") |
| addresses | 6 | street, house number, postcode, made-up town |
| emails | 6 | spoken addresses ("punkt", "at", "minus", "unterstrich") |
| counts | 8 | persons, rooms, nights |
| short_answers | 10 | "Ja.", "Nein.", "Genau." … |
| backchannels | 4 | "Mhm.", "Ja, ja." (said over the voice in the scenarios) |
| barge_in | 4 | interruptions |
| silence | 3 | 2, 5 and 8 s of only the line (and the condition's noise) |

Speaking styles across them: plain, hesitation ("ähm", "äh"), a pause of
1.5–2.4 s mid-sentence (`<pause S>` in the text), self-correction, spelled,
numbers in groups, very short.

**Everything is fictional.** The hotel is `testset/de/facts.md`'s Hotel
Beispielhof. Phone numbers are only the Bundesnetzagentur's numbers for films
and books ("Drama Numbers", never assigned to anyone: 030 23125 xxx,
069 90009 xxx, 040 66969 xxx, 0221 4710 xxx, 089 99998 xxx and a list of
mobile numbers; bundesnetzagentur.de, "Rufnummern für Medienproduktionen"); a
unit test refuses any other. E-mail domains end in `.example` (RFC 2606),
towns are made up. Names are common or rare surnames with no person behind
them.

## Voices

`de/voices.yaml`: six designed voices and the hard one.

| voice | made by | description (short) | seed | tempo | reference take: pitch, pace |
|---|---|---|---|---|---|
| `f-young-fast` | Qwen3-TTS VoiceDesign | young woman, bright, quick and casual | 202 | 1.08 | 286 Hz, 3.8 words/s |
| `f-middle` | Qwen3-TTS VoiceDesign | woman in her forties, warm, normal pace | 101 | 1.0 | 211 Hz, 2.8 words/s |
| `f-older-slow` | Qwen3-TTS VoiceDesign | woman about 75, soft, slow | 303 | 0.92 | 218 Hz, 2.2 words/s |
| `m-young-fast` | Qwen3-TTS VoiceDesign | young man, light, fast, a little mumbled | 202 | 1.1 | 157 Hz, 2.9 words/s |
| `m-middle` | Qwen3-TTS VoiceDesign | man late forties, deep, measured | 101 | 1.0 | 89 Hz, 2.7 words/s |
| `m-older-slow` | Qwen3-TTS VoiceDesign | man about 80, gravelly, slow | 101 | 0.9 | 158 Hz, 1.9 words/s |
| `piper-mls` | Piper `de_DE-mls-medium` speaker 10 | the bench's old voice, garbles some lines | – | – | – |

Pitch (median F0 of the voiced frames, autocorrelation) and pace are measured
on the reference take before `tempo`. The seeds were chosen from three
candidates each (101, 202, 303; `render --try-seeds`): every candidate was
read back word for word by the judge, so the choice followed the description
(pitch for female/male and age, words per second for fast/slow).

How a designed voice is made: Qwen3-TTS 12Hz **1.7B VoiceDesign** (Apache-2.0)
speaks `reference_text` from the description in `instruct`, with torch seeded
to `seed`; that take is the voice's reference clip. Every caller line is then
spoken in that voice by Qwen3-TTS 12Hz **0.6B Base** (Apache-2.0, voice clone;
the same weights the `cascade-gpu-rocm` TTS uses), seeded per line. `tempo`
then speeds the voice up or down with ffmpeg's `atempo` (pitch kept), so
"fast" and "slow" do not depend on the take. No real person's voice is
recorded or cloned. Both models run on the CPU in the TTS server's own image
(`srw-voice-qwen-tts:1`, PyTorch from the models folder): the GPU stays with
the profile's servers. Memory is the limit, not the CPU: the 1.7B design
model in float32 needs about 9 GB and runs in one container capped at 12 GB,
each clone worker is capped at 7 GB, and the renderer starts no more workers
than the host's free memory allows with 6 GB to spare (`--workers`,
`--threads` set the most it may use).

Choosing a seed: `render --try-seeds 101,111,121 --voices m-middle` renders
candidate takes into `.cache/evalset/voices/candidates/` and prints each
take's pitch, words per second and what the judge understood; the chosen
seed goes into `voices.yaml`. CPU float results can differ slightly between
machines and thread counts, so a re-render elsewhere may give a slightly
different take; the render manifest records every clip's hash.

**Rendering checks itself:** with `--judge-url`, a piece of caller speech the
judge cannot read back (WER over 0.34) is rendered again with the next seed,
at most three times, and the best take is kept; the manifest records seed,
attempts and what the judge heard, and `render` lists the pieces that stayed
hard. Spelled letters and number strings are where a judge and a TTS most
often disagree.

## Phone conditions

`de/conditions.yaml`, applied after rendering (`conditions.py`), on 16 kHz:

| condition | chain |
|---|---|
| `clean` | none: the STT's ceiling |
| `g711a`, `g711u` | 8 kHz G.711 A-law / µ-law (numpy, bit-exact with the ITU reference tables) |
| `g722` | G.722 through ffmpeg (HD voice) |
| `mobile` | band 300–3400 Hz, AMR-NB 7.95 kbit/s (ffmpeg, libopencore), then A-law into the PBX |
| `loss-3`, `loss-8` | A-law, then 3 % / 8 % of the 20 ms packets lost (Gilbert-Elliott bursts of 1.5 / 3), gaps silent |
| `street` | generated traffic (rumble, passing cars) at 10 dB SNR, mobile leg |
| `cafe` | babble at 10 dB SNR plus cups, A-law |
| `office` | ventilation, keyboard, two far voices at 15 dB SNR, A-law |
| `quiet` | 16 dB down (speech at about −36 dBFS), A-law |
| `clipped` | 18 dB of gain clipped hard, A-law |

Clips are brought to −20 dBFS of active speech first. **Noise is generated**
(`conditions.py`: filtered and modulated noise, clicks, decaying tones), not
recorded, so no licence applies; the café babble mixes the bench's own
synthetic Piper sentences (`scripts/voice-bench.sh audio`) when they are
rendered, else a generated murmur. The SNR is measured on the caller's active
speech. Random parts are seeded by condition and clip, so every run hears the
same audio.

## Scenarios

`de/scenarios/*.yaml`, one call each (20 calls, 35 caller turns). Format:

```yaml
id: s03-thinking-pauses            # = file name
title: A caller who pauses while thinking
why: the §9.9 finding it guards
categories: [back_office, counts, dates_times]
voice: m-older-slow                # voices.yaml
condition: g711a                   # conditions.yaml
number: "107"                      # bench number (call level); 107 accepts a transfer
turns:
  - say: Wir sind fünf Personen <pause 2.4> und bräuchten zwei Zimmer …   # or utterance: <corpus id>, or silence: 6
    when: after_voice              # after_voice (default) | during_voice | after_caller
    offset: 0                      # during_voice: s after the voice started; after_caller: s after the last line
    expect:
      outcome: delegate            # answer | delegate | transfer | message | ignore | any
      values:
        - {number: 5, in: request}
        - {keyword: mittwoch, in: request}
      absent: []                   # values that must not appear
      barge_in: false              # a real interruption: the voice must stop
    back_office:                   # the canned answer to a delegation in this turn
      match: ["zimmer|personen"]   # keyword groups in what the caller said ("a|b"); none = any
      answer: Von Montag bis Mittwoch sind zwei Doppelzimmer frei …
      after: 6                     # seconds until the back office answers
      transfer: false              # then ask for the transfer to extension 3000
```

| scenario | guards |
|---|---|
| s01 facts | two fact answers, no delegation |
| s02 availability, booking | relative dates in the request, relayed price, a follow-up with a name |
| s03 thinking pauses | §9.9: a caller pausing 1.8–2.4 s mid-sentence is not cut off, "Mittwoch" reaches the request |
| s04 message | a spelled rare name and a grouped callback number reach the back office |
| s05 spelled lookup | "Przybilla" spelled, mobile line |
| s06 transfer | transfer to a named person over G.722 |
| s07 follow-up "Ja, genau." | a short answer is a turn; the confirmation is delegated again |
| s08 backchannels | "Mhm", "Ja, ja" over the voice are not turns |
| s09 barge-in | a correction mid-answer stops the voice; the answer follows the correction |
| s10 silence, street | street noise alone makes no turn; then a question is answered |
| s11 long wait | the back office takes 15 s: the longest silence the caller hears |
| s12 e-mail, office | a spoken address reaches the back office exactly |
| s13 address, café | street, number, postcode, town in babble |
| s14 street facts | facts from a noisy mobile |
| s15 self-correction | the corrected date goes on; the wrong one is never confirmed |
| s16 family | counts and "übermorgen" in one sentence |
| s17 quiet caller | a caller at −36 dBFS is still heard |
| s18 hard voice | the A1 call in the Piper voice |
| s19 number in pairs | a fast mobile caller's callback number in a message |
| s20 what day | does the voice know today's date (C1 puts it into the prompt)? |

## Real recordings

`human/` (gitignored) takes real recordings later: copy `human.example.yaml`
to `human/manifest.yaml`, put the files beside it, and `render` converts them;
the STT level scores them like the synthetic clips (under every condition,
or as they are with `phone: true`). Each needs the speaker's consent for this
use, a pseudonymous speaker id and the corpus's fictional numbers and names;
the folder is never committed.

## Baselines

`baselines/<profile>.json` is a run's `summary.json` (no audio, no
transcripts): per category, condition, voice, value kind and scenario, the
turn-taking counts, wording faults, stage-time medians and what the caller
heard. `run` diffs against it automatically: rates and word error rates move
when they change by more than 0.02, times by more than 10 % (50 ms at least);
counts of faults move at any change. Store a new baseline (`baseline RUN`)
only on purpose, with the run's `summary.md` in the commit message.

## Licences

| what | licence |
|---|---|
| Qwen3-TTS 12Hz 1.7B VoiceDesign, 0.6B Base (caller voices) | Apache-2.0 |
| Piper `de_DE-mls-medium` (hard voice), trained on Multilingual LibriSpeech | MIT; data CC BY 4.0 |
| Qwen3-ASR 0.6B GGUF (judge) | Apache-2.0 |
| ffmpeg (G.722, AMR-NB via libopencore-amrnb, atempo) | LGPL/GPL tools run as separate programs on the test host, not shipped |
| noise | generated by `conditions.py`, no third-party audio |
| texts, names, numbers | written for this set; phone numbers from the Bundesnetzagentur's public list of unassigned media numbers |

## Unit tests

`tests/voice_bench/test_voice_bench_evalset.py` runs in the normal suite
without audio, servers or Docker: the shipped files load and every reference
resolves; phone numbers are Drama Numbers and e-mail domains `.example`; every
expected value matches its own text (so a perfect transcript passes) and
every value the voice must relay is in the canned answer; the value matchers,
the conditions (G.711 reference points, SNR, loss rate and bursts,
determinism; G.722/AMR-NB when ffmpeg is installed), turn scoring,
aggregation, diff and Markdown.
