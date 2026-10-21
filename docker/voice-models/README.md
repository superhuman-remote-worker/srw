# Voice model servers (`cascade-cpu`, `cascade-gpu`, `cascade-gpu-rocm` test profiles)

Local OpenAI-compatible servers for the voice gateway's cascade engine
(`src/voice_gateway/engines/cascade/`, design note `local_voice_gateway.md`
§4.3 and §6.2). For development and tests on one workstation; not a
deployment (that is WP5).

| Stage | `cascade-cpu` | `cascade-gpu` (WP4, any Vulkan GPU) | `cascade-gpu-rocm` (WP4, AMD with ROCm) |
|---|---|---|---|
| STT | Speaches 0.8.3 + faster-whisper small int8, CPU | whisper.cpp 1.9.5 server (`srw-voice-whisper-vulkan`, built here) + whisper-large-v3-turbo q8_0, Vulkan, beam 5 | same as `cascade-gpu` |
| Voice LLM | llama.cpp b11783 server, Vulkan: Gemma 4 12B it QAT Q4_0 (shared by all) | same | same |
| TTS | `srw-voice-piper` (built here): Piper `de_DE-mls-medium` speaker 10, CPU | llama.cpp b11783 server with Qwen3-TTS 12Hz 1.7B Base Q8_0, Vulkan, behind `srw-voice-audio-adapter` (built here) | `srw-voice-qwen-tts` (built here): faster-qwen3-tts with Qwen3-TTS 12Hz 0.6B Base on PyTorch 2.14.1 ROCm 7.2, streamed |
| VAD, turn | in the gateway: Silero VAD v6.2.3, Smart Turn v3.2 (Pipecat), CPU | same | same |

Both GPU voices speak as `de-1` from [`voices/`](voices/README.md).
**On the RX 9070 XT `cascade-gpu-rocm` is the one to demo:** its first audio
comes after ~0.4 s instead of the whole sentence. `cascade-gpu` runs on any
Vulkan GPU (Intel, AMD without ROCm, NVIDIA) with small images.

The bench's **judge** (`--profile judge`): Qwen3-ASR 0.6B Q8_0 on llama.cpp
(Vulkan) behind the same adapter. It writes down what a caller would
understand of the voice; it is another model family than the STT it judges.

## Licences (code and weights; all allow commercial use)

| Component | Code | Weights |
|---|---|---|
| Speaches 0.8.3 (image) | MIT | faster-whisper small: MIT (OpenAI Whisper, CTranslate2 conversion by Systran) |
| Piper 2023.11.14-2 | MIT | `de_DE-mls-medium`: MIT, trained from scratch on MLS German (CC BY 4.0) |
| llama.cpp b11783 (`ghcr.io/ggml-org/llama.cpp:server-vulkan`) | MIT | Gemma 4 12B: Apache-2.0 |
| whisper.cpp 1.9.5 | MIT | whisper-large-v3-turbo (ggml, ggerganov/whisper.cpp): MIT |
| Qwen3-TTS 12Hz 1.7B Base (GGUF by ggml-org) and 0.6B Base | — | Apache-2.0 (models and speech tokenizer) |
| PyTorch 2.14.1 ROCm 7.2 wheel (bundles ROCm libraries) | BSD-3-Clause and permissive third-party licences | — |
| faster-qwen3-tts 0.5.4 | MIT | — |
| qwen-tts-hf 0.1.1.post1 (Qwen3-TTS's package for Transformers 5) | Apache-2.0 | — |
| Transformers 5.15.1, Accelerate 1.12.0 and the other pinned packages of `qwen-tts-rocm.requirements.txt` | Apache-2.0, BSD, MIT, ISC | — |
| Qwen3-ASR 0.6B (GGUF by ggml-org), judge only | — | Apache-2.0 |
| `audio_adapter.py`, `piper_server.py`, `qwen_tts_server.py`, `qwen_tts_stubs/` (here) | this repository | — |
| Silero VAD v6.2.3 | MIT | MIT |
| Smart Turn v3.2 (in Pipecat 1.12.0, BSD-2) | BSD-2 | BSD-2 |
| Voice `de-1` | — | made by Qwen3-TTS itself, no real person's voice ([voices/](voices/README.md)) |

Compared in WP4 and not shipped: Chatterbox Multilingual 0.1.7 (MIT code
and weights, Perth watermark on every output).

## Run

```bash
# 1. Pinned models (sha256-checked) into .cache/voice-models (gitignored),
#    or VOICE_MODELS_DIR=/somewhere/with/space. Gemma is 7 GB; the gpu set
#    adds 3.2 GB, the judge 1 GB.
python docker/voice-models/models.py fetch                    # cascade-cpu
python docker/voice-models/models.py fetch --set gpu,judge    # cascade-gpu, judge
python docker/voice-models/models.py fetch --set gpu,rocm     # cascade-gpu-rocm:
#    + the 0.6B model, the 6.2 GB PyTorch ROCm wheel, and qwen-tts/runtime
#    (~15 GB), where pip in a pinned Python image installs the wheel and
#    the pinned packages; the server image itself stays small.

# 2a. cascade-cpu on 127.0.0.1:18450 (STT), :18451 (LLM), :18452 (TTS).
VOICE_LLM_RENDER_NODE=/dev/dri/renderD129 \
  docker compose -f docker/voice-models/compose.yaml up -d --build

# 2b. cascade-gpu: the LLM plus :18453 (STT) and :18454 (TTS).
VOICE_LLM_RENDER_NODE=/dev/dri/renderD129 \
  docker compose -f docker/voice-models/compose.yaml --profile gpu \
  up -d --build llm speech-gpu tts-gpu

# 2c. cascade-gpu-rocm: the LLM, the STT and :18456 (TTS). The TTS needs
#     /dev/kfd and the host's render group (VOICE_RENDER_GID, default 105).
VOICE_LLM_RENDER_NODE=/dev/dri/renderD129 VOICE_RENDER_GID=$(getent group render | cut -d: -f3) \
  docker compose -f docker/voice-models/compose.yaml --profile gpu --profile gpu-rocm \
  up -d --build llm speech-gpu tts-gpu-rocm

# 2d. The judge on :18455, for the benchmarks (stop it for latency runs on
#     a 16 GB card: it does not fit beside the three others and a desktop).
docker compose -f docker/voice-models/compose.yaml --profile judge up -d --build judge
```

The images built here (`srw-voice-piper`, `srw-voice-whisper-vulkan`,
`srw-voice-audio-adapter`, `srw-voice-qwen-tts`) stay local and are never
pushed.

- **Which render node:** `ls -l /dev/dri/by-path/`; pick the GPU's
  `renderD12x`. Passing all of `/dev/dri` makes llama.cpp split the model
  across every Vulkan device, an iGPU included.
- **SELinux** (Fedora): containers may not open `/dev/dri` devices
  (`container_use_devices` is off by default), so the GPU services run with
  `label=disable`. The others keep their labels and mount with `:z`.
- **No GPU:** `docker compose -f docker/voice-models/compose.yaml stop llm`,
  then `--profile cpu up -d llm-cpu`; set `VOICE_LLM_MODEL` to a smaller
  GGUF in the `llm/` folder for function tests.
- **Thinking** is off on the server (`--reasoning-budget 0`) and only there:
  with `chat_template_kwargs.enable_thinking: false` Gemma 4's template
  pre-fills an empty thought block and the model, a few turns into a call,
  emits nothing but more thought markers. Now and then Gemma still writes
  channel markup into a reply; the engine drops it before the TTS.
- **Prefix cache (WP4):** Gemma 4's sliding-window layers kept llama.cpp
  from reusing the cached prompt except around its window checkpoints: each
  turn's ~50 new tokens took ~0.45 s, first token ~0.5 s. The `llm` service
  runs `--swa-full` (window cache as long as the context) with an 8-bit KV
  cache and flash attention: first token ~0.12-0.15 s, for ~0.9 GB more
  than the default at the same context.
- **ROCm** on gfx1201 (RX 9070 XT) works with PyTorch's own ROCm 7.2
  wheel (fp16 matmul ~130 TFLOPS). MIOpen's exhaustive kernel search made
  the first sentences take 10x as long as they last; the image sets
  `MIOPEN_FIND_MODE=FAST`. The tmpfs `/tmp` holds the compiled GPU code and
  MIOpen's database (no audio), so the first sentence after a start is
  slower. Qwen3-TTS's package imports sox, onnxruntime and torchaudio for
  its 25 Hz tokenizer, which the 12 Hz models never use, and there is no
  ROCm torchaudio for torch 2.14: `qwen_tts_stubs/` stands in for them.
- **No audio on disk (A5):** the GPU servers run with a read-only root and
  a tmpfs `/tmp`; whisper.cpp decodes uploads in memory (no `--convert`),
  llama.cpp, the adapter and the PyTorch TTS keep audio in memory. Speaches spills uploads
  over 1 MB to its tmpfs `/tmp`; Piper writes each utterance to `/dev/shm`.
- Speaches runs offline (`HF_HUB_OFFLINE=1`) from the snapshots `models.py`
  pinned; it logs at `warning` because its debug log contains transcripts.
  The GPU servers log no transcripts or texts.

## What fits on which card

GPU memory with all three servers loaded, RX 9070 XT (16 GB): 13.8 GB
for `cascade-gpu`, 14.6 GB for `cascade-gpu-rocm`.

| Server | GPU memory |
|---|---|
| Voice LLM, Gemma 4 12B Q4_0, 12k context, `--swa-full`, q8_0 KV | 9.4 GB (16k f16 window cache: 12.8 GB; default cache: 8.3 GB) |
| TTS, Qwen3-TTS 1.7B Q8_0 + tokenizer on llama.cpp, one slot | 3.2 GB |
| TTS, Qwen3-TTS 0.6B on PyTorch ROCm (with its graphs and caches) | 4.0 GB |
| STT, whisper-large-v3-turbo q8_0 | 1.2 GB |
| Judge, Qwen3-ASR 0.6B Q8_0 (benchmarks only) | 1.4 GB |

So a 16 GB card holds the cascade for one call at a time; a desktop on the
same card leaves no room for the judge. More calls need more LLM context
(`VOICE_LLM_SLOTS`, `VOICE_LLM_CONTEXT`) and TTS slots (`VOICE_TTS_SLOTS`),
i.e. the bigger cards of design note §2.3.

## How they compare (WP4, RX 9070 XT, 2026-10-21)

German voice after an 8 kHz G.711 A-law leg, judged by Qwen3-ASR 0.6B (33
test-set lines and the A1 call's 9 answers; word error rate with numbers
compared as words):

| Voice | WER | lines word for word | first audio | real-time factor |
|---|---|---|---|---|
| Piper `mls` speaker 10 (CPU) | 0.33 | 10 of 42 | 0.09 s | 0.02 |
| Qwen3-TTS 1.7B on llama.cpp, voice `de-1` (Vulkan) | 0.015 | 39 of 42 | 1.8 s (≈0.67 s + 0.33 s per second of speech) | 0.52 |
| Qwen3-TTS 0.6B on PyTorch ROCm, streamed, voice `de-1` | 0.006 | 40 of 42 | 0.43 s (the judge shared the GPU; ~0.36 s alone) | 0.55 |
| Chatterbox Multilingual 0.1.7, cloning `de-1` (CPU, PyTorch) | 0.057 | 31 of 42 | whole sentence | 3.5 |

A1's fact answer "Der Check-out ist bis 11 Uhr möglich." comes out as "dass
das so möglich" with Piper and word for word with Qwen3-TTS and Chatterbox.
Chatterbox now and then trails off into babble after a sentence ("… im
Zimmer schläft? Ach, hält") or repeats a phrase, and is no faster on the
CPU than real time; Qwen3-TTS won, and on this card streamed from
PyTorch.

**llama.cpp's Qwen3-TTS streams in steps of the speech decoder's window**
(several seconds of audio), so a sentence's audio arrives when the sentence
is done: the TTS's first audio is the synthesis time of the first piece.
faster-qwen3-tts yields every 8 codec frames (0.67 s of speech). For both,
short pieces help: the engine starts a long first sentence at a clause
(`tts.clause_chars`), sends the greeting sentence by sentence, and keeps
the audio of short recurring texts (filler, greeting, the holding
sentences) in memory, so a delegated turn's "Einen Moment, ich schaue
nach." plays at once (`tts.cache_phrases`). On a CUDA card the same
`qwen_tts_server.py` with PyTorch's CUDA build should get near the ~0.1 s
first audio the model is known for (not measured here).

End of the caller's speech to the reply's first audio, per stage
(`voice_cascade_try.py`, the caller's lines in a male Qwen3-TTS voice so
that Smart Turn hears natural German; medians of three runs, ms):

| Turn, profile | turn wait | STT | LLM first token | LLM to first TTS text | TTS first audio | total |
|---|---|---|---|---|---|---|
| fact, `cascade-cpu` | 1029 | 828 | 155 | 393 | 66 | 1490 |
| fact, `cascade-gpu` | 506 | 304 | 153 | 401 | 1367 | 2291 |
| fact, `cascade-gpu-rocm` | 484 | 284 | 140 | 355 | 380 | 1220 |
| delegated, `cascade-cpu` | 1146 | 945 | 159 | 919 | 0 (cached) | 2091 |
| delegated, `cascade-gpu` | 542 | 340 | 168 | 992 | 0 (cached) | 1520 |
| delegated, `cascade-gpu-rocm` | 547 | 347 | 139 | 892 | 0 (cached) | 1440 |
| transfer request, `cascade-cpu` | 954 | 752 | 141 | 751 | 0 (cached) | 1708 |
| transfer request, `cascade-gpu` | 470 | 269 | 167 | 827 | 0 (cached) | 1282 |
| transfer request, `cascade-gpu-rocm` | 470 | 270 | 142 | 735 | 0 (cached) | 1206 |

The turn wait (end of speech until the turn goes to the LLM) holds 200 ms
of VAD silence and the STT request. With the bench's Piper caller clips
Smart Turn often waits its full 3 s before it lets a turn go: Piper's
sentence ends do not sound final.

STT on the test set's phone clips (WER against the text; time per
utterance):

| STT | bench clips (Piper voice) | the same lines in a Qwen3-TTS voice |
|---|---|---|
| faster-whisper small, CPU | 0.31, 0.95 s | 0.013, 0.90 s |
| whisper-large-v3-turbo, Vulkan, beam 5 | 0.30, 0.34 s | 0.015, 0.34 s |
| Qwen3-ASR 0.6B (the judge), Vulkan | 0.35, 0.16 s | — |

The bench clips' errors are mostly the Piper voice's: the judge does no
better on them. The GPU's gain is the time.

## Use

The profiles are [`cascade-cpu.yaml`](cascade-cpu.yaml) and
[`cascade-gpu.yaml`](cascade-gpu.yaml) (with `cascade-gpu-rocm`). Pipecat lives in the gateway's own
venv:

```bash
python3.13 -m venv .venv-voice
.venv-voice/bin/pip install -r src/voice_gateway/requirements.txt pytest pytest-asyncio
export PYTHONPATH=src

# One call without Asterisk: greeting, a fact, a delegation, a transfer
# request, with per-stage timings. Needs the test set's audio:
# scripts/voice-bench.sh audio
.venv-voice/bin/python scripts/voice_cascade_try.py \
    --profile-file docker/voice-models/cascade-gpu.yaml --profile cascade-gpu \
    --turns a02-checkout,d03-invoice-copy,t01-human \
    --asr-check --judge-url http://127.0.0.1:18455/v1

# Answer or delegate, and the wording rules, on utterances.yaml (text;
# --from-audio goes through STT).
.venv-voice/bin/python scripts/voice_cascade_eval.py \
    --profile-file docker/voice-models/cascade-gpu.yaml --profile cascade-gpu

# The TTS and STT on their own: WER through G.711, first audio, RTF.
.venv-voice/bin/python scripts/voice_models_bench.py tts \
    --tts-url http://127.0.0.1:18454/v1 --model qwen3-tts --voice de-1 \
    --judge-url http://127.0.0.1:18455/v1 --label qwen3-tts
.venv-voice/bin/python scripts/voice_models_bench.py stt \
    --stt-url http://127.0.0.1:18453/v1 --model whisper-large-v3-turbo --label turbo

# Unit tests (fake servers, no models):
.venv-voice/bin/python -m pytest --noconftest tests/test_voice_gateway_cascade*.py
```

Recordings and reports go to `.cache/voice-cascade-try/`,
`.cache/voice-cascade-eval/` and `.cache/voice-models-bench/` (gitignored);
they hold synthetic speech only. Nothing stores audio in a real call.
