# Voice model servers (`cascade-cpu` test profile)

Local OpenAI-compatible servers for the voice gateway's cascade engine
(`src/voice_gateway/engines/cascade/`, design note `local_voice_gateway.md`
§4.3 and §6.2). For development and tests on one workstation; not a
deployment (that is WP5).

| Stage | Server | Model | Runs on |
|---|---|---|---|
| STT | Speaches 0.8.3 (MIT), faster-whisper | `Systran/faster-whisper-small` (MIT), int8 | CPU |
| TTS | `srw-voice-piper` (built here): Piper 2023.11.14-2 (MIT) | `de_DE-mls-medium` speaker 10 (MIT; trained from scratch on MLS German, CC BY 4.0) | CPU |
| Voice LLM | llama.cpp server b11459 (MIT), Vulkan | Gemma 4 12B it, QAT Q4_0 GGUF from `google/` (Apache-2.0) | GPU |
| VAD | in the gateway (Pipecat) | Silero VAD v6.2.3 (MIT) | CPU |

Smart Turn v3.2 (BSD-2) ships inside Pipecat and needs nothing here.

**Why these.** Speaches serves faster-whisper behind the OpenAI API.
Piper's `mls` voice is the German Piper voice cleared for commercial use;
the other German Piper voices (Thorsten and friends) are fine-tuned from the
non-commercial `lessac` checkpoint. Its 236 speakers differ a lot: the first
one reads at half speed, and Speaches 0.8.3 always takes the first (it also
labels Piper's 22.05 kHz WAV as 24 kHz and its resampling changes the
length). Hence the small `piper_server.py` (standard library, one
persistent `piper --json-input` process) with the speaker per request;
speaker 10 reads at a natural pace and did as well as any over G.711 in a
sweep with whisper small. Better German voices (Qwen3-TTS, Magpie) need a
GPU and are WP4.

## Run

```bash
# 1. Pinned models (sha256-checked) into .cache/voice-models (gitignored),
#    or VOICE_MODELS_DIR=/somewhere/with/space. Gemma is 7 GB.
python docker/voice-models/models.py fetch

# 2. Servers on 127.0.0.1:18450 (STT), :18451 (LLM), :18452 (TTS).
#    The TTS image is built locally on the first `up` and never pushed.
VOICE_LLM_RENDER_NODE=/dev/dri/renderD129 \
  docker compose -f docker/voice-models/compose.yaml up -d --build
```

- **Which render node:** `ls -l /dev/dri/by-path/`; pick the GPU's
  `renderD12x`. Passing all of `/dev/dri` makes llama.cpp split the model
  across every Vulkan device, an iGPU included.
- **SELinux** (Fedora): containers may not open `/dev/dri` devices
  (`container_use_devices` is off by default), so the `llm` service runs
  with `label=disable`. The others keep their labels and mount with `:z`.
- **No GPU:** `docker compose -f docker/voice-models/compose.yaml stop llm`,
  then `--profile cpu up -d llm-cpu`; set `VOICE_LLM_MODEL` to a smaller
  GGUF in the `llm/` folder for function tests.
- Thinking is off on the server (`--reasoning-budget 0`) and only there:
  with `chat_template_kwargs.enable_thinking: false` Gemma 4's template
  pre-fills an empty thought block and the model, a few turns into a call,
  emits nothing but more thought markers (llama.cpp b11459).
- Speaches runs offline (`HF_HUB_OFFLINE=1`) from the snapshots `models.py`
  pinned; it logs at `warning` because its debug log contains transcripts.

## Use

The profile is [`cascade-cpu.yaml`](cascade-cpu.yaml). Pipecat lives in the
gateway's own venv:

```bash
python3.13 -m venv .venv-voice
.venv-voice/bin/pip install -r src/voice_gateway/requirements.txt pytest pytest-asyncio
export PYTHONPATH=src

# One call without Asterisk: greeting, a fact, a delegation, a barge-in.
# Needs the test set's audio: scripts/voice-bench.sh audio
.venv-voice/bin/python scripts/voice_cascade_try.py \
    --profile-file docker/voice-models/cascade-cpu.yaml --asr-check

# Answer or delegate on utterances.yaml (text; --from-audio goes through STT).
.venv-voice/bin/python scripts/voice_cascade_eval.py \
    --profile-file docker/voice-models/cascade-cpu.yaml

# Unit tests (fake servers, no models):
.venv-voice/bin/python -m pytest --noconftest tests/test_voice_gateway_cascade*.py
```

Recordings and reports go to `.cache/voice-cascade-try/` and
`.cache/voice-cascade-eval/` (gitignored). Nothing stores audio in a real
call; only the harness writes what its fake line heard.
