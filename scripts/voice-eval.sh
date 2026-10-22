#!/usr/bin/env bash
# The voice system's end-to-end test set (tests/voice_bench/evalset/README.md).
#
#   scripts/voice-eval.sh list                       # what the set holds
#   scripts/voice-eval.sh render                     # caller voices and clips (CPU, cached)
#   scripts/voice-eval.sh judge up                   # Qwen3-ASR judge on the CPU, :18495
#   scripts/voice-eval.sh run --profile cascade-gpu-rocm --levels stt,file,call \
#       --judge-url http://127.0.0.1:18495/v1
#   scripts/voice-eval.sh diff RUN_DIR | compare RUN_A RUN_B | baseline RUN_DIR
#
# Needs the voice environment (Pipecat, numpy): .venv-voice at the repo root
# or SRW_VOICE_PYTHON; ffmpeg for G.722, AMR-NB and tempo; Docker for the
# renderer, the judge and the call level.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -n "${SRW_VOICE_PYTHON:-}" ]]; then
    python_bin="$SRW_VOICE_PYTHON"
elif [[ -x "$repo_root/.venv-voice/bin/python" ]]; then
    python_bin="$repo_root/.venv-voice/bin/python"
else
    echo "voice-eval: no voice environment; set SRW_VOICE_PYTHON or create .venv-voice" >&2
    exit 2
fi

cd "$repo_root"
export PYTHONPATH="$repo_root/src${PYTHONPATH:+:$PYTHONPATH}"
exec "$python_bin" "$repo_root/scripts/voice_eval.py" "$@"
