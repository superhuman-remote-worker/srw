#!/bin/sh
# Render /audio/jobs.jsonl (one {"text", "speaker_id", "output_file"} object
# per line, written by tests/voice_bench/render_audio.py) with Piper, then
# derive the bench formats with sox:
#   <name>.16k.wav       16 kHz mono 16-bit PCM (bench playback, wideband STT)
#   <name>.8k-alaw.wav   8 kHz mono G.711 A-law (phone-quality test set)
# Output files named in the jobs are <name>.raw.wav (22.05 kHz, deleted after).
set -eu
model="${PIPER_MODEL:-/models/de_DE-mls-medium.onnx}"
jobs="${1:-/audio/jobs.jsonl}"
/opt/piper/piper --model "$model" --json-input --quiet < "$jobs"
for raw in $(sed -n 's/.*"output_file": *"\([^"]*\)".*/\1/p' "$jobs"); do
    base="${raw%.raw.wav}"
    # 0.4 s lead-in and 0.8 s tail of silence so looped playback has gaps.
    sox -q "$raw" -r 16000 -c 1 -b 16 -e signed-integer "$base.16k.wav" \
        pad 0.4 0.8 norm -3
    sox -q "$base.16k.wav" -r 8000 -c 1 -e a-law "$base.8k-alaw.wav"
    rm -f "$raw"
done
