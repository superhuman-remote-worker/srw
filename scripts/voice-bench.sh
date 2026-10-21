#!/usr/bin/env bash
# WP0 phone test bench for SRW's local voice gateway.
#
#   scripts/voice-bench.sh run                       # every profile vs. the stand-in DUT
#   scripts/voice-bench.sh run --skip-slow           # without session-timer
#   scripts/voice-bench.sh run --profile dtmf-info,refer-accept
#   scripts/voice-bench.sh run --dut external --dut-peer 172.30.86.50   # a real device (WP2)
#   scripts/voice-bench.sh list | models | audio | build | standin up|down | down
#
# Needs Docker with Compose v2+ and Python 3.10+ with PyYAML (the repo venv).
# Report: tests/voice_bench/results/<UTC time>/report.json; exit code 1 when
# a profile fails. Details: tests/voice_bench/README.md.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -n "${SRW_PYTHON:-}" ]]; then
    python_bin="$SRW_PYTHON"
elif [[ -x "$repo_root/.venv/bin/python" ]]; then
    python_bin="$repo_root/.venv/bin/python"
else
    python_bin="python3"
fi

cd "$repo_root"
exec "$python_bin" -m tests.voice_bench.bench "$@"
