#!/usr/bin/env bash
#
# Drive and observe the stateless execution lane on the local k3d cluster.
#
# These are the harnesses the S1 performance session used to turn "it feels
# slow" into per-phase numbers, and to prove the steal path. They exist because
# every measurement on this lane needs the same three things: a fresh auth
# token (they expire in ~15 min and fail as a silent 401), a way to tell when a
# turn actually landed (poll thread_messages, NOT the API — a queued turn has
# no pod), and the timing lines from both planes.
#
# Usage:
#   scripts/stateless-lane-probe.sh turn  "<message>"   [timeout_s]
#   scripts/stateless-lane-probe.sh burst <n>           [timeout_s]
#   scripts/stateless-lane-probe.sh kill  [timeout_s]
#
#   STATELESS_THREAD=<uuid>   thread to drive (must be on execution_lane='stateless')
#
# Find a lane thread:
#   kubectl --context=k3d-srw -n srw exec srw-postgres-0 -- psql -U srw -d srw \
#     -tAc "SELECT id, title FROM threads WHERE execution_lane='stateless'"
#
set -euo pipefail

CTX="--context=k3d-srw"
NS="-n srw"
THREAD="${STATELESS_THREAD:-}"

if [[ -z "$THREAD" ]]; then
    THREAD=$(kubectl $CTX $NS exec srw-postgres-0 -- psql -U srw -d srw -tAc \
        "SELECT id FROM threads WHERE execution_lane='stateless' ORDER BY created_at DESC LIMIT 1" \
        2>/dev/null | tr -d ' \r')
fi
if [[ -z "$THREAD" ]]; then
    echo "No stateless-lane thread found. Set STATELESS_THREAD=<uuid>, or flip a" >&2
    echo "DETACHED thread: UPDATE threads SET execution_lane='stateless'" >&2
    echo "WHERE id=... AND agent_id IS NULL;" >&2
    exit 1
fi

# The gates' Keycloak client mints the access token (scripts/k3d_gate_oauth.py):
# it carries `sub` and the audience the API's Bearer check accepts. An ID
# token, or admin-cli's lightweight access token, is refused with 401.
# SRW_K3D_TEST_USER / SRW_K3D_TEST_PASSWORD pick the account (default test).
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mint_token() {
    KUBE_CONTEXT=k3d-srw KUBE_NAMESPACE=srw python3 "$ROOT/scripts/k3d_gate_oauth.py"
}

max_seq() {
    kubectl $CTX $NS exec srw-postgres-0 -- psql -U srw -d srw -tAc \
        "SELECT COALESCE(MAX(seq),0) FROM thread_messages WHERE thread_id='$THREAD'" \
        2>/dev/null | tr -d ' \r'
}

# role is the LangChain vocabulary ('human'/'ai'), NOT 'user'/'assistant'.
ai_rows_since() {
    kubectl $CTX $NS exec srw-postgres-0 -- psql -U srw -d srw -tAc \
        "SELECT count(*) FROM thread_messages WHERE thread_id='$THREAD'
         AND role='ai' AND seq > $1" 2>/dev/null | tr -d ' \r'
}

post_input() {
    local token="$1" content="$2"
    kubectl $CTX $NS exec deploy/srw-orchestrator -c orchestrator -- \
        curl -s -X POST "http://localhost:8085/api/persistent/threads/$THREAD/input" \
        -H "Authorization: Bearer $token" -H "Content-Type: application/json" \
        -d "$(python3 -c 'import json,sys; print(json.dumps({"content": sys.argv[1]}))' "$content")"
}

timings() {
    local since="$1"
    echo "--- agent ---"
    kubectl $CTX $NS logs -l app=srw-agent-stateless --since="${since}s" --tail=400 --prefix 2>/dev/null \
        | grep -E "turn timing:|setup steps:|attach step:|pull detail|push detail|session reuse|affinity miss|run_queue (claim|complete|release):|lease lost" || true
    echo "--- orchestrator ---"
    kubectl $CTX $NS logs deploy/srw-orchestrator -c orchestrator --since="${since}s" --tail=400 2>/dev/null \
        | grep -E "claim-bundle timing:|run_queue (enqueue|reaper)" || true
}

cmd_turn() {
    local content="${1:?message required}" timeout="${2:-300}"
    local token before t0
    token=$(mint_token); before=$(max_seq); t0=$(date +%s)
    echo ">> $(post_input "$token" "$content")"
    for _ in $(seq 1 "$timeout"); do
        [[ "$(ai_rows_since "$before")" -ge 1 ]] && break
        sleep 2
    done
    echo "=== answered in $(( $(date +%s) - t0 ))s ==="
    timings $(( $(date +%s) - t0 + 10 ))
}

cmd_burst() {
    local n="${1:-3}" timeout="${2:-300}"
    local token before t0
    token=$(mint_token); before=$(max_seq); t0=$(date +%s)
    for i in $(seq 1 "$n"); do
        post_input "$token" "Burst $i of $n: reply with exactly BURST-$i and nothing else." >/dev/null
    done
    echo ">> posted $n"
    for _ in $(seq 1 "$timeout"); do
        [[ "$(ai_rows_since "$before")" -ge "$n" ]] && break
        sleep 2
    done
    echo "=== $n answered in $(( $(date +%s) - t0 ))s ==="
    # FIFO check: the ai rows must appear in the order the inputs were posted.
    kubectl $CTX $NS exec srw-postgres-0 -- psql -U srw -d srw -tAc \
        "SELECT seq || ' ' || role || ': ' || left(replace(content, E'\n',' '), 50)
         FROM thread_messages WHERE thread_id='$THREAD' AND seq > $before ORDER BY seq"
    timings $(( $(date +%s) - t0 + 10 ))
}

# Fault injection: kill the serving pod mid-turn and prove the reaper steals the
# lease and the answer is regenerated EXACTLY ONCE.
#
# Two traps this encodes:
#  * `kubectl exec ... kill -9 1` does NOTHING — a container PID 1 with no
#    handler is protected from in-namespace fatal signals. Delete the pod.
#  * turns are ~3-5s now, so a short prompt finishes before the kill lands.
#    Ask for something long and kill the instant the claim appears.
cmd_kill() {
    local timeout="${1:-240}"
    local marker="KILLTEST-$(date +%H%M%S)"
    local token before t0 server
    token=$(mint_token); before=$(max_seq); t0=$(date +%s)
    post_input "$token" "Write a detailed 60-line poem about distributed leases, fencing tokens and reapers. Take your time. End the final line with the exact token $marker" >/dev/null
    for _ in $(seq 1 60); do
        server=$(kubectl $CTX $NS exec srw-postgres-0 -- psql -U srw -d srw -tAc \
            "SELECT COALESCE(leased_by,'') FROM run_queue
             WHERE unit_id='$THREAD' AND state='leased'" 2>/dev/null | tr -d ' \r')
        [[ -n "$server" ]] && break
        sleep 1
    done
    [[ -n "$server" ]] || { echo "never claimed"; exit 1; }
    echo ">> claimed by $server at t+$(( $(date +%s) - t0 ))s; killing"
    kubectl $CTX $NS delete pod "$server" --force --grace-period=0 --wait=false 2>/dev/null || true
    local stolen=""
    for _ in $(seq 1 "$timeout"); do
        local row state
        row=$(kubectl $CTX $NS exec srw-postgres-0 -- psql -U srw -d srw -tAc \
            "SELECT state || '|' || COALESCE(last_leased_by,'NULL') || '|' || lease_token
             FROM run_queue WHERE unit_id='$THREAD'" 2>/dev/null | tr -d ' \r')
        state="${row%%|*}"
        if [[ "$state" != "leased" && -z "$stolen" ]]; then
            stolen=$(( $(date +%s) - t0 )); echo ">> stolen at t+${stolen}s -> $row"
        fi
        [[ "$(kubectl $CTX $NS exec srw-postgres-0 -- psql -U srw -d srw -tAc \
            "SELECT count(*) FROM thread_messages WHERE thread_id='$THREAD'
             AND role='ai' AND seq > $before AND content LIKE '%$marker%'" \
            2>/dev/null | tr -d ' \r')" -ge 1 ]] && break
        sleep 2
    done
    echo ">> settling 30s to catch a duplicate regeneration"
    sleep 30
    echo "=== marker answers (MUST be 1): $(kubectl $CTX $NS exec srw-postgres-0 -- psql -U srw -d srw -tAc \
        "SELECT count(*) FROM thread_messages WHERE thread_id='$THREAD'
         AND role='ai' AND seq > $before AND content LIKE '%$marker%'" | tr -d ' \r') ==="
    kubectl $CTX $NS exec srw-postgres-0 -- psql -U srw -d srw -tAc \
        "SELECT 'queue: ' || state || ' consumed=' || COALESCE(consumed_seq::text,'-')
         || ' input=' || COALESCE(input_seq::text,'-') || ' token=' || lease_token
         || ' attempts=' || attempts_since_completion FROM run_queue WHERE unit_id='$THREAD'"
    kubectl $CTX $NS exec srw-postgres-0 -- psql -U srw -d srw -tAc \
        "SELECT 'events_epoch: ' || events_epoch FROM threads WHERE id='$THREAD'"
}

case "${1:-}" in
    turn)  shift; cmd_turn "$@" ;;
    burst) shift; cmd_burst "$@" ;;
    kill)  shift; cmd_kill "$@" ;;
    *) sed -n '3,25p' "$0"; exit 1 ;;
esac
