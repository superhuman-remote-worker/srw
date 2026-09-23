# Stateless turn settlement after mid-turn compaction — validation ledger

Change: a stateless session turn that auto-compacts mid-turn used to crash at
turn end (`authoritative stateless turn lacks an exact input message id`), park
its `run_queue` unit and leave the cockpit on "generating" forever. Fix (all
in-tree, 2026-09-05): identity-preserving compaction for sessions, turn
membership stamps instead of an anchor walk, bounded settlement retry, a
transcript leg for skip-if-answered, the live request pinned through a summary,
`turn.error` on loop crash, and three cockpit corrections. Design + evidence:
`knowledge-base/knowledge/issues/stateless_turn_settlement_crashes_after_midturn_compaction.md`.

This file is the ledger of what is proven and what is still owed before the
change can be called done in production. Tick items off in place.

## 1. Proven

| what | how | result |
|---|---|---|
| kept window keeps objects/ids/stamps; pinned input re-seated as the original; default mode unchanged | `tests/test_context_safety.py::TestPreserveMessageIdentity` | green |
| reconcile selects by stamp after the input was summarised away; unstamped history without an anchor saves zero rows instead of raising | `tests/test_persistent_app.py` (`…selects_by_membership_after_compaction`, `…survives_evicted_input`, `…without_anchor_saves_nothing`) | green |
| settlement retry: transient → retried, contract error / lost lease → not retried, exhausted → propagates, pinned → best effort | `tests/test_persistent_app.py::TestReconcileTurnWithRetry` | green |
| loop crash emits `turn.error` before terminate, not on `LeaseLostError` | `tests/test_persistent_app.py::TestLoopCrashTerminalEdge` | green |
| every message a turn appends carries its stamp and survives eviction of the input | `tests/test_persistent_graph.py::…stamped_and_survive_input_eviction`, `tests/test_persistent_turn_membership.py` | green |
| composed (real loop + real identity-preserving `ContextManager` summarising mid-turn + incremental persist + authoritative reconcile, in-memory transcript): settles, effect `end_seq` = final answer, no fresh-id rows, and no reconciled row rewrites a durable row's content — a keep-window-capped / elided / shed copy is a marked compaction view the reconcile writes `ON CONFLICT (id) DO NOTHING`: it never overwrites the full durable row, and still fills a row whose incremental write was lost (no tool call left unpaired) (2026-09-22) | `tests/test_stateless_midturn_compaction_settlement.py`, `tests/test_batch_thread_messages.py::test_compaction_view_fills_a_lost_row_but_never_rewrites_a_landed_one` (real Postgres), `tests/test_postgres_db_save_message.py::test_batch_insert_if_absent_rows_never_update_and_keep_row_order`, `tests/test_context_safety.py::TestPreserveMessageIdentity` | green |
| executor: transcript leg completes an answered-but-unsettled input without attach; unanswered / pending-event keep the ordinary path | `tests/test_turn_executor.py::TestSkipIfAnswered::test_transcript_*` | green |
| cockpit: durable snapshot with `turn_in_flight=false` closes a retained turn; silence clock ignores `session.state` and stream reopens; replayed `turn_started` rebuilds the live turn | `cockpit/src/app/core/services/{turn-reducer,persistent-chat.service}.spec.ts` | 384 green, `tsc -p tsconfig.app.json` clean |
| no regression across the wider tree | `./scripts/pytest-fast.sh` 2026-09-05 | 22 743 passed, 125 skipped, **1 failed: `test_mcp_manager::test_connect_discover_call_close`** — its stdio echo server never starts on this host (fails standalone, unrelated) |
| k3d, new image (`srw-agent:tilt-4ea34be…`): an ordinary 10-tool-call stateless turn settles — `turn.completed`, no duplicate `tool_call_id`/ai rows, final "DONE" | thread `f2b47a86`, gemma-4-moe via `ai.h4ll.app` | pass (regression only — no compaction fired, see 2.1) |
| k3d: loop crash now leaves a terminal edge — journal `turn.started → turn.error`, `role='error'` row, unit released not parked | thread `81c9ba06` (crashed on k3d's own "cloud sync degraded; tool work refused" gate) | pass (incidental) |
| **k3d, 2.1 live (2026-09-23): mid-turn auto-compaction on the stateless lane settles.** MiniMax-M3 `context_window` 524288 → 26000 (reverted after). Six ~1000-word essay turns filled the history, then one 8-call write_file/read_file turn (T7) crossed the threshold mid-turn: agent log `Context compaction triggered` → `Compacted 16 messages to 12` ×2 → `Re-seated 1 protected message(s) after the summary` → `Compacted 22 messages to 13`; no `Persistent loop crashed`, no park. `thread_events`: `compaction.started`/`context.compacted` ×3 then `turn.completed` for turn 7, no `turn.error`. `run_queue`: `done`, `consumed_seq = input_seq = 825`. `thread_messages`: 3 `summary` rows for turn 7 (one per compaction), no duplicate `tool_call_id`, no duplicate ai rows, 0 `role='tool'` rows containing `[tool result truncated by compaction`. Next turn (T8) settled; its request was `[system, summary, T7 input as ordinary history, …, T8 input, turn-boundary guide]` (pin came off, no double re-seat). T8's "empty response" is the MiniMax think-leak (answer inside `reasoning_content`), not compaction. | thread `37a03704-bcb2-4160-bef3-28c9950d36b7`, MiniMax-M3, agent image with `f18040698` + `7164379c9` | **pass** |
| k3d negative (2026-09-23): when the summarisable region is smaller than its summary (keep window is COUNT-based — 10 messages — so large recent messages stay), compaction logs `Summary (N tokens) larger than original — skipping compaction`, the provider 413s (`context_overflow`), the turn records a `role='error'` row and the unit settles `done` — loud failure, no crash/park/hang. There is no trim fallback in that case — open finding, tracked in the vault note `knowledge-base/knowledge/issues/compaction_count_based_keep_window_cannot_free_space.md`. | threads `edc40075` (window 13000), `0cf6e6a0` (window 20000) | pass (failure is loud) |

## 2. Still owed

### 2.1 THE scenario, live: mid-turn auto-compaction on the stateless lane settles

**DONE 2026-09-23 — see §1 (thread `37a03704`).** Recipe note: MiniMax-M3's system prompt is ~8.7k tokens; windows of 13000/20000 leave too little summarisable history (compaction skips, 413). What worked: window 26000, six ~1000-word essay turns, then an 8-call tool turn. Original text follows.

Not yet exercised on a cluster. The thread-create API rebuilds `config_override`
from validated fragments (model / temperature / reasoning / permission mode /
workspace / tool groups) — `limits.*` and `context_management.*` are dropped, so
thresholds cannot come from the request. The lever that IS honoured at dispatch
is the model's `context_window` (Admin → Models); per-call input on a fresh
session is ≈11k tokens, so a 13 000-token window (threshold 10 400) forces
compaction from the second provider call on.

Recipe (k3d; scripts from the 2026-09-05 session are reproduced below):

1. `UPDATE models SET context_window=13000 WHERE model_id='gemma-4-moe';`
   (k3d value was `131072` — **revert afterwards**).
2. Create a session through the REST API as `test` (token recipe:
   `reference_k3d_api_smoke_auth`; `POST /api/persistent/threads` with
   `{"config_name":"session_base","permission_mode":"autonomous","model":"gemma-4-moe"}`),
   then `POST /api/persistent/threads/{id}/input` with a prompt that produces
   ≥ 8 tool-call pairs and a summarisable region, e.g. eight `write_file`
   (120-word paragraph each) + `read_file` pairs, one call at a time, ending
   with the single word DONE.
3. Poll `GET /api/persistent/threads/{id}/state` until `turn_in_flight=false`.

Expected signals (all must hold):

- agent log: `Context compaction triggered: N messages, T tokens` →
  `Compacted N messages to M` → `Re-seated 1 protected message(s) after the summary`
  (the pinned input) → **no** `Persistent loop crashed`, **no** `parked`.
- `thread_events`: `compaction.started` → `context.compacted` → … → `turn.completed`
  for the same turn; no `turn.error`.
- `run_queue`: `state='done'`, `consumed_seq = input_seq`.
- `thread_messages`: exactly one `role='summary'` row for the turn; no duplicate
  `tool_call_id` rows; no duplicate `ai` rows (content+tool_calls); the final
  `ai` row is the answer; no `role='tool'` row of the turn contains
  `[tool result truncated by compaction` (make at least one tool result
  exceed `keep_window_max_tool_result_chars`, 16 000, so the cap fires — the
  capped copy is written only to fill a row whose incremental write was lost).
- `llm_requests` (audit store) for a post-compaction call: the message list is
  `[system, "[Summary of prior work]…", <the user's request verbatim>, …kept window]`
  — the pin re-seated the request right after the recap.
- then a **second** turn on the same session settles normally (the pin came
  off; the previous input is not re-seated again).

Also run once with `keep_recent_messages` small enough that the compaction
happens while tool calls are still in flight (default 10 is fine with ≥ 12
messages in the turn).

### 2.2 Transcript leg of skip-if-answered, live

Unit-covered only. Live recipe on k3d after any settled turn:

```sql
-- simulate a predecessor that answered but never settled
UPDATE run_queue SET consumed_seq = consumed_seq - 1, state = 'queued',
       run_after = now(), queued_at = now()
 WHERE unit_id = '<thread>' AND state = 'done';
```

Expected: the next claim logs `run_queue complete: … (skip-if-answered:
transcript holds the final answer for input seq …)`, **no** claim-bundle
fetch, no attach, no LLM call, `state='done'` again. Negative case: append a
new human row first — the claim must run normally.

The real-world instance is dev thread `56944877-bbd7-454a-b769-97c188690b9f`
(still `parked`, lease 4, `input_seq=72204`, `consumed_seq=72200`). Once this
change is deployed to dev, a plain `_UNPARK_SQL` unpark is the production test
of this leg: expected outcome is the skip line above, not a re-run of the
Frankfurt research. Until then the safe recovery is
`UPDATE run_queue SET consumed_seq=input_seq …` THEN unpark.

### 2.3 Settlement retry, live

Unit-covered only (`TestReconcileTurnWithRetry`). No cheap live fault
injection exists; if one is added (e.g. a `STATELESS_SETTLEMENT_FAULT_ONCE`
env read by `_reconcile_turn_with_retry`), expect one
`Authoritative stateless turn persist attempt 1/3 failed transiently` line
followed by a normal `turn.completed`.

### 2.4 Cockpit, in a browser

- **Retained tab, stranded turn**: open a session in a tab, kill the agent pod
  mid-turn (`kubectl delete pod` on the serving stateless pod) so no terminal
  frame is journaled, wait for the reaper to re-queue, then background/foreground
  the tab (mobile) or trigger a reconnect. Expected: the bubble closes as
  *interrupted* when the durable `session.state` arrives with
  `turn_in_flight=false`; running-tool chip and compaction card clear; the
  successor claim's `turn.started` opens a fresh turn.
- **Silence badge**: with a quiet agent mid-turn, reopen the stream (toggle
  airplane mode / switch tabs). Expected: "Working — no output for Ns" keeps
  counting from the last agent frame; it must not reset to 0 on the reopen or
  on the `session.state` frame.
- **Replay rebuild**: mid-turn, delete the thread's cursor in DevTools →
  Application → IndexedDB, reconnect. Expected: the live turn is rebuilt from
  the replay — no doubled text, no second copy of thoughts/tool calls folded
  above the answer.
- **turn.error line**: after 2.1's negative twin (kill the pod inside the
  turn-complete hook) or any loop crash, the bubble closes and a muted error
  line "The session loop stopped before this turn could be settled …" is
  rendered and survives reload.

### 2.5 Pinned lane and other loop hosts

- `preserve_message_identity=True` is set for every `PersistentSession`, pinned
  included. Run one pinned-lane session (`execution_lane='pinned'`) through a
  compaction and a settlement: expected unchanged behaviour (lenient walk),
  no duplicate rows.
- `src/subagents/child.py` builds its own `ContextManager` (default mode,
  id-less copies) and settles through its own driver, so it is untouched.
  Decide whether children should also preserve identity (they have no reducer
  either); if yes, one child compaction test in `tests/test_subagent*`.
- Worker graph (`src/graph.py`) keeps the reducer contract (default mode).
  Existing goldens cover it; a bench job that compacts (any long tactical
  phase) is the live confirmation.

### 2.6 Environment findings from the k3d gate (blockers, not tests)

- k3d `openrouter` system key is expired (`401 API key expired`) — every
  `openrouter/*` model is dead on k3d until `deployment/values-local.yaml` is
  rotated.
- `gpt-5.5` points at `srw-codex-proxy:8317`, which is not deployed on k3d
  (`Connection error.`); `gemma-4-moe` via `ai.h4ll.app` works.
- Nextcloud share `POST …/shares` returned 403 once out of four (thread
  `81c9ba06`); the stateless lane then refuses tool work every claim until
  max attempts → parked. Worth its own look (`main_cloud.md` Issue 13 family).
- Three `ws-thread-*` pods sit in `Unknown` for days; gate threads `da786f53`
  (parked, pod killed under load) and `81c9ba06` (parked after 5 crash-loop
  attempts) refuse `DELETE` with 409 — the stateless-end-cannot-settle family.
  Clean them by hand (`run_queue` → `done`, thread → `ended`) or leave as
  fixtures for that issue.
- Tilt rebuilds the agent image on ANY tracked-file change (tests included)
  and rolls the stateless pods; stop `tilt up` before a live gate, and never
  run the full pytest suite on the same host at the same time (readiness
  probes time out → reaper steals the lease → the pod is killed).

## 3. Scripts used (reproduce from here)

Gate driver — mint token, create thread, send input, poll state
(`k3d_compaction_gate.sh`):

```bash
ORCH=$(kubectl --context=k3d-srw -n srw get pods -o name | grep srw-orchestrator | head -1)
kx() { kubectl --context=k3d-srw -n srw exec "$ORCH" -c orchestrator -- "$@"; }
TOKEN=$(kx curl -s -X POST http://srw-keycloak:8080/realms/srw/protocol/openid-connect/token \
  -d grant_type=password -d client_id=admin-cli -d username=test -d password=test -d scope=openid \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["id_token"])')
api() { kx curl -s -X "$1" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
        "http://localhost:8085/api$2" ${3:+-d "$3"}; }
TID=$(api POST /persistent/threads '{"config_name":"session_base","permission_mode":"autonomous","model":"gemma-4-moe","title":"compaction gate"}' \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["thread_id"])')
api POST "/persistent/threads/$TID/input" "$(python3 -c 'import json,sys; print(json.dumps({"content": sys.argv[1]}))' "<prompt>")"
api GET "/persistent/threads/$TID/state"   # poll until turn_in_flight=false
```

Verification (`k3d_compaction_verify.sh`), against `srw-postgres-0` /
`psql -U srw -d srw`:

```sql
SELECT state, lease_token, input_seq, consumed_seq FROM run_queue WHERE unit_id='<TID>';
SELECT seq, kind, left(payload::text,90) FROM thread_events WHERE thread_id='<TID>'
   AND kind IN ('turn.started','turn.completed','turn.error','compaction.started','context.compacted') ORDER BY seq;
SELECT turn_number, role, count(*) FROM thread_messages WHERE thread_id='<TID>' GROUP BY 1,2;
SELECT tool_call_id, count(*) FROM thread_messages WHERE thread_id='<TID>' AND role='tool' GROUP BY 1 HAVING count(*)>1;
```

plus `kubectl logs <stateless pod> | grep <TID prefix> | grep -E "Context compaction|Compacted|Re-seated|Persistent loop crashed|parked|run_queue complete"`.
