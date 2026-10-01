# Deterministic E2E model provider

This test-owned service replaces only SRW's external model boundary in the
full-stack application journey. The agent still makes real HTTP requests and consumes
OpenAI-compatible streaming responses. The service never belongs in a customer Helm
release.

It exposes two listeners from one non-root process:

- inference on `:8000`: public `/health`, authenticated `/v1/models`,
  `/v1/chat/completions`, `/v1/embeddings`, and `/v1/rerank`;
- control on loopback `:8001`: bearer-protected `/control/health` and
  `/control/scenarios/...` routes.

The Kubernetes Service publishes inference only. The harness reaches control with
`kubectl port-forward` to the owned pod. `E2E_CONTROL_TOKEN` and
`E2E_INFERENCE_API_KEY` are required process environment variables and come from the
run-owned `srw-e2e-model-fixture` Secret; neither has a checked-in default.

`E2E_CHAT_MODEL_ID` can replace the default `e2e-chat` wire ID with a unique
lowercase test model name. The manifest cutover smoke uses this when registering
its own temporary endpoint in an existing installation. The configured ID is the
only accepted chat model and is advertised by `/v1/models`; other IDs continue to
fail without being echoed into diagnostic records.

## Control contract

Arm an isolated run before inference:

```text
POST /control/scenarios/{run-id}/arm
Authorization: Bearer <run-owned-token>
Content-Type: application/json

{"scenario":"reply","required_responses":1}
```

`GET /control/scenarios/{run-id}` returns required/consumed/remaining response
counts, `unexpected_count`, grouped counters, and sanitized call metadata. It never
returns prompts, messages, tool arguments, or headers. `DELETE` on the same URL resets
the run after browser transport and resource cleanup have finished.

Normal messages carry `E2E-{run-id}` and receive `E2E_REPLY:{run-id}`. Lifecycle
requests without a token are associated only when exactly one run is armed. Unknown,
ambiguous, exhausted, unsupported-schema, and wrong-model requests fail closed.

Multi-stage lifecycle cases retain one run ID across End and Resume. After all
calls in a phase finish, `POST /control/scenarios/{run-id}/advance` adds another
response budget and chooses the next scenario. It preserves every call, its
sequence, consumed count, unexpected count and counter. The caller supplies
`expected_cancelled` as the cumulative number of intentional cancellations;
pending, unfinished and unaccounted work refuses advancement. Keep the run
armed through the associated background work and resource cleanup, then delete
it. A phase-local display may subtract earlier counters, but the cumulative
ledger remains the acceptance evidence.

`delegation-batch` drives the existing stateless recovery contract. It emits
two foreground `delegate_agent` calls for `probe` children, which run a bounded
`sleep 300` command. After the parent receives both tool results it emits one
final answer. This fixture exercises platform replacement while a delegation
batch is active; it does not enable pinned delegation fan-out.
When the parent binds shell tools, it first runs a small command so the live
gate can inspect its pane identity and prove shell preservation across claim
handoff.

The `search-job` scenario is a narrow live-gate driver. It advances the real agent
through its strategic setup, stages a two-todo tactical phase, calls `web_search`,
then returns through `job_complete` and the remaining strategic todos. It reads the
todo guide required by the enforced staging contract before staging, and reads the
verification guide at each completion boundary. Tool results still pass through SRW
normally and are never retained by the fixture.

The `worker-job` scenario is the hermetic sibling of the two below. `search-job`
and `fetch-job` are deliberately *live-gate* drivers: each requires a real
third-party provider (SearXNG, Crawl4AI) so it can exercise the off-pod
boundary. A profile that has neither — the owned minimal profile does not, and
adding one breaks its "exactly one endpoint, exactly two models" determinism
contract — cannot complete a worker job with either. `worker-job` binds only
`read_file`, `todo_complete`, `next_phase_todos` and `job_complete`: it reads
the todo guide the staging contract requires, runs the strategic todos, stages
a two-todo tactical phase, reads the verification guide at the completion
boundary, then returns through `job_complete`. It fails closed the same way when
a required tool is not bound.

`prepared-workspace-job` extends that workflow with a real `run_command` call
over the workspace SSH backend. It checks the prepared executable and the
per-workspace initialization receipt, then writes an execution marker. Fresh
runs require that marker to be absent; run IDs ending in `-reuse` require an
existing marker. The fixture advances only after the tool returns both exit
code zero and the exact run-specific proof. It retains counters, not shell
output. `scripts/workspace-preparation-srw-k3d-gate.py` uses this scenario to
exercise MCP admission and prepared VM isolation through the installed harness.

The `fetch-job` scenario follows the same fail-closed pattern for the off-pod fetch
boundary. It calls `extract_webpage` and `crawl_website` against `example.com` before
completing the job. It reads both guides required by the runtime's enforced staging and
completion contracts at their phase boundaries; fetched content still flows only through
SRW and is not retained.

## Local contract tests

From the repository root (using the repository Python environment):

```bash
pytest tests/e2e/app/deterministic_provider/ -q
ruff check tests/e2e/app/deterministic_provider/
```

The container build context is this directory:

```bash
docker build -t srw-e2e-model-fixture:local tests/e2e/app/deterministic_provider
```

Owned acceptance closes a settled life with `POST /control/scenarios/{run_id}/close`
(`expected_cancelled` defaults to zero). Closed scopes remain readable by GET
and in `closed_runs`; their IDs cannot be rearmed, advanced or reset. Closure
refuses pending or unaccounted work. A cancelled auxiliary call cannot satisfy
a required response. Late calls retain global unscoped accounting and leave
archived counters unchanged. The historical DELETE/reset contract remains for
fixture unit tests; owned acceptance callers use retaining close.
