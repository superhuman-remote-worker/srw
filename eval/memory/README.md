# Memory eval harness

Offline evaluation of the agent memory subsystem against
**LongMemEval** (arXiv:2410.10813, ICLR 2025) — Phase 2 of
[`knowledge-base/knowledge/features/agent_memory_overhaul.md`](../../knowledge-base/knowledge/features/agent_memory_overhaul.md).
Research grounding: `knowledge-base/knowledge/research/ai_memory/results/05_lifecycle_eval_report.md`.

The harness drives the production seam directly — `MemoryManager.assemble()`
and `capture()` from `src/agent/services/memory/` — with the production loader,
stores, embedding service, and prompts. No graph spin-up, no orchestrator.
What an arm measures is what a real session with that YAML would do.

## How LongMemEval maps onto the seam

| LongMemEval | Harness | Production analogue |
|---|---|---|
| one question instance | one `project_id` | a project |
| one haystack session | one `job_id` within the project | a thread/job of that project |
| evidence labels (`answer_session_ids`) | each memory row's `job_id` **is** its source session | — |
| asking the question | fresh reader scope + one `assemble(query=question)` | a new session in the project |

Ingestion is **incremental** (sessions replayed in haystack order, memories
accrue through `capture()`), not batch-loaded. Two ingestion modes:

- **seam** (`arms/persistent_current.yaml`) — production-faithful replay:
  per round `HumanMessage` → `assemble()` (the live per-turn read path, so
  access stats accrue) → `AIMessage` → `capture(turn_end)`;
  then `capture(session_end)` → teardown extraction. Costs real
  auxiliary-LLM calls.
- **verbatim** (`arms/flat_verbatim.yaml`) — no extraction LLM: every
  user-assistant round stored directly and dedup disabled (`dedup_threshold: 1.01`), so question-time
  retrieval is pure hybrid RRF over a flat round-granularity index. This is
  the cheap smoke arm and the published-baseline reproduction arm.

Scoring: injected memories → source sessions (first-occurrence collapse) →
**Recall@k / NDCG@k / coverage@k / first-hit rank** vs the evidence labels,
overall and per question type, plus cost (tokens injected, assemble
latency). Abstention questions (`*_abs`) are excluded from retrieval
aggregates (they return with the end-task slice).

## Quick start

```bash
# 1. Dataset (not committed — ~250 MB). From the LongMemEval repo:
#    https://github.com/xiaowu0162/LongMemEval (HF: xiaowu0162/longmemeval)
#    Put longmemeval_s.json under eval/memory/data/.

# 2. A pgvector server. Tunnel the cluster's pgvector to localhost:5433:
scripts/port-forward-dbs.sh &
export EVAL_VECTOR_DSN='postgresql://<user>:<password>@localhost:5433/srw_eval'
# (defaults to exactly that DSN; the runner creates the srw_eval database
#  and applies migrations/vector/ with --init-db)

# 3. Real embeddings (retrieval quality IS the measurement):
export EMBEDDING_BASE_URL=...   # e.g. the ai.h4ll.app router
export EMBEDDING_API_KEY=...
export EMBEDDING_MODEL=qwen3-embedding-8b   # 4096-d (schema dimension)

# 4. Smoke on the committed fixture (no LLM, ~20 embedding calls):
python -m eval.memory.run \
  --dataset eval/memory/fixtures/tiny_longmemeval.json \
  --arm eval/memory/arms/flat_verbatim.yaml --init-db \
  --out eval/memory/runs/smoke_flat

# 5. Real runs:
python -m eval.memory.run --dataset eval/memory/data/longmemeval_s.json \
  --arm eval/memory/arms/flat_verbatim.yaml --init-db          # full set OK
python -m eval.memory.run --dataset eval/memory/data/longmemeval_s.json \
  --arm eval/memory/arms/persistent_current.yaml --limit 20    # LLM cost!

# 6. Reports & A/B:
python -m eval.memory.report eval/memory/runs/<run>
python -m eval.memory.report eval/memory/runs/<runA> eval/memory/runs/<runB>

# 6b. Question-time-only A/Bs (scorers, budgets, injection policies):
#     ingest ONCE without --cleanup, then re-query the same corpora with
#     other arms in minutes (scope run_id is taken from the source run):
python -m eval.memory.run --dataset ... --arm <ingest-arm> --out runs/<base>      # no --cleanup
python -m eval.memory.run --dataset ... --arm <readpath-arm> \
  --requery-from eval/memory/runs/<base> --out runs/<base>_rerank --limit 20
#     COMPARABILITY: every assemble mutates corpus state (access bumps), so
#     snapshot access_count/last_accessed into a side table once after
#     ingest and restore it between requery arms. Reranker arms need
#     top_k >= store size, and reranker.timeout: 60 (full-store batches trip the 10s default when
#     the endpoint is cold/contended — failures are contained passthroughs,
#     visible as "scorer 'reranker' failed (contained)" in run.log, which
#     silently turns the arm into a legacy-order measurement).
#     GATE ARMS: top_k must cover the candidate count with margin (512 for
#     the ~270-row s20 corpora) — candidates past top_k stay UNSCORED and
#     pass the gate fail-open, silently inflating kept-items (caught on
#     affe2881: 263 candidates vs top_k 256 → 7-item leak). Audit: a kept
#     item with items[].score == 0.0 in a gated arm is leak evidence
#     (a true 0.0 rerank score sits below any positive floor).

# 7. End-task accuracy (reader + LongMemEval judge over a finished run):
export EVAL_READER_MODEL=... EVAL_READER_BASE_URL=... EVAL_READER_API_KEY=...
export EVAL_JUDGE_MODEL=...  # both fall back to EVAL_AUX_*
python -m eval.memory.judge eval/memory/runs/<run>
python -m eval.memory.judge eval/memory/runs/<run> --calibrate labels.json

# 8. Contradiction-survival probe (fact → supersede → query):
python -m eval.memory.run --dataset eval/memory/fixtures/contradiction_probe.json \
  --arm <arm> --out eval/memory/runs/contra_<arm>
python -m eval.memory.contradiction eval/memory/runs/contra_<arm> --reader
```

Seam-mode arms additionally need the extraction model: fill the
`auxiliary:` block in the arm (model + base_url + `api_key_env` naming an
env var). Arm-level extras: `extraction_prompt_file:` overrides the
matrix-resolved extraction prompt (the prompt-variant A/B knob, e.g.
`config/prompts/memory_extraction_prompt_complete.txt`);
`config_overrides.memory.pipeline.scorers: [reranker]` +
`config_overrides.memory.reranker:` switch on the reranker scorer (its
transport defaults to the arm's auxiliary endpoint);
`config_overrides.memory.pipeline.policies: [bounded]` +
`config_overrides.memory.bounded: {max_items: N, max_tokens: T}` cap the
injected memory items AFTER scorers run (`memory.budget_tokens` trims
inside the retriever in legacy order — before a reranker can act — so
post-scorer bounding must use the policy, not the budget);
`policies: [gate, bounded]` + `config_overrides.memory.gate:
{threshold: X, mode: relative|absolute}` additionally drops
below-floor memory items before the cap — floor = X (absolute) or
X × the assemble's top score (relative; the measured choice — absolute
cutoffs lose weakly-phrased evidence because qwen3-reranker's absolute
scale varies by orders of magnitude per query). Items the scorer never
saw pass through ungated, so a contained scorer failure degrades to
the full dump, not an empty injection; `config_overrides.memory.bounded.
include_knowledge: true` counts KB notes against `max_tokens` too (B5
one-budget); `config_overrides.memory.query: {digest: true}` switches
query formation to the unified request digest (window + task frame) at
every assemble — both the per-turn ingest reads and question time, so
it is requery-able (note: at question time the answering session is one
message deep, so digest ≡ legacy there by construction — a digest
requery measures plumbing + no-regression, not the windowing benefit). Results stream to `<out>/results.jsonl` per question — rerunning
with the same `--out`/`--run-id` resumes, so a flaky aux endpoint only
costs the in-flight questions. `run.log` in the run dir captures all
contained seam warnings; a seam arm with `stored=0` across many questions
means extraction is failing, not that the system has no memory.

### Borrowing the k3d cluster's routing

If the local k3d stack is up, the embedding/auxiliary endpoints + keys the
cluster dispatches with can be resolved from the orchestrator (they live
encrypted in the model catalog, not in env):

```bash
kubectl --context=k3d-srw -n srw port-forward svc/srw-pgvector 15433:5432 &
export EVAL_VECTOR_DSN="postgresql://srw:<VECTOR_POSTGRES_PASSWORD>@localhost:15433/srw_eval"
# In-pod, resolve_default_for_capability("embedding"/"auxiliary") +
# get_user_llm_endpoint() print the decrypted base_url/api_key — see the
# Phase-2 setup notes in the overhaul doc.
```

## Cost & scale guidance

- **verbatim**: embeddings only. Each question's round texts are
  batch-primed up front (`PrimedEmbedding`, 32 texts/request → ~8
  requests per ~250-round question instead of ~250) and `store()` then
  consumes the cache; vectors are identical either way. Batching cuts
  request overhead and retry exposure, but it cannot beat the backend's
  *throughput*: the k3d-default endpoint (`qwen3-embedding-8b-strix`)
  serializes at **~0.8 unique texts/s** however you slice it (parallel
  singles, batches — same rate; beware benchmarking with near-identical
  texts, prefix caching makes it look 20–50× faster). At that rate the
  full _S set (~124k rounds) is ~42 h; a stratified `--limit 100` is
  ~9 h and statistically sufficient for the ballpark check (recall@5
  SE ≈ 0.04). Same `--out`/`--run-id` resumes, so a subset run can be
  extended to the full set later.
- **seam**: extraction fires per `observer_interval` turns **and** per
  session end → roughly `sessions × (1 + turns/interval)` aux calls per
  question (~50–80 for _S questions). Start with `--limit 10..20`
  (type-stratified, seeded) and scale once numbers stabilize.
- `--cleanup` deletes each question's memory rows after scoring (the
  result row keeps the ranking); without it, rows stay queryable per
  `run_id`-derived scopes and reruns never collide (fresh uuid5 scopes).

## Reproducing a published baseline (the Phase-2 acceptance anchor)

The paper's retrieval table reports session/round-granularity Recall@k /
NDCG@k for BM25 and dense retrievers (Stella, Contriever, GTE). Our
`flat_verbatim` arm is the closest stack-equivalent: round granularity,
our embedder, RRF-fused dense+sparse+recency. Run it on the full _S set
and compare `summary.json` to the paper's dense-retriever rows — the
check is *ballpark agreement* (our embedder is stronger than the paper's;
wild divergence means a harness bug, not a system difference). That
anchors the metric implementation; `persistent_current` on the same
questions is then the recorded **current-system baseline**.

## Deliberate deviations from production (all metric-motivated)

- `capture()` is awaited (production fire-and-forgets `turn_end`) — same
  writer effects, deterministic completion.
- `retrieval_timeout=None` (production: 5 s per store call, skip-on-timeout)
  — a silent skip would corrupt metrics; slowness should fail loud.
- No KB bucket (LongMemEval has no `kb_write`); `kb_notes` binds inert,
  same as a session without Neo4j.
- `created_at` is ingestion time, so the recency channel ranks by haystack
  order (which is chronological) rather than real timestamps;
  `ingestion.date_prefix: true` optionally embeds session dates in content
  for temporal-reasoning A/Bs.
- The extraction-window cap (`_MAX_OBSERVATION_WINDOW`) applies as in
  production.

## End-task accuracy (judge.py)

Two stages over a finished run's `results.jsonl` (each row carries the
question, gold answer, and the production-rendered `injected_context`
captured at answer time — judging needs no DB and no dataset file):

1. **Reader**: a chat model answers the question from the injected block
   only. Fixed frame across arms, so the block is the only variable.
2. **Judge**: the verbatim LongMemEval `evaluate_qa.py` prompts (per
   question type; abstention variant for `*_abs`), temperature 0,
   max_tokens 10, verdict = `"yes" in response.lower()`. Reusing the
   paper's protocol inherits its published calibration; run
   `--calibrate labels.json` (`{question_id: true|false}` hand labels)
   to measure *our* judge model's agreement — target >97 %, re-judge
   with a stronger `EVAL_JUDGE_MODEL` if below.

Outputs `judge.jsonl` (resume-safe) + `judge_summary.json` (accuracy
overall / by type / `abstention_score`). Retrieval metrics and end-task
accuracy stay separate on purpose: reading is its own bottleneck
(brief 05) — compare both columns before attributing a delta.

## Contradiction-survival probe (contradiction.py)

`fixtures/contradiction_probe.json`: 8 LongMemEval-schema instances
(fact stated → fillers → fact superseded → fillers) with a `probe` block
naming the old/new values and their sessions. Any arm ingests it through
the normal runner; the scorer then reports, per layer:

- retrieval order: `update_injected` / `update_above_original`
- reading (`--reader`): answers classified **current / stale / miss** by
  exact substring — no judge LLM; mentioning old + new counts as current
  (the knowledge-update convention).

`stale` survival on the seam arm is the Phase-4 acceptance metric; the
knowledge-update slice of the real-data judge run is the same signal
in vivo.

## Lifecycle / supersede probe (lifecycle.py)

`fixtures/lifecycle_probe.json`: 15 LongMemEval-schema instances carrying a
`lifecycle` block (`category` + `expect_valid` / `expect_retired` /
`expect_unique`) across every verdict outcome — UPDATE, chained UPDATE,
coexist (ADD), false-contradiction, merge-additive, NOOP — mixing personal and
project facts. Where the contradiction probe asks "did the stale value stop
being *read*", this scores the **bi-temporal DB state directly**: after an arm
ingests through the verdict write path (NOT `--cleanup` — rows must persist), it
reads `memories` per `project_uuid(run_id, qid)` and classifies each labelled
value **ok / false_retired / missed_retire / duplicate / not_extracted**.

```
python -m eval.memory.run --dataset eval/memory/fixtures/lifecycle_probe.json \
    --arm eval/memory/data/contra_complete_verdict_k3d.yaml --out runs/lifecycle_verdict
python -m eval.memory.lifecycle runs/lifecycle_verdict --verbose
```

Headline metrics: **`false_retire_rate`** (a should-survive value wrongly retired
— the over-retiring signal the contradiction probe is structurally blind to,
since there every case *wants* something retired), `missed_retire_rate`,
`duplicate_rate`. `not_extracted` is reported separately (an extraction gap, not
a lifecycle fault). The fixture is regenerated by `fixtures/build_lifecycle_probe.py`.

## Layout

```
arms/        committed arm YAMLs (config variants under test)
fixtures/    committed LongMemEval-schema datasets (tests + smoke + probe)
data/        real datasets (gitignored)
runs/        results.jsonl / summary.json / report.md / run.log (gitignored)
datasets.py  loader + typed records + stratified subsetting
arms.py      ArmSpec + production-loader config resolution
infra.py     DB/migrations/embeddings/aux-LLM/manager builders + scope uuids
ingest.py    seam + verbatim session replay (+ batch-primed embeddings)
query.py     question-time assemble + provenance → session ranking
metrics.py   Recall@k / NDCG@k / coverage / aggregation
judge.py     reader + LongMemEval LLM-judge + calibration
contradiction.py  probe scorer (retrieval order + current/stale/miss)
lifecycle.py      supersede scorer (bi-temporal state: false/missed-retire, dup)
run.py       CLI runner (resume-safe, concurrent per question)
report.py    markdown rendering + arm-vs-arm deltas
```

Tests: `tests/test_memory_eval_harness.py` (fully offline — fakes via the
`HarnessHandles` factory seams).

## Phase-2 slices still to build here

- **Production-trace set**: bespoke second dataset from real jobs/threads
  now that the LongMemEval loop is trusted.
- Recall-shape (single- vs multi-hop) is covered by the by-type slices:
  `multi-session` coverage@k *is* the multi-hop signal; a finer per-hop
  breakdown only if the Phase-7 graph verdict needs it.
