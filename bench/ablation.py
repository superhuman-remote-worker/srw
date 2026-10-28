"""Memory / knowledge-base ablation campaign on the server-side Job Bench.

Answers "is the memory system worth it?" with the instrument we already have:
the same pinned task set, one variable per arm, replicates, server-computed
report. Five arms over the memory pipeline (everything else identical):

  off          no auto-injection at all, no extraction. KB tools stay callable
               (the agent can still kb_search on its own — pull, not push).
  kb-only      KB notes auto-injected (top-5), no memory retrieval/extraction.
  memory-only  memory retrieval + extraction (current stack), no KB injection.
  current      the defaults — memory + KB, reranker/gate/bounded, ingestion.
  skip-own     the defaults, but a job's own memory is not injected while its
               source turns are still in context
               (memory.retrieval.same_conversation: skip_in_context).

Why one project (and one bench run) PER ARM: memory and KB notes are
project-scoped — every job in a project reads what earlier jobs wrote — so
arms sharing a project would contaminate each other. Each arm therefore gets
a fresh project and its own single-arm run; `report` joins the runs. Inside
an arm, replicates run sequentially (max_in_flight 1 by default) so replicate
k sees whatever replicates < k left behind — that IS the cross-job effect
under test, so compare replicate 1 vs 3 within the memory arms too.

Metrics (per task x arm, medians across clean replicates): completion
(deliverable gate), wall minutes, main-call count, median prompt tokens,
cache-hit %, plus main/auxiliary token totals from the audit store so the
extraction/assembly cost of the memory arms is visible, not just their
effect on the main calls.

Usage (SRW_API_URL / SRW_TOKEN as for bench/submit.py):

    python bench/ablation.py submit --name mem-ablation-01 \
        --tasks S1-outbox-note,S3-glossary,S4-csv-totals,M2-runbook,D1-wordfreq-kata,D2-inventory-bugfix \
        --replicates 3 --model gemma-4-moe
    python bench/ablation.py status --name mem-ablation-01
    python bench/ablation.py report --name mem-ablation-01 [--json]
    python bench/ablation.py cancel --name mem-ablation-01

State lives in bench/runs/<name>/ablation.json (gitignored).
"""

from __future__ import annotations

import argparse
import copy
import json
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _api import ApiError, get, post  # noqa: E402
from submit import load_tasks  # noqa: E402

BENCH_DIR = Path(__file__).resolve().parent
RUNS_DIR = BENCH_DIR / "runs"

# The default writer list from config/overlays/worker.yaml `memory.pipeline`.
# deep_merge replaces lists wholesale, so an arm that keeps extraction must
# restate it. Keep in sync with the YAML when the default pipeline changes.
DEFAULT_WRITERS = [
    "interval_extractor",
    "phase_boundary_extractor",
    "pre_compaction_extractor",
    "compaction_memory",
    "queued_memory",
]

# Everything an arm changes is under `memory`. `manager.enabled` stays on in
# every arm so the seam is the single injection path: an empty pipeline binds
# a no-op manager (nothing retrieved, capture() has no writers) and the legacy
# direct-store blocks stay skipped. `memory.enabled: false` additionally skips
# RecallStore construction (no store => todo_complete queues nothing either).
ARMS: dict[str, dict] = {
    "off": {
        "memory": {
            "enabled": False,
            "pipeline": {
                "retrievers": [],
                "scorers": [],
                "policies": [],
                "writers": [],
            },
        }
    },
    "kb-only": {
        "memory": {
            "enabled": False,
            "pipeline": {
                "retrievers": ["kb_notes"],
                "scorers": [],
                "policies": [],
                "writers": [],
            },
        }
    },
    "memory-only": {
        "memory": {
            "pipeline": {
                "retrievers": ["recall_hybrid"],
                "scorers": ["reranker"],
                "policies": ["gate", "bounded"],
                "writers": list(DEFAULT_WRITERS),
            },
        }
    },
    "current": {},
    "skip-own": {"memory": {"retrieval": {"same_conversation": "skip_in_context"}}},
}

AUX_CALL_TYPES = ("memory_extraction", "memory_assembly", "auxiliary", "summarization")


def state_path(name: str) -> Path:
    return RUNS_DIR / name / "ablation.json"


def load_state(name: str) -> dict:
    path = state_path(name)
    if not path.exists():
        raise SystemExit(f"no campaign state at {path} — run `submit` first")
    return json.loads(path.read_text())


def save_state(name: str, state: dict) -> None:
    path = state_path(name)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2))


def caller_id() -> str:
    me = get("/api/auth/me")
    user = me.get("user") or me
    uid = user.get("id")
    if not uid:
        raise SystemExit(f"could not resolve caller id from /api/auth/me: {me}")
    return str(uid)


def create_project(name: str, arm: str, uid: str, description: str) -> str:
    data = post(
        "/api/projects",
        {
            "name": f"bench-{name}-{arm}",
            "description": description,
            "goal": f"Job Bench ablation campaign '{name}', arm '{arm}'",
            "user_id": uid,
        },
    )
    project = data.get("project") or data
    pid = project.get("id") or project.get("project_id")
    if not pid:
        raise RuntimeError(f"no project id in create response: {data}")
    return str(pid)


# ---------------------------------------------------------------- submit ---


def cmd_submit(args: argparse.Namespace) -> None:
    arms = [a.strip() for a in args.arms.split(",") if a.strip()]
    unknown = [a for a in arms if a not in ARMS]
    if unknown:
        raise SystemExit(f"unknown arm(s) {unknown}; known: {sorted(ARMS)}")
    only = set(args.tasks.split(",")) if args.tasks else None
    tasks = load_tasks(Path(args.tasks_file), only)
    if not tasks:
        raise SystemExit("no tasks selected")
    if state_path(args.name).exists() and not args.force:
        raise SystemExit(
            f"{state_path(args.name)} exists — pick a new --name (campaigns are "
            "immutable) or pass --force to overwrite the local state file"
        )

    print(
        f"campaign {args.name}: {len(tasks)} task(s) x {len(arms)} arm(s) x "
        f"{args.replicates} replicate(s) = {len(tasks) * len(arms) * args.replicates} "
        f"jobs; model {args.model}; max_in_flight {args.max_in_flight} per arm"
    )
    for arm in arms:
        print(f"  arm {arm:12s} config_override = {json.dumps(ARMS[arm])}")
    if args.dry_run:
        return

    uid = caller_id()
    state = {
        "name": args.name,
        "model": args.model,
        "replicates": args.replicates,
        "max_in_flight": args.max_in_flight,
        "tasks": [t["id"] for t in tasks],
        "created_at": datetime.now(timezone.utc).isoformat(),
        "arms": {},
    }
    description = (
        f"Memory/KB ablation '{args.name}'. Throwaway bench project — one per arm "
        "so project-scoped memory and KB notes never cross arms."
    )
    for arm in arms:
        pid = create_project(args.name, arm, uid, description)
        spec = {
            "name": f"{args.name}/{arm}",
            "tasks": tasks,
            "replicates": args.replicates,
            "max_in_flight": args.max_in_flight,
            "arms": [
                {
                    "name": arm,
                    "model": args.model,
                    "config_override": copy.deepcopy(ARMS[arm]),
                    **({"execution_lane": args.lane} if args.lane else {}),
                }
            ],
            "project_id": pid,
        }
        try:
            run = post("/api/bench/runs", spec)
        except ApiError as e:
            state["arms"][arm] = {"project_id": pid, "error": str(e)}
            save_state(args.name, state)
            raise
        run_id = run.get("id") or run.get("run_id") or (run.get("run") or {}).get("id")
        state["arms"][arm] = {"project_id": pid, "run_id": str(run_id)}
        save_state(args.name, state)
        print(f"  {arm:12s} project {pid}  run {run_id}")
    print(f"state: {state_path(args.name)}")


# ---------------------------------------------------------------- status ---


def cmd_status(args: argparse.Namespace) -> None:
    state = load_state(args.name)
    for arm, ref in state["arms"].items():
        if not ref.get("run_id"):
            print(f"{arm:12s} (no run: {ref.get('error')})")
            continue
        run = get(f"/api/bench/runs/{ref['run_id']}")
        run = run.get("run") or run
        # v1 stores the submission ledger directly as the `state` list.
        ledger = run.get("state") or []
        if isinstance(ledger, dict):
            ledger = ledger.get("submissions") or []
        counts: dict[str, int] = {}
        for entry in ledger:
            key = entry.get("final_status") or entry.get("last_status") or "in_flight"
            counts[key] = counts.get(key, 0) + 1
        print(
            f"{arm:12s} run {ref['run_id'][:8]} status={run.get('status')} "
            f"submitted={len(ledger)} {json.dumps(counts, sort_keys=True)}"
        )


# ---------------------------------------------------------------- cancel ---


def cmd_cancel(args: argparse.Namespace) -> None:
    state = load_state(args.name)
    for arm, ref in state["arms"].items():
        if ref.get("run_id"):
            post(f"/api/bench/runs/{ref['run_id']}/cancel", {})
            print(f"{arm:12s} cancelled run {ref['run_id']}")


# ---------------------------------------------------------------- report ---


def _num(value) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _median(values: list[float | None]) -> float | None:
    clean = [v for v in values if v is not None]
    return statistics.median(clean) if clean else None


def _fmt(value: float | None, digits: int = 1) -> str:
    if value is None:
        return "—"
    if digits == 0:
        return f"{value:,.0f}"
    return f"{value:,.{digits}f}"


def _token_int(usage: dict, *keys: str) -> int:
    for key in keys:
        val = usage.get(key)
        if val is not None:
            try:
                return int(val)
            except (TypeError, ValueError):
                continue
    return 0


def job_token_split(job_id: str) -> dict[str, int]:
    """Main vs auxiliary tokens for one job from the audit store.

    Paginates GET /api/jobs/{id}/llm-requests (all call types). Returns zeros
    when the audit tier is unavailable — reported as "unknown", never as 0.
    """
    totals = {
        "main_in": 0,
        "main_out": 0,
        "aux_in": 0,
        "aux_out": 0,
        "aux_calls": 0,
        "main_calls": 0,
    }
    offset, limit = 0, 100
    while True:
        try:
            page = get(
                f"/api/jobs/{job_id}/llm-requests",
                limit=limit,
                offset=offset,
                call_type="all",
            )
        except ApiError as e:
            if e.status == 503:
                return {}
            raise
        entries = page.get("entries") or page.get("requests") or []
        for entry in entries:
            usage = entry.get("token_usage") or {}
            tin = _token_int(usage, "input_tokens", "prompt_tokens")
            tout = _token_int(usage, "output_tokens", "completion_tokens")
            call_type = entry.get("call_type") or "main"
            if call_type == "main":
                totals["main_in"] += tin
                totals["main_out"] += tout
                totals["main_calls"] += 1
            else:
                totals["aux_in"] += tin
                totals["aux_out"] += tout
                totals["aux_calls"] += 1
        total = page.get("total")
        offset += len(entries)
        if not entries or (total is not None and offset >= int(total)):
            break
    return totals


def cmd_report(args: argparse.Namespace) -> None:
    state = load_state(args.name)
    arms = [a for a, ref in state["arms"].items() if ref.get("run_id")]
    reports = {
        arm: get(f"/api/bench/runs/{state['arms'][arm]['run_id']}/report")
        for arm in arms
    }

    # Per-job token split (main vs aux) from the audit store, cached in state.
    splits: dict[str, dict] = state.setdefault("token_splits", {})
    for arm in arms:
        for job in reports[arm].get("jobs") or []:
            jid = job.get("job_id")
            if not jid or job.get("status") not in ("completed", "failed", "cancelled"):
                continue
            if jid not in splits or not splits[jid]:
                splits[jid] = job_token_split(jid)
    save_state(args.name, state)

    if args.json:
        print(json.dumps({"state": state, "reports": reports}, indent=2, default=str))
        return

    # ---- per task x arm -------------------------------------------------
    print(
        f"== ablation {state['name']}  model {state['model']}  "
        f"{state['replicates']} replicate(s)  arms {', '.join(arms)}"
    )
    header = f"{'task':22s} {'arm':12s} {'done':>5s} {'wall':>7s} {'calls':>6s} {'p-tok':>8s} {'cache%':>7s} {'aux-in':>9s} {'aux/main':>9s}"
    print(header)
    print("-" * len(header))
    arm_totals: dict[str, dict] = {
        a: {
            "jobs": 0,
            "completed": 0,
            "wall": 0.0,
            "main_in": 0,
            "aux_in": 0,
            "aux_calls": 0,
            "main_calls": 0,
            "unknown": 0,
        }
        for a in arms
    }
    for task_id in state["tasks"]:
        for arm in arms:
            agg = next(
                (
                    r
                    for r in reports[arm].get("aggregates") or []
                    if r.get("task") == task_id
                ),
                None,
            )
            jobs = [
                j for j in reports[arm].get("jobs") or [] if j.get("task") == task_id
            ]
            if not agg:
                print(f"{task_id:22s} {arm:12s} {'—':>5s}")
                continue
            metrics = agg.get("metrics") or {}
            done = f"{agg.get('completed_jobs', 0)}/{agg.get('expected_replicates', 0)}"
            wall = _num((metrics.get("wall_minutes") or {}).get("median"))
            calls = _num((metrics.get("request_count") or {}).get("median"))
            ptok = _num((metrics.get("median_prompt_tokens") or {}).get("median"))
            cache = _num((metrics.get("cache_hit_pct") or {}).get("median"))
            aux_in, main_in, unknown = [], [], 0
            for j in jobs:
                sp = splits.get(j.get("job_id") or "") or {}
                if not sp:
                    unknown += 1
                    continue
                aux_in.append(sp["aux_in"])
                main_in.append(sp["main_in"])
            ratio = None
            if aux_in and main_in and sum(main_in):
                ratio = sum(aux_in) / sum(main_in)
            print(
                f"{task_id:22s} {arm:12s} {done:>5s} {_fmt(wall):>7s} {_fmt(calls, 0):>6s} "
                f"{_fmt(ptok, 0):>8s} {_fmt(cache):>7s} {_fmt(_median(aux_in), 0):>9s} {_fmt(ratio, 2):>9s}"
                + (f"  ({unknown} job(s) without audit rows)" if unknown else "")
            )
            t = arm_totals[arm]
            t["jobs"] += agg.get("terminal_jobs", 0)
            t["completed"] += agg.get("completed_jobs", 0)
            t["wall"] += (wall or 0.0) * max(agg.get("included_jobs", 0), 0)
            t["main_in"] += sum(main_in)
            t["aux_in"] += sum(aux_in)
            for j in jobs:
                sp = splits.get(j.get("job_id") or "") or {}
                t["aux_calls"] += sp.get("aux_calls", 0)
                t["main_calls"] += sp.get("main_calls", 0)
            t["unknown"] += unknown

    # ---- per arm --------------------------------------------------------
    print()
    print(
        f"{'arm':12s} {'completed':>10s} {'main-in tok':>12s} {'aux-in tok':>11s} {'aux calls':>10s} {'main calls':>11s}"
    )
    for arm in arms:
        t = arm_totals[arm]
        print(
            f"{arm:12s} {t['completed']:>4d}/{t['jobs']:<5d} {t['main_in']:>12,d} {t['aux_in']:>11,d} "
            f"{t['aux_calls']:>10,d} {t['main_calls']:>11,d}"
            + (f"  ({t['unknown']} job(s) unknown)" if t["unknown"] else "")
        )
    print()
    print(
        "wall/calls/p-tok/cache% = medians over clean replicates (server report); "
        "aux-in = median auxiliary input tokens per job; aux/main = summed aux "
        "input over summed main input for the cell. Cross-arm deltas on n=3 are "
        "weak evidence per cell — read the per-arm totals and the D1/D2 rows first."
    )


# ------------------------------------------------------------------ main ---


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("submit", help="create one project + one bench run per arm")
    s.add_argument(
        "--name", required=True, help="campaign name (immutable; new name per campaign)"
    )
    s.add_argument(
        "--tasks", help="comma-separated task ids from tasks.yaml (default: all)"
    )
    s.add_argument("--tasks-file", default=str(BENCH_DIR / "tasks.yaml"))
    s.add_argument(
        "--arms",
        default=",".join(ARMS),
        help=f"comma-separated subset of {sorted(ARMS)}",
    )
    s.add_argument("--replicates", type=int, default=3)
    s.add_argument("--model", default="gemma-4-moe")
    s.add_argument(
        "--max-in-flight",
        type=int,
        default=1,
        help="per arm; 1 keeps replicates sequential inside a project",
    )
    s.add_argument(
        "--lane",
        choices=["pinned", "stateless"],
        default=None,
        help="execution lane for every arm (default: JobCreate default)",
    )
    s.add_argument(
        "--force", action="store_true", help="overwrite an existing local state file"
    )
    s.add_argument("--dry-run", action="store_true")
    s.set_defaults(fn=cmd_submit)

    for name, fn, help_ in (
        ("status", cmd_status, "submission ledger per arm"),
        ("cancel", cmd_cancel, "cancel every arm's run"),
    ):
        p = sub.add_parser(name, help=help_)
        p.add_argument("--name", required=True)
        p.set_defaults(fn=fn)

    r = sub.add_parser("report", help="joined per-task x arm report")
    r.add_argument("--name", required=True)
    r.add_argument("--json", action="store_true")
    r.set_defaults(fn=cmd_report)

    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
