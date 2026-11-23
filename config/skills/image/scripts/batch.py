#!/usr/bin/env python3
"""Generate many images from a JSONL manifest, under a hard spend cap.

    python3 skills/image/scripts/batch.py manifest.jsonl \\
        --max-spend 5.00 --price-per-image 0.04 [--concurrency 2] \\
        [--provider openai|gemini] [--model M] [--retries 2]

One JSON object per manifest line:

    {"prompt": "...", "out": "output/set/003.png",
     "size": "1024x1536", "quality": "low", "refs": ["sheet.png"], "n": 1}

Rules this script keeps, because nothing else caps the user's own key:

- It refuses to run without ``--max-spend`` (USD) and ``--price-per-image``
  (USD, read it from the provider's price page for this model, size and
  quality). Every attempt reserves ``price × n`` before it is sent; a retry
  reserves again. When the next attempt would pass the cap, the run stops
  and lists what is left (exit status 2).
- 429 and 5xx answers are retried with a growing pause; other errors are not.
- Every attempt is appended to ``.provenance/batch-ledger.jsonl`` (what was
  asked, what it was estimated to cost, what happened, the provider's usage).
- An entry whose output already exists is skipped, so running the same
  manifest again continues where it stopped.

Stdlib only (imports imagegen.py from the same folder).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from imagegen import (  # noqa: E402
    DEFAULT_MODELS,
    PROVIDERS,
    ImageError,
    generate,
    provider_from_env,
    save,
    target_paths,
)

MAX_CONCURRENCY = 8
LEDGER = Path(".provenance") / "batch-ledger.jsonl"


class Budget:
    """Reservations against the cap, shared by the worker threads."""

    def __init__(self, cap: float):
        self.cap = cap
        self.spent = 0.0
        self.stopped = False
        self._lock = threading.Lock()

    def reserve(self, amount: float) -> bool:
        with self._lock:
            if self.stopped or self.spent + amount > self.cap + 1e-9:
                self.stopped = True
                return False
            self.spent += amount
            return True


def _ledger(entry: dict, lock: threading.Lock) -> None:
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    with lock, LEDGER.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"time": datetime.now(timezone.utc).isoformat(), **entry}) + "\n")


def _done(item: dict) -> bool:
    n = int(item.get("n") or 1)
    for mime in ("image/png", "image/jpeg", "image/webp"):
        if all(path.exists() for path in target_paths(item["out"], n, mime)):
            return True
    return False


def load_manifest(path: str) -> list[dict]:
    items = []
    for number, line in enumerate(Path(path).read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        item = json.loads(line)
        if not item.get("prompt") or not item.get("out"):
            raise SystemExit(f"Error: manifest line {number} needs \"prompt\" and \"out\"")
        n = int(item.get("n") or 1)
        if not 1 <= n <= 4:
            raise SystemExit(f"Error: manifest line {number}: n must be 1 to 4")
        items.append(item)
    return items


def run(items, *, provider, model, price, budget, concurrency, retries) -> dict:
    lock = threading.Lock()
    results = {"done": [], "skipped": [], "failed": [], "not_run": []}

    def work(item: dict) -> None:
        if _done(item):
            results["skipped"].append(item["out"])
            return
        n = int(item.get("n") or 1)
        attempt = 0
        while True:
            if not budget.reserve(price * n):
                results["not_run"].append(item["out"])
                return
            entry = {"out": item["out"], "n": n, "estimated_usd": round(price * n, 6), "attempt": attempt + 1, "provider": provider, "model": model}
            try:
                result = generate(item["prompt"], provider=provider, model=model, n=n, size=item.get("size"), quality=item.get("quality"), refs=item.get("refs") or ())
            except ImageError as exc:
                _ledger({**entry, "status": "error", "error": str(exc)[:300]}, lock)
                if exc.retryable and attempt < retries:
                    attempt += 1
                    continue
                results["failed"].append(f"{item['out']}: {exc}")
                return
            paths = save(
                result.images,
                item["out"],
                {"tool": "batch.py", "prompt": item["prompt"], "provider": provider, "model": model, "size": item.get("size"), "quality": item.get("quality"), "references": item.get("refs") or [], "billing": "user key"},
            )
            _ledger({**entry, "status": "ok", "paths": [str(p) for p in paths], "usage": result.usage}, lock)
            results["done"].extend(str(p) for p in paths)
            return

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        list(pool.map(work, items))
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("manifest")
    parser.add_argument("--max-spend", type=float, required=True, help="hard cap in USD for this run")
    parser.add_argument("--price-per-image", type=float, required=True, help="USD per image at this model/size/quality")
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--provider", choices=sorted(PROVIDERS))
    parser.add_argument("--model")
    args = parser.parse_args(argv)
    if args.max_spend <= 0 or args.price_per_image <= 0:
        parser.error("--max-spend and --price-per-image must be above 0")
    concurrency = max(1, min(args.concurrency, MAX_CONCURRENCY))
    try:
        provider = args.provider or provider_from_env()
    except ImageError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    model = args.model or os.environ.get("IMAGEGEN_MODEL") or DEFAULT_MODELS[provider]
    items = load_manifest(args.manifest)
    budget = Budget(args.max_spend)
    results = run(items, provider=provider, model=model, price=args.price_per_image, budget=budget, concurrency=concurrency, retries=max(0, args.retries))
    summary = {
        "images": len(results["done"]),
        "skipped_existing": len(results["skipped"]),
        "failed": results["failed"],
        "not_run_spend_cap": results["not_run"],
        "estimated_spend_usd": round(budget.spent, 4),
        "max_spend_usd": args.max_spend,
        "ledger": str(LEDGER),
    }
    print(json.dumps(summary, indent=2))
    if results["not_run"]:
        print(f"Stopped at the spend cap: {len(results['not_run'])} entries not run.", file=sys.stderr)
        return 2
    return 1 if results["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
