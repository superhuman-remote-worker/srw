#!/usr/bin/env python3
"""Partition pytest's actual collection by file, then run one bounded shard."""

from __future__ import annotations

import argparse
from collections import Counter
import json
import math
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def partition_files(files, weights, count):
    """Longest measured file first; every collected file belongs to one shard."""
    if count < 1:
        raise ValueError("shard count must be positive")
    groups = [[] for _ in range(count)]
    loads = [0.0] * count

    def weight(path):
        value = float(weights.get(path, 1.0))
        return value if math.isfinite(value) and value > 0 else 1.0

    for path in sorted(set(files), key=lambda path: (-weight(path), path)):
        index = min(range(count), key=lambda i: (loads[i], len(groups[i]), i))
        groups[index].append(path)
        loads[index] += weight(path)
    return [sorted(group) for group in groups]


class Collection:
    def __init__(self):
        self.items = {}
        self.skipped_files = set()

    def pytest_collection_finish(self, session):
        self.items = {
            item.nodeid: item.path.resolve().relative_to(ROOT).as_posix()
            for item in session.items
        }

    def pytest_collectreport(self, report):
        if report.failed:
            print(report.longrepr, file=sys.stderr)
        elif report.skipped:
            # Module-level importorskip has no test items. Still run that file
            # once so its skip remains visible in the shard's JUnit report.
            path = (ROOT / report.nodeid.split("::", 1)[0]).resolve()
            if path.is_file():
                self.skipped_files.add(path.relative_to(ROOT).as_posix())


def main():
    import pytest

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index", type=int, required=True)
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument(
        "--timings", type=Path, default=ROOT / "policy/pytest_file_timings.json"
    )
    parser.add_argument("--report-dir", type=Path, required=True)
    parser.add_argument("--plan-only", action="store_true")
    parser.add_argument(
        "targets", nargs="+", help="Same file/directory selection as pytest"
    )
    args = parser.parse_args()
    if not 0 <= args.index < args.count:
        parser.error("index must be between zero and count - 1")
    if any("::" in target for target in args.targets):
        parser.error("targets must be files or directories, not individual node IDs")
    os.chdir(ROOT)
    collection = Collection()
    result = pytest.main(
        [
            *args.targets,
            "--collect-only",
            "-p",
            "no:terminal",
            "-p",
            "no:cacheprovider",
        ],
        plugins=[collection],
    )
    if result:
        return int(result)
    weights = json.loads(args.timings.read_text()) if args.timings.exists() else {}
    groups = partition_files(
        set(collection.items.values()) | collection.skipped_files, weights, args.count
    )
    chosen = groups[args.index]
    args.report_dir.mkdir(parents=True, exist_ok=True)
    counts = Counter(collection.items.values())
    manifest = {
        "index": args.index,
        "count": args.count,
        "collected_tests": len(collection.items),
        "selected_tests": sum(counts[path] for path in chosen),
        "files": chosen,
        "collection_skips": sorted(collection.skipped_files.intersection(chosen)),
        "nodeids": sorted(
            node for node, path in collection.items.items() if path in chosen
        ),
    }
    (args.report_dir / "selection.json").write_text(
        json.dumps(manifest, indent=2) + "\n"
    )
    print(
        f"Shard {args.index + 1}/{args.count}: {len(chosen)} files, "
        f"{manifest['selected_tests']}/{len(collection.items)} tests",
        flush=True,
    )
    # An empty shard is successful only after a successful nonempty collection.
    # Never call the shell runner with no targets: it would run the whole suite.
    if args.plan_only or not chosen:
        return 0
    env = dict(os.environ, SRW_PYTHON=sys.executable)
    result = subprocess.run(
        [
            str(ROOT / "scripts/pytest-fast.sh"),
            *chosen,
            "-q",
            "--tb=short",
            "--maxfail=0",
            "--durations=30",
            "-p",
            "no:cacheprovider",
            f"--junitxml={args.report_dir / 'junit.xml'}",
        ],
        env=env,
    )
    # Pytest exits 5 when a shard contains only module-level skips. The parent
    # collection proved there are tests elsewhere; this shard still reports its
    # skips. Collection errors and actual failures retain their nonzero exits.
    if result.returncode == 5 and set(chosen) <= collection.skipped_files:
        return 0
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
