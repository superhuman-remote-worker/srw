#!/usr/bin/env python3
"""Convert JUnit reports to advisory per-file scheduling weights."""

from __future__ import annotations

import argparse
from collections import defaultdict
import json
from pathlib import Path
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]


def file_timings(reports, root=ROOT):
    totals = defaultdict(float)
    for report in reports:
        for case in ET.parse(report).iter("testcase"):
            parts = case.get("classname", "").split(".")
            for length in range(len(parts), 0, -1):
                path = Path(*parts[:length]).with_suffix(".py")
                if (root / path).is_file():
                    totals[path.as_posix()] += float(case.get("time", "0"))
                    break
    return {path: round(seconds, 3) for path, seconds in sorted(totals.items())}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", nargs="+", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    args.output.write_text(json.dumps(file_timings(args.reports), indent=2) + "\n")
