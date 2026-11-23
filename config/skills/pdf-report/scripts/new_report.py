#!/usr/bin/env python3
"""Start a report from the house template.

usage: python skills/pdf-report/scripts/new_report.py [work/report.html]

Copies assets/report.html and assets/report.css into the target's folder
(default work/, never inside skills/), leaves existing files alone, and prints
the next commands.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

ASSETS = Path(__file__).resolve().parent.parent / "assets"


def main(argv: list[str]) -> int:
    target = Path(argv[1]) if len(argv) > 1 else Path("work/report.html")
    if "skills" in target.parts:
        print(
            "put your report outside skills/ (for example work/report.html)",
            file=sys.stderr,
        )
        return 2
    target.parent.mkdir(parents=True, exist_ok=True)
    css = target.parent / "report.css"
    for src, dst in ((ASSETS / "report.html", target), (ASSETS / "report.css", css)):
        if dst.exists():
            print(f"kept existing {dst}")
        else:
            shutil.copy2(src, dst)
            print(f"created {dst}")
    print(
        f"next: replace the example content in {target} (keep the <link> to report.css),\n"
        f"then: python skills/pdf-report/scripts/render_pdf.py {target} output/<name>.pdf"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
