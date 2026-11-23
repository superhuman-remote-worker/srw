#!/usr/bin/env python3
"""Start an HTML slide deck from the house template.

usage: python skills/slides/scripts/new_deck.py [work/deck.html]

Copies assets/deck.html and assets/deck.css into the target's folder
(default work/, never inside skills/), leaves existing files alone, and prints
the next commands.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

ASSETS = Path(__file__).resolve().parent.parent / "assets"


def main(argv: list[str]) -> int:
    target = Path(argv[1]) if len(argv) > 1 else Path("work/deck.html")
    if "skills" in target.parts:
        print(
            "put your deck outside skills/ (for example work/deck.html)",
            file=sys.stderr,
        )
        return 2
    target.parent.mkdir(parents=True, exist_ok=True)
    css = target.parent / "deck.css"
    for src, dst in ((ASSETS / "deck.html", target), (ASSETS / "deck.css", css)):
        if dst.exists():
            print(f"kept existing {dst}")
        else:
            shutil.copy2(src, dst)
            print(f"created {dst}")
    print(
        f'next: replace the example slides in {target}; keep one <section class="slide"> '
        "per slide and the <link> to deck.css,\n"
        f"then: python skills/slides/scripts/render_deck.py {target} output/<name>.pdf"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
