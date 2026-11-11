#!/usr/bin/env python3
"""Hidden checks and page renders for the W* office tasks in tasks.yaml.

usage: python bench/grade_office.py TASK_ID OUTPUT_DIR [--render]

OUTPUT_DIR is a job's downloaded `output/` folder. Prints one JSON object:
{"task": ..., "passed": bool, "checks": {name: bool}, "notes": [...]}.
--render writes PNG pages of the deliverable to OUTPUT_DIR/_pages/ for the
blind visual judge (pairs of arms, same task).

Needs the workspace image's tools (pypdf, openpyxl, python-pptx,
python-docx, Pillow, LibreOffice, poppler). Run it there:

    docker run --rm -v "$PWD":/w -w /w --entrypoint python3 <workspace-image> \
        bench/grade_office.py W3-event-budget-xlsx runs/x/W3/rep1/output --render
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

RECALC_PROFILE = """<?xml version="1.0" encoding="UTF-8"?>
<oor:items xmlns:oor="http://openoffice.org/2001/registry" xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
<item oor:path="/org.openoffice.Office.Calc/Formula/Load"><prop oor:name="OOXMLRecalcMode" oor:op="fuse"><value>0</value></prop></item>
</oor:items>
"""


def soffice_convert(src: Path, fmt: str, outdir: Path) -> Path:
    profile = outdir / "lo-profile"
    (profile / "user").mkdir(parents=True, exist_ok=True)
    (profile / "user" / "registrymodifications.xcu").write_text(RECALC_PROFILE)
    subprocess.run(
        [
            "soffice",
            f"-env:UserInstallation={profile.as_uri()}",
            "--headless",
            "--convert-to",
            fmt,
            "--outdir",
            str(outdir),
            str(src),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=240,
    )
    return outdir / (src.stem + "." + fmt.split(":")[0])


def pdf_text_and_pages(pdf: Path) -> tuple[str, int, bool]:
    from pypdf import PdfReader

    reader = PdfReader(str(pdf))
    box = reader.pages[0].mediabox
    landscape = float(box.width) > float(box.height)
    return (
        "\n".join(p.extract_text() or "" for p in reader.pages),
        len(reader.pages),
        landscape,
    )


def xlsx_values(path: Path) -> tuple[list, dict]:
    """(all recalculated numeric values, {coordinate: formula}) of the workbook."""
    from openpyxl import load_workbook

    with tempfile.TemporaryDirectory() as tmp:
        recalculated = soffice_convert(path, "xlsx:Calc MS Excel 2007 XML", Path(tmp))
        values = load_workbook(recalculated, data_only=True)
        numbers = [
            c.value
            for ws in values.worksheets
            for row in ws.iter_rows()
            for c in row
            if isinstance(c.value, (int, float)) and not isinstance(c.value, bool)
        ]
    formulas = load_workbook(path)
    found = {
        f"{ws.title}!{c.coordinate}": c.value
        for ws in formulas.worksheets
        for row in ws.iter_rows()
        for c in row
        if isinstance(c.value, str) and c.value.startswith("=")
    }
    return numbers, found


def near(values, target, tol=0.6) -> bool:
    return any(abs(v - target) <= tol for v in values)


def check_budget_updates(path: Path) -> bool:
    """Set the 120-guest input to 100, recalculate, expect the new total."""
    from openpyxl import load_workbook

    wb = load_workbook(path)
    for ws in wb.worksheets:
        for row in ws.iter_rows():
            for cell in row:
                if cell.value == 120:
                    cell.value = 100
                    with tempfile.TemporaryDirectory() as tmp:
                        changed = Path(tmp) / "changed.xlsx"
                        wb.save(changed)
                        numbers, _ = xlsx_values(changed)
                    return near(numbers, 11000.0) and near(numbers, 110.0)
    return False


def grade(task: str, out: Path) -> tuple[dict, list[str]]:
    checks: dict[str, bool] = {}
    notes: list[str] = []
    if task.startswith("W1"):
        pdf = out / "uferstube_overview.pdf"
        checks["pdf_exists"] = pdf.is_file()
        if pdf.is_file():
            text, pages, _ = pdf_text_and_pages(pdf)
            low = text.lower()
            checks["pages_1_to_6"] = 1 <= pages <= 6
            checks["names_june_drop"] = bool(re.search(r"jun", low)) and (
                "lunch" in low or "mittag" in low
            )
            checks["names_december"] = bool(re.search(r"dec|dez", low))
            checks["names_february"] = bool(re.search(r"feb", low))
            checks["has_spend_per_cover"] = bool(
                re.search(r"per cover|pro gast|pro cover|spend", low)
            )
    elif task.startswith("W2"):
        png = out / "tickets.png"
        checks["png_exists"] = png.is_file()
        if png.is_file():
            from PIL import Image

            w, h = Image.open(png).size
            checks["at_least_600x300"] = w >= 600 and h >= 300
            notes.append(f"{w}x{h}")
    elif task.startswith("W3"):
        xlsx = out / "summer_party_budget.xlsx"
        checks["xlsx_exists"] = xlsx.is_file()
        if xlsx.is_file():
            numbers, formulas = xlsx_values(xlsx)
            checks["has_5_formulas"] = len(formulas) >= 5
            checks["total_12276"] = near(numbers, 12276.0)
            checks["per_guest_102_30"] = near(numbers, 102.3, 0.06)
            checks["updates_with_guests"] = check_budget_updates(xlsx)
    elif task.startswith("W4"):
        pdf = out / "q3_results.pdf"
        checks["pdf_exists"] = pdf.is_file()
        if pdf.is_file():
            text, pages, landscape = pdf_text_and_pages(pdf)
            checks["six_to_eight_slides"] = 6 <= pages <= 8
            checks["landscape"] = landscape
            checks["mentions_nps_46"] = "46" in text
    elif task.startswith("W5"):
        md = out / "findings.md"
        checks["md_exists"] = md.is_file()
        if md.is_file():
            low = md.read_text(encoding="utf-8", errors="replace").lower()
            checks["names_start_week"] = bool(
                re.search(r"10-06|oct(ober)?\.? 6|6\.? oct|06\.10|6 october", low)
            )
            checks["names_checkout_step"] = "checkout" in low and "order" in low
    elif task.startswith("W6"):
        docx = out / "brief_lieferant.docx"
        checks["docx_exists"] = docx.is_file()
        if docx.is_file():
            from docx import Document

            d = Document(str(docx))
            text = "\n".join(p.text for p in d.paragraphs) + "\n".join(
                c.text for t in d.tables for r in t.rows for c in r.cells
            )
            checks["closing"] = "Mit freundlichen Grüßen" in text
            checks["facts"] = all(s in text for s in ("Kessler", "2026-1187", "214,80"))
    elif task.startswith("W7"):
        pptx = out / "onboarding.pptx"
        checks["pptx_exists"] = pptx.is_file()
        if pptx.is_file():
            from pptx import Presentation

            prs = Presentation(str(pptx))
            text = " ".join(
                s.text_frame.text
                for slide in prs.slides
                for s in slide.shapes
                if s.has_text_frame
            )
            checks["five_to_seven_slides"] = 5 <= len(prs.slides) <= 7
            checks["names_head_chef"] = "Marta Lenz" in text
            checks["cold_chain_7"] = "7" in text
    else:
        raise SystemExit(f"not an office task: {task}")
    return checks, notes


def render(out: Path) -> list[str]:
    pages_dir = out / "_pages"
    shutil.rmtree(pages_dir, ignore_errors=True)
    pages_dir.mkdir()
    rendered = []
    for f in sorted(out.iterdir()):
        if f.suffix.lower() in {".png", ".jpg", ".jpeg"}:
            shutil.copy2(f, pages_dir / f.name)
            rendered.append(f.name)
            continue
        if f.suffix.lower() not in {".pdf", ".docx", ".pptx", ".xlsx"}:
            continue
        with tempfile.TemporaryDirectory() as tmp:
            pdf = (
                f
                if f.suffix.lower() == ".pdf"
                else soffice_convert(f, "pdf", Path(tmp))
            )
            subprocess.run(
                [
                    "pdftoppm",
                    "-png",
                    "-r",
                    "60",
                    "-l",
                    "8",
                    str(pdf),
                    str(pages_dir / f.stem),
                ],
                check=True,
            )
        rendered += sorted(p.name for p in pages_dir.glob(f"{f.stem}*.png"))
    return rendered


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("task")
    ap.add_argument("output_dir")
    ap.add_argument("--render", action="store_true")
    args = ap.parse_args()
    out = Path(args.output_dir)
    if not out.is_dir():
        print(
            json.dumps(
                {
                    "task": args.task,
                    "passed": False,
                    "checks": {},
                    "notes": ["no output dir"],
                }
            )
        )
        return 1
    checks, notes = grade(args.task, out)
    result = {
        "task": args.task,
        "passed": all(checks.values()),
        "checks": checks,
        "notes": notes,
    }
    if args.render:
        result["pages"] = render(out)
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
