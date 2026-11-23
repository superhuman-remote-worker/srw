#!/usr/bin/env python3
"""Recalculate an .xlsx with LibreOffice and report what a user would trip on.

usage: python xlsx_check.py FILE.xlsx [--store-values] [--rows N]

Reports per sheet: size, formula count, cells that evaluate to an error,
numeric columns without a number format, and the computed values of the last
N rows (totals). --store-values replaces FILE with the recalculated copy so
viewers that do not calculate show values; it keeps formulas and most
formatting, but check charts and conditional formats afterwards.
Exit code 1 when any formula evaluates to an error.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ERRORS = ("#DIV/0!", "#REF!", "#NAME?", "#VALUE!", "#N/A", "#NUM!", "#NULL!", "Err:")

# A throwaway LibreOffice profile that recalculates every formula on load
# (0 = always). The default profile never recalculates Excel files, and a
# shared profile also blocks a second soffice running at the same time.
RECALC_PROFILE = """<?xml version="1.0" encoding="UTF-8"?>
<oor:items xmlns:oor="http://openoffice.org/2001/registry"
           xmlns:xs="http://www.w3.org/2001/XMLSchema"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
<item oor:path="/org.openoffice.Office.Calc/Formula/Load"><prop oor:name="OOXMLRecalcMode" oor:op="fuse"><value>0</value></prop></item>
<item oor:path="/org.openoffice.Office.Calc/Formula/Load"><prop oor:name="ODFRecalcMode" oor:op="fuse"><value>0</value></prop></item>
</oor:items>
"""


def recalculated_copy(src: Path, tmp: Path) -> Path:
    profile = tmp / "profile"
    (profile / "user").mkdir(parents=True)
    (profile / "user" / "registrymodifications.xcu").write_text(RECALC_PROFILE)
    work = tmp / "in"
    work.mkdir()
    copy = work / src.name
    shutil.copy2(src, copy)
    out = tmp / "out"
    subprocess.run(
        ["soffice", f"-env:UserInstallation={profile.as_uri()}", "--headless", "--calc",
         "--convert-to", "xlsx:Calc MS Excel 2007 XML", "--outdir", str(out), str(copy)],
        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=240,
    )
    result = out / (src.stem + ".xlsx")
    if not result.is_file():
        raise SystemExit("LibreOffice did not produce a recalculated copy")
    return result


def is_number_format_missing(cell) -> bool:
    return cell.number_format == "General"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("file")
    ap.add_argument("--store-values", action="store_true")
    ap.add_argument("--rows", type=int, default=3, help="last rows to print per sheet")
    args = ap.parse_args()
    src = Path(args.file)
    if not src.is_file():
        print(f"no such file: {src}", file=sys.stderr)
        return 2

    from openpyxl import load_workbook

    formulas = load_workbook(src)  # formulas as written
    with tempfile.TemporaryDirectory() as tmp:
        recalculated = recalculated_copy(src, Path(tmp))
        values = load_workbook(recalculated, data_only=True)
        error_count = 0
        for ws in formulas.worksheets:
            vs = values[ws.title]
            n_formulas = 0
            errors: list[str] = []
            unformatted: set[str] = set()
            for row in ws.iter_rows():
                for cell in row:
                    is_formula = isinstance(cell.value, str) and cell.value.startswith("=")
                    n_formulas += is_formula
                    value = vs[cell.coordinate].value
                    if isinstance(value, str) and value.startswith(ERRORS):
                        errors.append(f"{cell.coordinate} {value} ({cell.value})")
                    if cell.row > 1 and isinstance(value, (int, float)) and not isinstance(value, bool):
                        if is_number_format_missing(cell) and (is_formula or abs(value) >= 1000 or value != int(value)):
                            unformatted.add(cell.column_letter)
                    if is_formula and value is None:
                        errors.append(f"{cell.coordinate} evaluated to nothing ({cell.value})")
            error_count += len(errors)
            print(f"== sheet '{ws.title}': {ws.max_row} rows x {ws.max_column} cols, {n_formulas} formulas")
            for e in errors[:25]:
                print(f"  ERROR {e}")
            if len(errors) > 25:
                print(f"  … {len(errors) - 25} more errors")
            if unformatted:
                print(f"  WARNING no number format in column(s) {', '.join(sorted(unformatted))}")
            if ws.freeze_panes is None and ws.max_row > 20:
                print("  note: header row not frozen")
            first = max(1, vs.max_row - args.rows + 1)
            for r in range(first, vs.max_row + 1):
                cells = [vs.cell(row=r, column=c).value for c in range(1, min(vs.max_column, 10) + 1)]
                print(f"  row {r}: " + " | ".join("" if v is None else str(v) for v in cells))
        if args.store_values:
            shutil.copy2(recalculated, src)
            print(f"stored recalculated values in {src}")
    print("OK: no formula errors" if error_count == 0 else f"FAIL: {error_count} formula problem(s)")
    return 0 if error_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
