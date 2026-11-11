#!/usr/bin/env bash
# =============================================================================
# Build-time conformance gate for the workspace office and data stack.
# =============================================================================
#
# The bundled office skills (config/skills/pdf-report, charts, data-analysis,
# spreadsheet, document, slides) tell the agent these tools are installed. If
# one is missing the agent burns turns installing it or gives up, so both
# workspace images assert the same list:
#   - docker/Dockerfile.workspace   (container workspace, `full` target)
#   - docker/agent-vm-base/         (VM golden image, asserted in stage2)
# Shared, not duplicated, for the reason docker/assert-browser-stack.sh gives.
#
# Usage: assert-office-stack.sh   (exits non-zero with a diagnostic on failure)
# =============================================================================
set -uo pipefail

# Run from a directory every user can traverse (stage2 runs gates as
# agent-host from a 0750 home).
cd /

fail=0

_check() {
    local label="$1"
    shift
    local out
    if out=$("$@" 2>&1); then
        printf '  ok       %s\n' "$label"
    else
        printf '  MISSING  %s\n' "$label"
        if [ -n "$out" ]; then
            printf '%s\n' "$out" | sed 's/^/           | /'
        fi
        fail=1
    fi
}

echo "Asserting workspace office stack:"

for module in pandas numpy scipy matplotlib openpyxl xlsxwriter pptx docx \
    reportlab pypdf pdfplumber PIL jinja2 markdown tabulate; do
    _check "python: import ${module}" python3 -c "import ${module}"
done

# matplotlib must render headless (no display in a workspace).
_check "matplotlib renders a PNG headless" python3 -c "
import matplotlib, tempfile, os
matplotlib.use('Agg')
import matplotlib.pyplot as plt
fig, ax = plt.subplots()
ax.plot([0, 1], [0, 1])
path = os.path.join(tempfile.mkdtemp(), 'probe.png')
fig.savefig(path)
assert os.path.getsize(path) > 0
"

for bin in soffice pandoc pdftoppm pdftotext pdfinfo rsvg-convert agent-chromium; do
    _check "binary: ${bin}" command -v "${bin}"
done

# Calibri/Cambria metric twins keep Office documents from reflowing when
# LibreOffice renders them; Noto/DejaVu/Liberation cover the rest.
for family in "Carlito" "Caladea" "Liberation Sans" "Noto Sans" "DejaVu Sans"; do
    _check "font: ${family}" sh -c "fc-list : family | grep -qi '^${family}'"
done

if [ "$fail" -ne 0 ]; then
    echo "Office stack incomplete: the office skills would promise tools this image lacks." >&2
    exit 1
fi
echo "Office stack complete."
