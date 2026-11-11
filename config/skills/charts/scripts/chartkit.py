"""Small helpers for house-style matplotlib charts.

    import sys; sys.path.insert(0, "skills/charts/scripts")
    import chartkit as ck
    ck.use()                                   # style + headless backend
    fig, ax = ck.plt.subplots()
    ax.bar(months, covers, color=ck.highlight(months, {"Jun", "Jul", "Aug"}))
    ax.yaxis.set_major_formatter(ck.number_format("de"))
    ck.save(fig, "work/covers")                # writes covers.svg and covers.png

Nothing here is required; it only saves retyping the same lines.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

STYLE = Path(__file__).resolve().parent.parent / "assets" / "srw.mplstyle"

COLORS = {
    "ink": "#1b2230",
    "muted": "#5d6677",
    "accent": "#2456c9",
    "highlight": "#d9822b",
    "context": "#c9d3e6",  # de-emphasised bars and lines
    "good": "#2f8f5b",
    "bad": "#c43d3d",
}

__all__ = [
    "COLORS",
    "plt",
    "use",
    "highlight",
    "number_format",
    "label_bars",
    "label_line_ends",
    "save",
]


SCALED = (
    "font.size",
    "axes.titlesize",
    "axes.labelsize",
    "xtick.labelsize",
    "ytick.labelsize",
    "legend.fontsize",
    "lines.linewidth",
    "axes.titlepad",
)


def use(scale: float = 1.0) -> None:
    """Apply the house style. Call once before creating figures.
    scale=1.6 for slides (11 x 5.5 in figures shown on a 13.3 in slide):
    every font size and the line width grow together."""
    plt.style.use(str(STYLE))
    if scale != 1.0:
        for key in SCALED:
            plt.rcParams[key] = float(plt.rcParams[key]) * scale


def highlight(
    labels, marked, color: str = COLORS["highlight"], rest: str = COLORS["context"]
) -> list[str]:
    """One colour per label: the marked ones stand out, the rest recede."""
    marked = set(marked)
    return [color if label in marked else rest for label in labels]


def number_format(
    locale: str = "en", decimals: int = 0, prefix: str = "", suffix: str = ""
) -> FuncFormatter:
    """Axis formatter with thousands separators: en 1,234.5 / de 1.234,5."""

    def fmt(value, _pos=None):
        text = f"{value:,.{decimals}f}"
        if locale == "de":
            text = text.replace(",", "_").replace(".", ",").replace("_", ".")
        return f"{prefix}{text}{suffix}"

    return FuncFormatter(fmt)


def label_bars(
    ax,
    bars,
    fmt=lambda v: f"{v:,.0f}",
    horizontal: bool = False,
    color: str | None = None,
) -> None:
    """Write each bar's value at its end, so the reader needs no gridlines.
    Pass horizontal=True for ax.barh bars."""
    for bar in bars:
        if horizontal:
            value = bar.get_width()
            xy = (bar.get_x() + value, bar.get_y() + bar.get_height() / 2)
            offset, ha, va = (4, 0), "left", "center"
        else:
            value = bar.get_height()
            xy = (bar.get_x() + bar.get_width() / 2, bar.get_y() + value)
            offset, ha, va = (0, 3), "center", "bottom"
        ax.annotate(
            fmt(value),
            xy,
            xytext=offset,
            textcoords="offset points",
            ha=ha,
            va=va,
            fontsize=8.5,
            color=color or COLORS["muted"],
        )


def label_line_ends(ax, fmt=None) -> None:
    """Name each line at its right end instead of using a legend."""
    for line in ax.get_lines():
        xs, ys = line.get_xdata(), line.get_ydata()
        if len(xs) == 0 or str(line.get_label()).startswith("_"):
            continue
        text = line.get_label() if fmt is None else f"{line.get_label()} {fmt(ys[-1])}"
        ax.annotate(
            text,
            (xs[-1], ys[-1]),
            xytext=(6, 0),
            textcoords="offset points",
            va="center",
            fontsize=plt.rcParams["legend.fontsize"],
            # A context-grey line keeps a readable label.
            color=COLORS["muted"]
            if line.get_color() == COLORS["context"]
            else line.get_color(),
        )


def save(fig, stem: str, formats: tuple[str, ...] = ("svg", "png")) -> list[Path]:
    """Save the figure as stem.svg (for PDF/HTML) and stem.png (for chat,
    canvas, Word and PowerPoint), close it and print the paths."""
    paths = []
    base = Path(stem)
    base.parent.mkdir(parents=True, exist_ok=True)
    for ext in formats:
        path = base.with_suffix("." + ext)
        fig.savefig(path)
        paths.append(path)
        print(path)
    plt.close(fig)
    return paths
