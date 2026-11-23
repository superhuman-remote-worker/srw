"""bench/grade_office.py, the hidden checks of the W* office tasks.

It dispatches on the exact task id (a prefix test graded W10-W19 as W1) and
grades the two image tasks added for image generation WP7: W8, a deck PDF
with generated pictures, and W9, a character training set with a contact
sheet.
"""

from __future__ import annotations

import importlib.util
import io
import zipfile
from pathlib import Path

import pytest
from PIL import Image

_PATH = Path(__file__).resolve().parents[1] / "bench" / "grade_office.py"
_spec = importlib.util.spec_from_file_location("grade_office", _PATH)
grade_office = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(grade_office)


def _png(size, color=(200, 120, 40)) -> bytes:
    out = io.BytesIO()
    Image.new("RGB", size, color).save(out, format="PNG")
    return out.getvalue()


def test_tasks_dispatch_on_their_exact_id():
    assert grade_office.task_number("W1-restaurant-pdf-report") == "W1"
    assert grade_office.task_number("W10-anything") == "W10"
    with pytest.raises(SystemExit, match="not an office task: W10-anything"):
        grade_office.grade("W10-anything", Path("."))


def _dataset(
    tmp_path, *, count=16, size=(832, 1216), nested=False, captions=True, trigger="mrvl"
):
    out = tmp_path / "output"
    out.mkdir()
    with zipfile.ZipFile(out / "mira_dataset.zip", "w") as archive:
        for i in range(count):
            prefix = "set/" if nested else ""
            archive.writestr(f"{prefix}{i:03d}.png", _png(size, (10 * i, 80, 120)))
            if captions:
                archive.writestr(
                    f"{prefix}{i:03d}.txt", f"{trigger}, a woman on a pier at dusk"
                )
    Image.new("RGB", (1600, 1200), "#222").save(out / "contact_sheet.png")
    return out


def test_w9_passes_a_good_training_set(tmp_path):
    checks, notes = grade_office.grade("W9-character-dataset-zip", _dataset(tmp_path))
    assert all(checks.values()), checks
    assert notes[0] == "16 images, 16 captions"


@pytest.mark.parametrize(
    "kwargs, failing",
    [
        ({"nested": True}, {"flat"}),
        # No caption means no trigger either.
        ({"captions": False}, {"one_caption_per_image", "trigger_first"}),
        ({"trigger": "mira"}, {"trigger_first"}),
        ({"size": (512, 512)}, {"min_edge_768"}),
        ({"count": 6}, {"10_to_40_images"}),
    ],
)
def test_w9_names_what_is_wrong(tmp_path, kwargs, failing):
    checks, _ = grade_office.grade(
        "W9-character-dataset-zip", _dataset(tmp_path, **kwargs)
    )
    assert {key for key, value in checks.items() if not value} == failing, checks


def test_w9_counts_an_unreadable_image_as_a_failure(tmp_path):
    out = _dataset(tmp_path)
    with zipfile.ZipFile(out / "mira_dataset.zip", "a") as archive:
        archive.writestr("broken.png", b"not a png")
        archive.writestr("broken.txt", "mrvl, broken")
    checks, _ = grade_office.grade("W9-character-dataset-zip", out)
    assert checks["every_image_opens"] is False


def test_w8_finds_pictures_on_the_slides(tmp_path):
    out = tmp_path / "output"
    out.mkdir()
    slides = []
    for i in range(5):
        slide = Image.new("RGB", (1600, 900), "white")
        if i < 3:
            slide.paste(Image.new("RGB", (700, 500), (180, 90, 30)), (800, 200))
        slides.append(slide)
    slides[0].save(out / "autumn_menu.pdf", save_all=True, append_images=slides[1:])
    for i in range(3):
        (out / f"dish_{i}.png").write_bytes(_png((1024, 1024)))
    checks, notes = grade_office.grade("W8-deck-with-pictures-pdf", out)
    # An image-only PDF has no text: the dish check fails, the rest holds.
    assert (
        checks["pdf_exists"] and checks["landscape"] and checks["five_to_seven_slides"]
    )
    assert checks["pictures_on_3_slides"] and checks["pictures_kept_as_files"]
    assert checks["names_the_dishes"] is False
    assert "5 slides with pictures" in notes[0]
