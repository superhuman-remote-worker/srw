"""``compose_images``: contact sheets and character sheets in the workspace.

Pillow in the agent process, so it works on every tier, including the
shell-less ``virtual`` one (image_generation.md WP5, FM11). Decoding goes
through :mod:`agent.services.image_compose` and its pixel cap. The agent
shows a sheet to the user with ``set_canvas(renderer="image")``.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import posixpath
from typing import Any, List, Optional

from langchain_core.tools import tool
from pydantic import BaseModel, Field

from agent.services.image_compose import (
    DEFAULT_CELL,
    MAX_CELLS,
    ImageComposeError,
    SheetTile,
    contact_sheet,
)
from agent.services.image_content import IMAGE_DATA_TAG_TEMPLATE
from agent.tools.context import ToolContext

logger = logging.getLogger(__name__)


class ComposeImagesArguments(BaseModel):
    paths: List[str] = Field(
        ...,
        description=f"Workspace paths of the images, in order (at most {MAX_CELLS}).",
    )
    path: str = Field(
        ...,
        description="Where to save the sheet, e.g. output/images/sheet.png (PNG).",
    )
    labels: Optional[List[str]] = Field(
        None,
        description=(
            "One caption per image (e.g. front, side, back). Default: the file names."
        ),
    )
    columns: Optional[int] = Field(
        None, ge=1, le=16, description="Images per row. Default: a near-square grid."
    )
    cell: int = Field(
        DEFAULT_CELL,
        ge=64,
        le=2048,
        description="Edge of each cell in pixels; each image is fitted whole.",
    )
    numbered: bool = Field(True, description="Number the captions 1, 2, 3, …")
    overwrite: bool = Field(False, description="Replace an existing file at path.")


def create_image_tools(context: ToolContext) -> List[Any]:
    """``[compose_images]`` for an agent with a workspace."""

    def _read(path: str) -> bytes:
        data = context.workspace_manager.backend.read_file(path, binary=True)
        return data.encode() if isinstance(data, str) else data

    @tool(args_schema=ComposeImagesArguments)
    async def compose_images(
        paths: List[str],
        path: str,
        labels: Optional[List[str]] = None,
        columns: Optional[int] = None,
        cell: int = DEFAULT_CELL,
        numbered: bool = True,
        overwrite: bool = False,
    ) -> str:
        """Put several workspace images into one labelled grid image (a contact
        sheet, a character sheet, a before/after pair) and save it as a PNG.

        Use it to compare variations side by side, to show the user a set at
        once (then set_canvas with renderer="image"), or to build a reference
        sheet for generate_image. Every image is fitted whole into its cell.
        The sheet comes back for you to look at.
        """
        wanted = [p.strip() for p in paths if isinstance(p, str) and p.strip()]
        if not wanted:
            return "Error: give at least one image path."
        if len(wanted) > MAX_CELLS:
            return f"Error: at most {MAX_CELLS} images per sheet."
        if labels is not None and len(labels) != len(wanted):
            return (
                f"Error: {len(labels)} labels for {len(wanted)} images; give one "
                "label per image or none."
            )
        if not path.lower().endswith(".png"):
            path = posixpath.splitext(path)[0] + ".png"
        tiles: list[SheetTile] = []
        for index, image_path in enumerate(wanted):
            try:
                data = await asyncio.to_thread(_read, image_path)
            except (FileNotFoundError, OSError, ValueError) as exc:
                return f"Error: cannot read {image_path}: {exc}"
            label = labels[index] if labels else posixpath.basename(image_path)
            tiles.append(SheetTile(data=data, label=str(label)))
        try:
            sheet = await asyncio.to_thread(
                contact_sheet, tiles, columns=columns, cell=cell, numbered=numbered
            )
        except ImageComposeError as exc:
            return f"Error: cannot compose the sheet: {exc}"
        from agent.tools.workspace.files import save_workspace_file

        outcome = await asyncio.to_thread(
            save_workspace_file, context, path, sheet, overwrite=overwrite
        )
        if outcome.startswith("Error"):
            return outcome
        from agent.services.image_compose import image_size

        size = image_size(sheet) or (0, 0)
        text = (
            f"Saved a sheet of {len(tiles)} image(s) as {path} "
            f'({size[0]}×{size[1]} px). Show it with set_canvas(renderer="image").'
        )
        if context.get_phase_multimodal():
            b64 = base64.b64encode(sheet).decode()
            return (
                text + "\n" + IMAGE_DATA_TAG_TEMPLATE.format(mime="image/png", b64=b64)
            )
        return text

    return [compose_images]
