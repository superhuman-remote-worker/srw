"""The harness slots: where a connection waits for SRW's built-in tools.

A managed connection, or the one MCP manager, sits in
``ToolContext.datasources`` under its slot. The tools never name a connector
type to find it: they ask for their own tool category, and the driver specs
tie each category to the driver that delivers a connection for it.
"""

from __future__ import annotations

from shared.connectors.builtin import DATASOURCE_SPECS
from shared.connectors.contract import DriverSpec

#: The forms whose delivery is a connection held in a harness slot.
SLOT_FORMS: tuple[str, ...] = ("managed_connection", "mcp_client")


def connection_slot(spec: DriverSpec | None) -> str | None:
    """The slot a driver's connection takes, or ``None`` if it holds none."""
    if spec is None or not spec.legacy_type:
        return None
    if not any(form in spec.delivery_forms for form in SLOT_FORMS):
        return None
    return spec.legacy_type


def _slots_by_category() -> dict[str, str]:
    slots: dict[str, str] = {}
    for spec in DATASOURCE_SPECS:
        slot = connection_slot(spec)
        if slot is None or not spec.tool_category:
            continue
        if spec.tool_category in slots:
            raise RuntimeError(
                f"tool category {spec.tool_category!r} has two connection slots"
            )
        slots[spec.tool_category] = slot
    return slots


#: Tool category to the slot its tools read.
SLOT_BY_CATEGORY: dict[str, str] = _slots_by_category()


def slot_for_category(category: str) -> str | None:
    """The slot a tool category's tools read (``None``: no connection)."""
    return SLOT_BY_CATEGORY.get(category)
