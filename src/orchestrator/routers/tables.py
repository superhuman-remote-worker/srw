"""Read-only, admin-gated PostgreSQL table inspection."""

from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from orchestrator.database import ALLOWED_TABLES
from orchestrator.security.access import (
    log_security_event,
    mcp_scope_project_id,
    require_admin,
)

router = APIRouter(prefix="/api/tables")


@dataclass
class TablesDependencies:
    """The application owns the database and supplies its authorization gate."""

    db: Any
    require_admin: Callable[[Request, Any], Awaitable[dict[str, Any]]] = require_admin


def get_tables_dependencies(request: Request) -> TablesDependencies:
    return request.app.state.tables_dependencies


async def _refuse_project_scoped_token(
    request: Request, db: Any, user: dict[str, Any]
) -> None:
    """Keep a project-scoped MCP token off the raw tables (N35).

    The admin gate passes for an admin's ``project:<uuid>`` token, but these
    reads span every project and nothing here could apply the scope, so the
    token is refused rather than widened to the whole database.
    """
    if mcp_scope_project_id(user) is None:
        return
    await log_security_event(
        db,
        resource_type="admin_tables",
        user=user,
        detail="Project-scoped MCP token cannot read raw tables",
        request=request,
    )
    raise HTTPException(status_code=403, detail="Access denied by MCP token scope")


@router.get("")
async def list_tables(
    request: Request,
    *,
    dependencies: TablesDependencies = Depends(get_tables_dependencies),
) -> list[dict[str, Any]]:
    """List available tables with row counts. **Admin only** (P4d) —
    raw postgres table dump."""
    user = await dependencies.require_admin(request, dependencies.db)
    await _refuse_project_scoped_token(request, dependencies.db, user)
    return await dependencies.db.get_tables()


@router.get("/{table_name}")
async def get_table_data(
    request: Request,
    table_name: str,
    page: int = Query(default=1, ge=-1),
    page_size: int = Query(default=50, ge=1, le=500, alias="pageSize"),
    *,
    dependencies: TablesDependencies = Depends(get_tables_dependencies),
) -> dict[str, Any]:
    """Get paginated table data. Use page=-1 to request the last page.
    **Admin only** (P4d) — raw postgres rows."""
    user = await dependencies.require_admin(request, dependencies.db)
    await _refuse_project_scoped_token(request, dependencies.db, user)
    if table_name not in ALLOWED_TABLES:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

    try:
        return await dependencies.db.get_table_data(table_name, page, page_size)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/{table_name}/schema")
async def get_table_schema(
    request: Request,
    table_name: str,
    *,
    dependencies: TablesDependencies = Depends(get_tables_dependencies),
) -> list[dict[str, Any]]:
    """Get column definitions for a table. **Admin only** (P4d)."""
    user = await dependencies.require_admin(request, dependencies.db)
    await _refuse_project_scoped_token(request, dependencies.db, user)
    if table_name not in ALLOWED_TABLES:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

    try:
        return await dependencies.db.get_table_schema(table_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
