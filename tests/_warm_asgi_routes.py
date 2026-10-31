"""Build an in-process app's routes before a test times a request.

FastAPI (0.137+) builds each included router's routes on the first request
that walks them. For ``orchestrator.main.app`` that is about 600 routes and
0.2-1 s of synchronous work, more on a loaded CI worker. A test that sends
the agent's calls to the app through ``httpx.ASGITransport`` runs that work
on its own event loop. Inside a deadline such as the subagent runtime's 5 s
ledger timeout, it fails the first call. A deployed orchestrator has built
its routes by the time an agent calls it, so it never charges that call.

An unmatched path walks every router, so one such request builds them all.
It goes through its own client: the routes are built once per app, and the
test's client and its request hooks see none of it.
"""

from __future__ import annotations

from typing import Any

import httpx


async def warm_asgi_routes(app: Any) -> None:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://warm.test"
    ) as client:
        response = await client.get("/__warm_asgi_routes__")
    assert response.status_code == 404
