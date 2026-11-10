"""The orchestrator's API reference is served under /api.

/api is the only prefix every exposure mode routes to the orchestrator: the
cockpit host (sameOriginApi) and single-origin installs send the rest to the
cockpit. The Automations page links to <apiUrl>/api/docs, which 404'd while the
reference sat at FastAPI's /docs default.
"""

from fastapi.testclient import TestClient


def test_reference_routes_live_under_the_api_prefix() -> None:
    import orchestrator.main

    client = TestClient(orchestrator.main.app)

    spec = client.get("/api/openapi.json")
    assert spec.status_code == 200
    assert spec.json()["info"]["title"] == "SRW API"
    assert "/api/automations" in spec.json()["paths"]

    for path in ("/api/docs", "/api/redoc"):
        page = client.get(path)
        assert page.status_code == 200, path
        assert "/api/openapi.json" in page.text, path

    for old in ("/docs", "/redoc", "/openapi.json"):
        assert client.get(old).status_code == 404, old
