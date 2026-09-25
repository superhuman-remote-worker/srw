"""C2 — endpoint inventory snapshot test.

Re-runs ``scripts/check_endpoint_auth.py`` against the declared application —
``app`` in ``src/orchestrator/main.py``, or ``include_routers(app)`` in
``src/orchestrator/application/routes.py`` once main.py only calls the
application factory — and its included routers. Mounted HTTP methods and
WebSockets under /api, /auth and /wopi must match the committed manifest at
``policy/endpoint_inventory.txt``. Unmounted routers and framework-generated
routes are excluded; unsupported dynamic composition fails explicitly.

When a new endpoint is added (or an existing endpoint's gate changes), this
test fails until either:

  * a gate is added (``Depends(require_*)`` or an in-body ``require_*`` call),
    and ``python scripts/check_endpoint_auth.py --write`` is re-run; **or**
  * the endpoint is deliberately public, in which case mark it with
    ``# nosec: public <reason>`` on the line immediately above the
    app or router route decorator, then re-run ``--write``.

The snapshot makes route and source-gate changes reviewable. Gate-name labels
do not prove authorization behavior; dedicated access-control tests do that.
"""

import difflib
import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "check_endpoint_auth.py"
MANIFEST = REPO_ROOT / "policy" / "endpoint_inventory.txt"


def _load_script():
    spec = importlib.util.spec_from_file_location("check_endpoint_auth", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_endpoint_auth"] = module
    spec.loader.exec_module(module)
    return module


def test_missing_source_is_an_error(tmp_path):
    script = _load_script()
    with pytest.raises(FileNotFoundError, match="route source is missing"):
        script.collect_endpoints(tmp_path / "missing" / "main.py")


def test_existing_application_may_have_no_endpoints(tmp_path):
    script = _load_script()
    main = tmp_path / "main.py"
    main.write_text("from fastapi import FastAPI\napp = FastAPI()\n")
    assert script.collect_endpoints(main) == []


def test_source_without_a_composition_entry_is_an_error(tmp_path):
    """No FastAPI instance and no ``include_routers``: loud, never empty.

    Once main.py only calls an application factory, a renamed or missing
    registration function must not read as an application without routes.
    """
    script = _load_script()
    main = tmp_path / "main.py"
    main.write_text("app = object()\n")
    with pytest.raises(script.UnsupportedRouteError, match="named FastAPI instance"):
        script.collect_endpoints(main)


def test_endpoint_inventory_matches_manifest():
    """The manifest must reflect the mounted application and its source gates."""
    script = _load_script()
    endpoints = script.collect_endpoints()
    rendered = script.render_manifest(endpoints)
    on_disk = MANIFEST.read_text()
    if rendered != on_disk:
        diff = "\n".join(
            difflib.unified_diff(
                on_disk.splitlines(),
                rendered.splitlines(),
                fromfile=str(MANIFEST.relative_to(REPO_ROOT)),
                tofile="<regenerated>",
                lineterm="",
            )
        )
        pytest.fail(
            "Endpoint inventory is stale. Run:\n\n"
            "    python scripts/check_endpoint_auth.py --write\n\n"
            "and commit the result. Diff:\n\n" + diff
        )


def test_no_new_unscoped_endpoints():
    """The number of unscoped endpoints must not increase.

    Neither main.py nor any mounted router carries an unscoped endpoint. The
    last five — the ``/api/uploads`` routes, which surfaced when the walk was
    extended to include_router-mounted modules and took no user identity of
    any kind — were gated in the 2026-08-27 security audit follow-up
    (``tests/test_uploads_security.py``). New unscoped endpoints have to be
    acknowledged either by adding a gate or by marking them
    ``# nosec: public <reason>`` above the decorator.
    """
    script = _load_script()
    endpoints = script.collect_endpoints()
    manifest_unscoped = [
        line for line in MANIFEST.read_text().splitlines() if line.endswith("unscoped")
    ]
    current_unscoped = [e for e in endpoints if e.classification == "unscoped"]
    assert len(current_unscoped) <= len(manifest_unscoped), (
        f"New unscoped endpoint(s) added: {len(current_unscoped)} now vs "
        f"{len(manifest_unscoped)} grandfathered. Gate the new endpoint(s) "
        "with `Depends(require_*)` / in-body `require_*`, or annotate with "
        "`# nosec: public <reason>` above the decorator."
    )


def test_vm_resource_inventory_publish_has_its_distinct_hmac_gate():
    endpoints = _load_script().collect_endpoints()
    matches = [
        e
        for e in endpoints
        if e.path == "/api/internal/vm-resource-inventory/publish"
        and e.method.upper() == "POST"
    ]
    assert len(matches) == 1
    assert matches[0].classification == "internal:require_vm_inventory_authority"
