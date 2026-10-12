"""Compare the static inventory with small real FastAPI compositions, offline."""

import importlib
import sys
import textwrap

import pytest

from tests.test_endpoint_inventory import _load_script


@pytest.fixture
def application_sources(tmp_path, monkeypatch):
    packages = []
    monkeypatch.syspath_prepend(str(tmp_path))

    def write(files):
        name = f"route_fixture_{len(packages)}"
        packages.append(name)
        directory = tmp_path / name
        directory.mkdir()
        (directory / "__init__.py").write_text("")
        for relative, source in files.items():
            path = directory / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(textwrap.dedent(source).replace("__PACKAGE__", name))
        return directory / "main.py", name

    yield write

    for key in list(sys.modules):
        if any(key == name or key.startswith(name + ".") for name in packages):
            del sys.modules[key]


def mounted_sequence(app):
    """Read actual FastAPI route contexts, in match order, incl. hidden HTTP and WS.

    New FastAPI versions retain included routers; older versions copy their
    routes directly. This compatibility stays in the test oracle, not the
    dependency-free source scanner. Framework-generated Starlette docs routes
    are intentionally outside the declared-application-route scope.
    """
    from fastapi import routing

    iterator = getattr(routing, "_iter_routes_with_context", None)
    routes = (
        iterator(app.routes) if iterator else ((route, None) for route in app.routes)
    )
    result = []
    for route, context in routes:
        effective = getattr(context, "starlette_route", None)
        if effective is not None:
            path = effective.path
        else:
            path = context.path if context is not None else route.path
        if isinstance(route, routing.APIRoute):
            result.extend((method, path) for method in sorted(route.methods))
        elif isinstance(route, routing.APIWebSocketRoute):
            result.append(("WS", path))
    return result


def mounted_identities(app):
    return sorted(mounted_sequence(app))


def identities(routes):
    return sorted((route.method, route.path) for route in routes)


def test_composed_routers_match_real_fastapi_including_prefixes_aliases_and_ws(
    application_sources,
):
    main, package = application_sources(
        {
            "main.py": """
            from fastapi import FastAPI
            from .routers import mounted as api
            from . import unused

            app = FastAPI()

            @app.get('/api/direct')
            def direct():
                return None

            app.include_router(api, prefix='/api')
            app.include_router(router=api, prefix='/auth', include_in_schema=False)
        """,
            "routers/__init__.py": """
            from fastapi import APIRouter as Router
            from .leaf import router as child

            mounted: Router = Router(prefix='/v1')
            mounted.include_router(child, prefix='/nested')
        """,
            "routers/leaf.py": """
            import fastapi as api

            router = api.APIRouter(prefix='/inner', include_in_schema=False)
            unused = api.APIRouter(prefix='/unused')

            @router.get('/item')
            def item():
                return None

            @router.api_route(path='/multi', methods=('GET', 'HEAD'))
            def multi():
                return None

            @router.api_route('/default')
            def default():
                return None

            @router.websocket('/socket')
            async def socket(websocket):
                pass

            @unused.get('/not-mounted')
            def not_mounted():
                return None
        """,
            "unused.py": """
            from fastapi import APIRouter
            router = APIRouter(prefix='/api/ghost')
            @router.get('/never-mounted')
            def ghost():
                pass
        """,
        }
    )
    script = _load_script()
    found = script.discover_routes(main)
    app = importlib.import_module(package + ".main").app

    assert identities(found) == mounted_identities(app)
    assert len(found) == 11
    assert ("WS", "/api/v1/nested/inner/socket") in identities(found)
    assert ("HEAD", "/auth/v1/nested/inner/multi") in identities(found)
    assert not any("unused" in route.path or "ghost" in route.path for route in found)
    assert not any(
        route.method == "HEAD" and route.path.endswith("/item") for route in found
    )


@pytest.mark.parametrize(
    ("import_source", "reference"),
    [
        ("from . import routes as api", "api.router"),
        ("import __PACKAGE__.routes as api", "api.router"),
        ("import __PACKAGE__.routes", "__PACKAGE__.routes.router"),
    ],
)
def test_imported_module_aliases_resolve_the_same_mounted_router(
    application_sources, import_source, reference
):
    main, package = application_sources(
        {
            "main.py": f"from fastapi import FastAPI\n{import_source}\napp = FastAPI()\napp.include_router({reference}, prefix='/api')\n",
            "routes.py": "from fastapi import APIRouter\nrouter = APIRouter(prefix='/v1')\n@router.get('/x')\ndef route(): pass\n",
        }
    )
    script = _load_script()
    assert (
        identities(script.discover_routes(main))
        == mounted_identities(importlib.import_module(package + ".main").app)
        == [("GET", "/api/v1/x")]
    )


def test_route_identity_and_gate_label_survive_a_main_to_router_move(
    application_sources,
):
    before, before_package = application_sources(
        {
            "main.py": """
            from fastapi import FastAPI
            app = FastAPI()
            def require_approved_user():
                pass
            @app.api_route('/api/contacts', methods=['GET', 'HEAD'])
            def contacts():
                require_approved_user()
        """,
        }
    )
    after, after_package = application_sources(
        {
            "main.py": """
            from fastapi import FastAPI
            from .contacts import router
            app = FastAPI()
            app.include_router(router, prefix='/api')
        """,
            "contacts.py": """
            from fastapi import APIRouter
            router = APIRouter()
            def require_approved_user():
                pass
            @router.api_route('/contacts', methods=['GET', 'HEAD'])
            def contacts():
                require_approved_user()
        """,
        }
    )
    script = _load_script()
    for main, package in ((before, before_package), (after, after_package)):
        assert identities(script.discover_routes(main)) == mounted_identities(
            importlib.import_module(package + ".main").app
        )
    assert script.render_manifest(
        script.collect_endpoints(before)
    ) == script.render_manifest(script.collect_endpoints(after))


def test_discovery_does_not_import_application_code_and_policy_scope_is_explicit(
    application_sources,
):
    main, _package = application_sources(
        {
            "main.py": """
            from fastapi import FastAPI
            raise RuntimeError('production must never be imported by this gate')
            app = FastAPI()
            @app.get('/debug/only')
            def debug():
                pass
            @app.get('/api/public')
            def public():
                pass
        """,
        }
    )
    script = _load_script()
    routes = script.discover_routes(main)
    assert identities(routes) == [("GET", "/api/public"), ("GET", "/debug/only")]
    assert identities(script.collect_endpoints(main)) == [("GET", "/api/public")]


def test_literal_branch_exclusion_and_explicit_http_methods(application_sources):
    main, package = application_sources(
        {
            "main.py": """
            from fastapi import FastAPI, APIRouter
            app = FastAPI()
            router = APIRouter(prefix='/api')
            @router.head('/item')
            @router.options('/item')
            @router.trace('/item')
            def item():
                pass
            if False:
                @router.get('/disabled')
                def disabled():
                    pass
            if True:
                app.include_router(router)
        """,
        }
    )
    script = _load_script()
    assert (
        identities(script.discover_routes(main))
        == mounted_identities(importlib.import_module(package + ".main").app)
        == [("HEAD", "/api/item"), ("OPTIONS", "/api/item"), ("TRACE", "/api/item")]
    )


def test_classification_remains_separate_from_router_dependencies(application_sources):
    main, _package = application_sources(
        {
            "main.py": """
            from fastapi import FastAPI, Depends, APIRouter
            def require_approved_user():
                pass
            router = APIRouter(dependencies=[Depends(require_approved_user)])
            # nosec: public fixture-owned callback
            @router.api_route('/public', methods=['GET', 'POST'])
            def public():
                pass
            @router.get('/implicit')
            def implicit():
                pass
            app = FastAPI()
            app.include_router(router, prefix='/api', dependencies=[Depends(require_approved_user)])
        """,
        }
    )
    script = _load_script()
    routes = script.discover_routes(main)
    assert not hasattr(routes[0], "classification")
    endpoints = script.classify_routes(routes, main_path=main)
    assert {
        route.classification for route in endpoints if route.path == "/api/public"
    } == {"public:fixture-owned callback"}
    assert (
        next(
            route for route in endpoints if route.path == "/api/implicit"
        ).classification
        == "unscoped"
    )


def test_application_dependency_state_does_not_change_route_identity(
    application_sources,
):
    main, package = application_sources(
        {
            "main.py": """
            from fastapi import FastAPI, Depends
            app = FastAPI()
            app.state.contacts_dependencies = object()
            def original_dependency():
                return None
            def replacement_dependency():
                return None
            app.dependency_overrides[original_dependency] = replacement_dependency
            @app.get('/api/contacts')
            def contacts(dependency=Depends(original_dependency)):
                return None
        """,
        }
    )
    script = _load_script()
    assert (
        identities(script.discover_routes(main))
        == mounted_identities(importlib.import_module(package + ".main").app)
        == [("GET", "/api/contacts")]
    )


def test_repository_declarations_match_the_assembled_application():
    from orchestrator.main import app

    script = _load_script()
    assert identities(script.discover_routes()) == mounted_identities(app)


@pytest.mark.parametrize(
    ("body", "reason"),
    [
        ("@app.get(PATH)\ndef route(): pass", "route path must be a literal"),
        (
            "@app.api_route('/api/x', methods=METHODS)\ndef route(): pass",
            "methods must be a nonempty literal",
        ),
        (
            "@app.api_route('/api/x', methods=[])\ndef route(): pass",
            "methods must be a nonempty literal",
        ),
        ("@app.get('/api/x', **options)\ndef route(): pass", "expanded decorator"),
        (
            "router = APIRouter(prefix=PREFIX)\napp.include_router(router)",
            "router prefix must be a literal",
        ),
        (
            "router = APIRouter()\napp.include_router(router, prefix=PREFIX)",
            "router prefix must be a literal",
        ),
        ("app.include_router(make_router())", "requires a named APIRouter"),
        (
            "if ENABLED:\n    @app.get('/api/x')\n    def route(): pass",
            "conditional, looped or nested",
        ),
        (
            "for router in routers:\n    app.include_router(router)",
            "conditional, looped or nested",
        ),
        ("app.mount('/api', other_app)", "mount route mutation"),
        ("app.add_api_route('/api/x', handler)", "add_api_route route mutation"),
        (
            "@app.websocket_route('/api/x')\nasync def socket(ws): pass",
            "websocket_route registration",
        ),
        (
            "router = APIRouter()\nresult = app.include_router(router)",
            "registration inside an assignment",
        ),
        ("app.router.routes = []", "assignment to router state"),
        ("app.routes.append(route)", "append route mutation"),
        (
            "register = app.get\n@register('/api/x')\ndef route(): pass",
            "dynamic route decorator",
        ),
        ("configure_routes(app)", "registration helper"),
        ("helpers.configure_routes(app)", "registration helper"),
        (
            "router = APIRouter()\napp.include_router(router)\n@router.get('/api/late')\ndef late(): pass",
            "registration after include_router",
        ),
        (
            "router = APIRouter()\nrouter.include_router(router)\napp.include_router(router)",
            "cyclic include_router",
        ),
    ],
)
def test_unsupported_registration_fails_instead_of_silently_omitting_routes(
    application_sources, body, reason
):
    main, _package = application_sources(
        {
            "main.py": "from fastapi import FastAPI, APIRouter\napp = FastAPI()\n"
            + body
            + "\n",
        }
    )
    script = _load_script()
    with pytest.raises(script.UnsupportedRouteError, match=reason) as raised:
        script.discover_routes(main)
    assert str(main) in str(raised.value)


def test_dynamic_app_factory_is_explicitly_unsupported(application_sources):
    main, _package = application_sources(
        {
            "main.py": "app = create_app()\n@app.get('/api/x')\ndef route(): pass\n",
        }
    )
    script = _load_script()
    with pytest.raises(script.UnsupportedRouteError, match="named FastAPI instance"):
        script.discover_routes(main)


def test_delegated_browser_websocket_retains_the_audited_gate_label(
    application_sources,
):
    main, _package = application_sources(
        {
            "main.py": """
            from fastapi import FastAPI
            from service import relay_browser_stream
            app = FastAPI()
            @app.websocket('/api/persistent/threads/{thread_id}/browser/stream')
            async def browser(ws, thread_id):
                await relay_browser_stream(ws, thread_id, db=database)
        """,
        }
    )
    script = _load_script()
    endpoints = script.collect_endpoints(main)
    assert [(route.method, route.classification) for route in endpoints] == [
        ("WS", "gated:relay_browser_stream")
    ]


def test_registration_on_an_imported_router_is_explicitly_unsupported(
    application_sources,
):
    main, _package = application_sources(
        {
            "main.py": """
            from fastapi import FastAPI
            from .routes import router
            app = FastAPI()
            @router.get('/api/x')
            def route(): pass
            app.include_router(router)
        """,
            "routes.py": "from fastapi import APIRouter\nrouter = APIRouter()\n",
        }
    )
    script = _load_script()
    with pytest.raises(
        script.UnsupportedRouteError, match="registration on an imported router"
    ):
        script.discover_routes(main)


def test_cli_reports_excluded_routes_and_refuses_unsupported_composition(
    application_sources, monkeypatch, capsys
):
    main, _package = application_sources(
        {
            "main.py": "from fastapi import FastAPI\napp = FastAPI()\n@app.get('/outside')\ndef outside(): pass\n",
        }
    )
    script = _load_script()
    routes = script.discover_routes(main)
    monkeypatch.setattr(script, "discover_routes", lambda: routes)
    monkeypatch.setattr(sys, "argv", ["check_endpoint_auth.py"])
    assert script.main() == 0
    captured = capsys.readouterr()
    assert "GET /outside" in captured.err
    assert "/outside" not in captured.out

    def unsupported():
        raise script.UnsupportedRouteError("fixture.py:9: dynamic prefix")

    monkeypatch.setattr(script, "discover_routes", unsupported)
    monkeypatch.setattr(sys, "argv", ["check_endpoint_auth.py", "--write"])
    manifest = main.parent / "inventory.txt"
    monkeypatch.setattr(script, "MANIFEST", manifest)
    assert script.main() == 2
    assert not manifest.exists()
    assert "fixture.py:9: dynamic prefix" in capsys.readouterr().err


@pytest.mark.parametrize(
    "gate_call", ["require_admin()", "dependencies.require_admin()"]
)
def test_extracted_admin_gate_retains_the_existing_inventory_label(
    application_sources, gate_call
):
    before, _ = application_sources(
        {
            "main.py": """
            from fastapi import FastAPI
            app = FastAPI()
            @app.get('/api/tables')
            def list_tables():
                _require_admin()
        """
        }
    )
    after, _ = application_sources(
        {
            "main.py": """
                from fastapi import FastAPI
                from .tables import router
                app = FastAPI()
                app.include_router(router)
            """,
            "tables.py": f"""
                from fastapi import APIRouter
                router = APIRouter(prefix='/api/tables')
                @router.get('')
                def list_tables():
                    require_approved_user()
                    {gate_call}
            """,
        }
    )
    script = _load_script()
    before_manifest = script.render_manifest(script.collect_endpoints(before))
    after_manifest = script.render_manifest(script.collect_endpoints(after))
    assert "admin:_require_admin" in before_manifest
    assert after_manifest == before_manifest


# -- Function form: main.py calls a factory, application/routes.py registers --

_FACTORY_MAIN = """
from .application import create_app

app = create_app()
"""

_FACTORY_PACKAGE = """
from fastapi import FastAPI

from .routes import include_routers


def create_app() -> FastAPI:
    app = FastAPI(title='fixture')
    include_routers(app)
    return app
"""


def _factory_application(files):
    return {
        "main.py": _FACTORY_MAIN,
        "application/__init__.py": _FACTORY_PACKAGE,
        **files,
    }


def test_registration_function_is_discovered_in_include_order_like_real_fastapi(
    application_sources,
):
    main, package = application_sources(
        _factory_application(
            {
                "application/routes.py": """
                \"\"\"The application's router registration.\"\"\"
                from fastapi import FastAPI

                from .. import settings
                from ..routers import alpha as alpha_routes
                from ..routers.zeta import router as zeta_router


                def include_routers(app: FastAPI) -> None:
                    \"\"\"Register every router, in match order.\"\"\"
                    app.include_router(zeta_router)
                    settings.configure(store_factory=lambda: settings.STORE)
                    app.include_router(alpha_routes.router, prefix='/api')
                    settings.configure_from_environment(settings.STORE)
                    app.router.include_router(alpha_routes.end_router)
                """,
                "settings.py": """
                STORE = object()
                def configure(*, store_factory):
                    return None
                def configure_from_environment(store):
                    return None
                """,
                "routers/__init__.py": "",
                "routers/zeta.py": """
                from fastapi import APIRouter
                router = APIRouter(prefix='/api/zeta')
                @router.get('/first')
                def first():
                    return None
                """,
                "routers/alpha.py": """
                from fastapi import APIRouter
                router = APIRouter(prefix='/v1')
                end_router = APIRouter(prefix='/api/end')
                unused = APIRouter(prefix='/api/unused')
                @router.api_route('/item', methods=['GET', 'HEAD'])
                def item():
                    return None
                @router.websocket('/socket')
                async def socket(websocket):
                    pass
                @end_router.post('/last')
                def last():
                    return None
                @unused.get('/ghost')
                def ghost():
                    return None
                """,
            }
        )
    )
    script = _load_script()
    app = importlib.import_module(package + ".main").app

    assert identities(script.discover_routes(main)) == mounted_identities(app)
    discovery = script._RouteDiscovery(main)
    application = discovery.registration_function(
        main.parent / "application" / "routes.py", "include_routers"
    )
    ordered = [(route.method, route.path) for route in discovery.expand(application)]
    assert ordered == mounted_sequence(app)
    assert ordered == [
        ("GET", "/api/zeta/first"),
        ("GET", "/api/v1/item"),
        ("HEAD", "/api/v1/item"),
        ("WS", "/api/v1/socket"),
        ("POST", "/api/end/last"),
    ]


def test_moved_routes_keep_identity_nosec_public_and_gate_labels(
    application_sources,
):
    readiness = """
    # nosec: public auth-bootstrap (Bearer-required, intentionally pre-approval — onboarding first paint)
    @{owner}.get('/api/system/readiness')
    async def system_readiness(request: Request):
        await get_current_user(request, postgres_db)
    """
    project_job = """
    @{owner}.post(
        '/api/projects/{{project_id}}/jobs',
        operation_id='create_project_job_api_projects__project_id__jobs_post',
    )
    async def create_project_job(request: Request, project_id: str):
        await require_project_member(request, postgres_db, project_id, min_role='editor')
    """
    jobs = """
    from fastapi import APIRouter
    router = APIRouter(prefix='/api/jobs')
    @router.get('')
    def list_jobs():
        require_approved_user()
    """

    def joined(*parts):
        return "".join(textwrap.dedent(part) for part in parts)

    before, before_package = application_sources(
        {
            "main.py": joined(
                """
                from fastapi import FastAPI, Request
                from .routers.jobs import router as jobs_router
                app = FastAPI()
                app.include_router(jobs_router)
                """,
                readiness.format(owner="app"),
                project_job.format(owner="app"),
            ),
            "routers/__init__.py": "",
            "routers/jobs.py": jobs,
        }
    )
    after, after_package = application_sources(
        _factory_application(
            {
                "application/routes.py": """
                from fastapi import FastAPI
                from ..routers.jobs import router as jobs_router
                from ..routers.system_readiness import router as system_readiness_router
                from ..routers.project_jobs import router as project_jobs_router
                def include_routers(app: FastAPI) -> None:
                    app.include_router(jobs_router)
                    app.include_router(system_readiness_router)
                    app.include_router(project_jobs_router)
                """,
                "routers/__init__.py": "",
                "routers/jobs.py": jobs,
                "routers/system_readiness.py": joined(
                    "from fastapi import APIRouter, Request\nrouter = APIRouter()\n",
                    readiness.format(owner="router"),
                ),
                "routers/project_jobs.py": joined(
                    "from fastapi import APIRouter, Request\nrouter = APIRouter()\n",
                    project_job.format(owner="router"),
                ),
            }
        )
    )
    script = _load_script()
    for main, package in ((before, before_package), (after, after_package)):
        app = importlib.import_module(package + ".main").app
        assert identities(script.discover_routes(main)) == mounted_identities(app)
    endpoints = script.collect_endpoints(after)
    assert script.render_manifest(endpoints) == script.render_manifest(
        script.collect_endpoints(before)
    )
    assert {(e.method, e.path): e.classification for e in endpoints} == {
        ("GET", "/api/jobs"): "gated:require_approved_user",
        ("GET", "/api/system/readiness"): (
            "public:auth-bootstrap (Bearer-required, intentionally "
            "pre-approval — onboarding first paint)"
        ),
        ("POST", "/api/projects/{project_id}/jobs"): "gated:require_project_member",
    }


_ROUTES_MODULE = """from fastapi import APIRouter
router = APIRouter(prefix='/api/x')
@router.get('/item')
def item(): pass
"""


@pytest.mark.parametrize(
    ("body", "reason"),
    [
        (
            "if ENABLED:\n        app.include_router(router)",
            "conditional, looped or nested",
        ),
        (
            "for item in [router]:\n        app.include_router(item)",
            "conditional, looped or nested",
        ),
        (
            "try:\n        app.include_router(router)\n    finally:\n        pass",
            "conditional, looped or nested",
        ),
        (
            "with context():\n        app.include_router(router)",
            "conditional, looped or nested",
        ),
        ("def nested():\n        pass", "conditional, looped or nested"),
        ("alias = app\n    alias.include_router(router)", "Assign is not supported"),
        ("return app.include_router(router)", "Return is not supported"),
        ("register(app)", "registration helper"),
        ("helpers.register(app=app)", "registration helper"),
        ("configure(store=(alias := app))", "assignment expressions"),
        ("app.mount('/api', other_app)", "mount route mutation"),
        ("app.add_api_route('/api/y', handler)", "add_api_route route mutation"),
        ("app.include_router(make_router())", "requires a named APIRouter"),
        ("app.include_router(app)", "requires a named APIRouter"),
        (
            "app.include_router(router, prefix=PREFIX)",
            "router prefix must be a literal",
        ),
    ],
)
def test_unsupported_registration_function_body_fails(
    application_sources, body, reason
):
    main, _package = application_sources(
        _factory_application(
            {
                "application/routes.py": _ROUTES_MODULE
                + f"def include_routers(app):\n    {body}\n",
            }
        )
    )
    script = _load_script()
    with pytest.raises(script.UnsupportedRouteError, match=reason) as raised:
        script.discover_routes(main)
    assert str(main.parent / "application" / "routes.py") in str(raised.value)


@pytest.mark.parametrize(
    ("definition", "reason"),
    [
        (
            "def other(app):\n    app.include_router(router)",
            r"include_routers\(\) is not defined",
        ),
        (
            "async def include_routers(app):\n    app.include_router(router)",
            "plain, undecorated synchronous",
        ),
        (
            "@decorate\ndef include_routers(app):\n    app.include_router(router)",
            "plain, undecorated synchronous",
        ),
        (
            "def include_routers(app, extra):\n    app.include_router(extra)",
            "exactly one parameter",
        ),
        (
            "def include_routers(app, *, extra=None):\n    app.include_router(router)",
            "exactly one parameter",
        ),
        ("def include_routers():\n    pass", "exactly one parameter"),
        (
            "def include_routers(app):\n    pass\n"
            "def include_routers(app):\n    app.include_router(router)",
            "exactly once",
        ),
        (
            "def include_routers(app):\n    app.include_router(router)\n"
            "include_routers = other",
            "exactly once",
        ),
        (
            "if ENABLED:\n"
            "    def include_routers(app):\n        app.include_router(router)",
            "exactly once",
        ),
    ],
)
def test_registration_function_shape_is_exact(application_sources, definition, reason):
    main, _package = application_sources(
        _factory_application({"application/routes.py": _ROUTES_MODULE + definition})
    )
    script = _load_script()
    with pytest.raises(script.UnsupportedRouteError, match=reason):
        script.discover_routes(main)


@pytest.mark.parametrize(
    ("main_source", "reason"),
    [
        (
            "from fastapi import FastAPI\napp = FastAPI()\n",
            "exactly one composition entry",
        ),
        (
            _FACTORY_MAIN + "@app.get('/api/late')\ndef late(): pass\n",
            r"route registration on 'app' outside include_routers\(\)",
        ),
        (
            _FACTORY_MAIN + "app.include_router(router)\n",
            r"route registration on 'app' outside include_routers\(\)",
        ),
        (
            _FACTORY_MAIN + "if DEBUG:\n    app.include_router(router)\n",
            "conditional, looped or nested",
        ),
        (_FACTORY_MAIN + "register(app)\n", "registration helper"),
        (_FACTORY_MAIN + "app.mount('/api', other)\n", "mount route mutation"),
        (
            "from fastapi import APIRouter\napp = APIRouter()\n",
            "must be built by the application factory",
        ),
    ],
)
def test_function_form_main_module_must_not_register_on_the_application(
    application_sources, main_source, reason
):
    main, _package = application_sources(
        _factory_application(
            {
                "main.py": main_source,
                "application/routes.py": _ROUTES_MODULE
                + "def include_routers(app):\n    app.include_router(router)\n",
            }
        )
    )
    script = _load_script()
    with pytest.raises(script.UnsupportedRouteError, match=reason) as raised:
        script.discover_routes(main)
    assert str(main) in str(raised.value)


def test_function_form_main_module_may_configure_the_application(
    application_sources,
):
    """State, middleware and handlers on the factory app are not registration."""
    main, package = application_sources(
        _factory_application(
            {
                "main.py": _FACTORY_MAIN
                + """
from starlette.middleware.gzip import GZipMiddleware

app.state.store = object()
app.add_middleware(GZipMiddleware)

@app.exception_handler(ValueError)
async def value_error(request, exc):
    return None

@app.middleware('http')
async def passthrough(request, call_next):
    return await call_next(request)

async def lifespan(app):
    yield app.state.store

if __name__ == '__main__':
    run(app)
""",
                "application/routes.py": _ROUTES_MODULE
                + "def include_routers(app):\n    app.include_router(router)\n",
            }
        )
    )
    script = _load_script()
    assert (
        identities(script.discover_routes(main))
        == mounted_identities(importlib.import_module(package + ".main").app)
        == [("GET", "/api/x/item")]
    )


def test_function_form_entry_is_an_explicit_constant():
    script = _load_script()
    assert script.APPLICATION_ROUTES == (
        script.ORCHESTRATOR / "application" / "routes.py"
    )
    assert script.MAIN_PY.parent / script.APPLICATION_ROUTES_RELATIVE == (
        script.APPLICATION_ROUTES
    )
    assert script.ROUTES_FUNCTION == "include_routers"
