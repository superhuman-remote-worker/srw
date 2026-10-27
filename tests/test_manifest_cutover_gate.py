"""Safety and real-resource protocol checks for the explicitly run HTTP gate."""

import asyncio
from copy import deepcopy
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from fastapi import FastAPI, HTTPException
import httpx
import pytest

from tests import test_manifest_native_full_schema as full_schema

actor = full_schema.actor
database = full_schema.database
postgres_url = full_schema.postgres_url
PREFIX = "cutover-123456789abc-"


@pytest.fixture
def gate_module():
    path = Path(__file__).resolve().parents[1] / "scripts/manifests-cutover-k3d-gate.py"
    spec = importlib.util.spec_from_file_location("manifest_cutover_gate", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def counts(**changes):
    return {
        "activeOwnedResources": 0,
        "ownedProjects": 0,
        "ownedRevisions": 0,
        "legacyExpertPayloads": 0,
        "legacyProjectPayloads": 0,
        "rejectedResources": 0,
        "rejectedOperations": 0,
        "rejectedJobs": 0,
        "rejectedExecutions": 0,
        "rejectedAttempts": 0,
        "rejectedWorkspaceBindings": 0,
        **changes,
    }


def test_gate_fixtures_are_valid_and_have_no_workspace_or_team_effects(gate_module):
    docs = [
        gate_module.authored_expert(PREFIX),
        gate_module.authored_project(PREFIX),
        gate_module.authored_job(PREFIX),
    ]
    gate_module.validate_documents(docs)
    project, job = docs[1:]
    assert "team" not in project["spec"]
    assert job["spec"]["execution"]["workspace"] is None
    assert job["spec"]["execution"]["connectors"] == {}
    assert all(
        (gate_module.ROOT / path).is_file() for path in gate_module.source_paths()
    )
    assert "src/orchestrator/main.py" in gate_module.source_paths()
    assert "src/orchestrator/database/postgres.py" in gate_module.source_paths()


def test_deployment_mismatch_precedes_auth_and_writes(gate_module, monkeypatch, capsys):
    def stop():
        raise gate_module.GateFailure("Deployed cutover source differs.")

    monkeypatch.setattr(gate_module, "deployed_identity", stop)
    monkeypatch.setattr(
        gate_module.httpx,
        "Client",
        lambda **kwargs: pytest.fail(
            "No auth or API client before deployment verification"
        ),
    )
    assert gate_module.main([]) == 1
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "failed"
    assert result["failure"] == "Deployed cutover source differs."


def test_transport_failure_registers_cleanup_without_replaying_mutation(gate_module):
    document = gate_module.authored_expert(PREFIX)
    owned = {
        "resource": deepcopy(document),
        "uid": str(uuid4()),
        "resourceVersion": 3,
    }
    unrelated = deepcopy(owned)
    unrelated["uid"] = str(uuid4())
    unrelated["resource"]["metadata"]["name"] = "someone-elses-resource"
    requests, deleted = [], []

    def respond(request):
        requests.append(request)
        if request.method == "POST":
            raise httpx.ReadTimeout("credential-must-not-appear", request=request)
        if request.url.path == "/api/resources":
            return httpx.Response(200, json={"resources": [owned, unrelated]})
        if request.method == "DELETE":
            assert request.url.path == "/api/resources/" + owned["uid"]
            assert request.url.params["expected_version"] == "3"
            deleted.append(owned["uid"])
            return httpx.Response(200, json={"deleted": True, "uid": owned["uid"]})
        return httpx.Response(404 if deleted else 200, json={} if deleted else owned)

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        gate = gate_module.CutoverGate(
            client, prefix=PREFIX, inspect_db=counts, inspect_native=lambda: []
        )
        gate.headers = {"Authorization": "Bearer private-token"}
        with pytest.raises(
            gate_module.GateFailure, match="outcome is unknown"
        ) as error:
            gate.apply(document)
        assert "credential-must-not-appear" not in str(error.value)
        gate.cleanup()
    assert len([request for request in requests if request.method == "POST"]) == 1
    assert deleted == [owned["uid"]]
    assert gate.evidence["cleanup"]["complete"] is True
    assert "private-token" not in json.dumps(gate.evidence)


def test_cleanup_refuses_changed_ownership_marker(gate_module):
    document = gate_module.authored_expert(PREFIX)
    document["metadata"]["annotations"][gate_module.OWNER_LABEL] = "another-run"

    def respond(request):
        assert request.method == "GET" and request.url.path == "/api/resources"
        return httpx.Response(
            200, json={"resources": [{"resource": document, "uid": str(uuid4())}]}
        )

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        gate = gate_module.CutoverGate(
            client,
            prefix=PREFIX,
            inspect_db=lambda: counts(activeOwnedResources=1),
            inspect_native=lambda: [],
        )
        gate.cleanup_intents.add(("Expert", document["metadata"]["name"]))
        gate.cleanup()
    assert gate.evidence["cleanup"]["complete"] is False
    assert gate.evidence["cleanup"]["failedOperations"] == 1


@pytest.mark.parametrize("opt_in", [True, False])
def test_the_unverified_opt_in_fails_the_gate_early_and_by_name(
    gate_module, monkeypatch, opt_in
):
    import hashlib

    def command(args):
        if "config" in args:
            server = "https://127.0.0.1:6443"
            return json.dumps({"clusters": [{"cluster": {"server": server}}]})
        if "nodes" in args:
            return json.dumps({"items": [{"metadata": {"name": "k3d-srw-server-0"}}]})
        if "pods" in args:
            return json.dumps({"items": []})
        pytest.fail(f"unexpected command {args}")

    files = {
        path: hashlib.sha256((gate_module.ROOT / path).read_bytes()).hexdigest()
        for path in gate_module.source_paths()
    }
    monkeypatch.setattr(gate_module, "command", command)
    monkeypatch.setattr(
        gate_module,
        "remote_json",
        lambda code: {
            "files": files,
            "nativeHostingEnabled": False,
            "unverifiedOptIn": opt_in,
            "nativeNamespace": "srw-native",
        },
    )
    with pytest.raises(gate_module.GateFailure) as failure:
        gate_module.deployed_identity()
    message = str(failure.value)
    if opt_in:
        assert "MANIFEST_ALLOW_APPROVED_IMAGES_WITHOUT_VERIFIED_ISOLATION" in message
        assert "unset the opt-in" in message
    else:  # past the opt-in check: no ready orchestrator in the fake cluster
        assert "ready local orchestrator" in message


def test_arbitrary_api_errors_never_enter_gate_output(gate_module, monkeypatch, capsys):
    monkeypatch.setattr(
        gate_module, "deployed_identity", lambda: {"nativeNamespace": "srw-native"}
    )

    def fail(self):
        raise RuntimeError("Bearer private-token and database-password")

    monkeypatch.setattr(gate_module.CutoverGate, "run", fail)
    assert gate_module.main([]) == 1
    output = capsys.readouterr().out
    assert "private-token" not in output and "database-password" not in output
    assert "RuntimeError" in output


def test_database_probe_is_read_only_and_returns_counts_only(gate_module, monkeypatch):
    captured = []

    def inspect(code):
        compile(code, "<database gate probe>", "exec")
        captured.append(code)
        return counts()

    monkeypatch.setattr(gate_module, "remote_json", inspect)
    assert (
        gate_module.database_evidence(PREFIX, [PREFIX + "job", PREFIX + "rollback"])
        == counts()
    )
    code = captured[0]
    assert "readonly=True" in code and "repeatable_read" in code
    assert "SELECT count(*)" in code
    assert "SELECT *" not in code
    assert "print(json.dumps(result))" in code
    monkeypatch.setattr(
        gate_module, "remote_json", lambda code: {"private": "credential-value"}
    )
    with pytest.raises(gate_module.GateFailure, match="invalid counts"):
        gate_module.database_evidence(PREFIX, [PREFIX + "job"])


@pytest.mark.parametrize("case", ["wrong-code", "persisted-job"])
def test_503_is_not_accepted_without_capability_and_rollback_evidence(
    gate_module, case
):
    db_reads = 0

    def inspect():
        nonlocal db_reads
        db_reads += 1
        return counts(rejectedJobs=1 if case == "persisted-job" and db_reads > 1 else 0)

    def respond(request):
        assert request.method == "POST" and request.url.path == "/api/manifests/apply"
        return httpx.Response(
            503,
            json={
                "detail": {
                    "code": "WrongCode"
                    if case == "wrong-code"
                    else "HostingCapabilityUnavailable"
                }
            },
        )

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        gate = gate_module.CutoverGate(
            client, prefix=PREFIX, inspect_db=inspect, inspect_native=lambda: []
        )
        with pytest.raises(gate_module.GateFailure):
            gate.exercise_disabled_hosting()
    assert not gate.evidence["checks"]


@pytest.mark.asyncio
async def test_project_gate_against_real_routes_and_full_postgres_schema(
    gate_module, database, actor
):
    """Real desired-state routes/DB; no Kubernetes, auth provider or cloud calls."""
    from orchestrator.routers.manifests import (
        ManifestDependencies,
        router as manifest_router,
    )
    from orchestrator.routers.projects import (
        ProjectsDependencies,
        router as projects_router,
    )
    from orchestrator.services.manifest_projects import validate_project_activation
    from orchestrator.services.manifest_resources import ManifestResourceService
    from orchestrator.services.manifests import ManifestService

    user = {**actor, "is_admin": False}

    async def approved(*args, **kwargs):
        return user

    async def project_member(request, store, project_id):
        project = await database.get_project(project_id)
        if not project:
            raise HTTPException(404, "Project not found")
        pytest.fail(
            "The gate must not call the cloud-healing detail route for a live Project"
        )

    async def activate(prepared, user, **kwargs):
        return await validate_project_activation(database, prepared, user, **kwargs)

    resources = ManifestResourceService(database, project_activation=activate)
    app = FastAPI()
    app.state.manifest_dependencies_factory = lambda: ManifestDependencies(
        ManifestService(), approved, resources=resources
    )
    app.state.projects_dependencies_factory = lambda: ProjectsDependencies(
        store=database,
        operations=SimpleNamespace(store=database),
        require_admin=approved,
        require_approved_user=approved,
        require_project_member=project_member,
    )
    app.include_router(manifest_router)
    app.include_router(projects_router)
    loop = asyncio.get_running_loop()

    async def inspect():
        return counts(
            activeOwnedResources=await database.fetchval(
                "SELECT count(*) FROM srw_resources WHERE deleted_at IS NULL"
            ),
            ownedProjects=await database.fetchval("SELECT count(*) FROM projects"),
            ownedRevisions=await database.fetchval(
                "SELECT count(*) FROM srw_resource_revisions"
            ),
            legacyProjectPayloads=await database.fetchval(
                "SELECT count(*) FROM projects WHERE default_config_name IS NOT NULL OR default_config_override IS NOT NULL"
            ),
        )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url=gate_module.API
    ) as api:

        async def forward(request):
            return await api.send(request)

        def send(request):
            response = asyncio.run_coroutine_threadsafe(forward(request), loop).result(
                timeout=30
            )
            # Async transport responses must be materialized for sync httpx.
            return httpx.Response(
                response.status_code, content=response.content, headers=response.headers
            )

        def exercise():
            with httpx.Client(transport=httpx.MockTransport(send)) as client:
                gate = gate_module.CutoverGate(
                    client,
                    prefix=PREFIX,
                    inspect_db=lambda: asyncio.run_coroutine_threadsafe(
                        inspect(), loop
                    ).result(timeout=30),
                    inspect_native=lambda: [],
                )
                try:
                    gate.exercise_project()
                finally:
                    gate.cleanup()
                assert gate.evidence["cleanup"]["complete"] is True
                assert len(gate.evidence["checks"]) == 2

        await asyncio.to_thread(exercise)


def test_cleanup_retires_jobs_before_their_referenced_experts(gate_module):
    expert = gate_module.authored_expert(PREFIX)
    job = gate_module.authored_job(PREFIX)
    resources = [
        {"uid": str(uuid4()), "resource": doc, "resourceVersion": 1}
        for doc in (expert, job)
    ]
    remaining = {item["uid"]: item for item in resources}
    deleted = []

    def respond(request):
        if request.url.path == "/api/resources":
            return httpx.Response(200, json={"resources": list(remaining.values())})
        uid = request.url.path.rsplit("/", 1)[1]
        item = remaining.get(uid)
        if request.method == "DELETE":
            kind = item["resource"]["kind"]
            if kind == "Expert" and any(
                r["resource"]["kind"] == "Job" for r in remaining.values()
            ):
                return httpx.Response(409, json={"detail": "Referenced by a Job"})
            deleted.append(kind)
            del remaining[uid]
            return httpx.Response(200, json={"deleted": True})
        return httpx.Response(200 if item else 404, json=item or {})

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        gate = gate_module.CutoverGate(
            client,
            prefix=PREFIX,
            inspect_db=lambda: counts(activeOwnedResources=len(remaining)),
            inspect_native=lambda: [],
        )
        gate.cleanup_intents = {
            (doc["kind"], doc["metadata"]["name"]) for doc in (expert, job)
        }
        gate.cleanup()
    assert deleted == ["Job", "Expert"]
    assert gate.evidence["cleanup"]["complete"] is True
