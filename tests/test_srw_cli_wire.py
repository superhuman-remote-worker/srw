"""The built ``srw`` binary against the real manifest, identity and version routes.

The Go client is written against an API without typed responses, so nothing
but a real request shows when the two drift apart. These cases run the binary
as a subprocess against real routers, the real token resolver, the real scope
table and a real PostgreSQL with the current schema
(``tests/_srw_cli_harness.py``). They assert on what a person or a script
sees: exit codes, tables and messages, not only ``-o json``.
"""

from __future__ import annotations

import json

import pytest
from testcontainers.postgres import PostgresContainer

from tests._srw_cli_harness import Installation, Srw, build_binary, postgres_url

READ = ("jobs:read",)
WRITE = ("jobs:read", "resources:write")


def expert(name: str, **config) -> dict:
    return {
        "apiVersion": "srw/v1alpha1",
        "kind": "Expert",
        "metadata": {"name": name},
        "spec": {"runtime": {"image": "example/custom:1", "config": config}},
    }


JOB = {
    "apiVersion": "srw/v1alpha1",
    "kind": "Job",
    "metadata": {"name": "assignment"},
    "spec": {
        "task": {"text": "Do the work"},
        "execution": {"expert": {"ref": {"name": "reviewer"}}},
    },
}
PROJECT = {
    "apiVersion": "srw/v1alpha1",
    "kind": "Project",
    "metadata": {"name": "team"},
    "spec": {"resources": {}},
}


@pytest.fixture(scope="module")
def installation():
    with PostgresContainer("pgvector/pgvector:pg15") as container:
        running = Installation(postgres_url(container)).start()
        yield running
        running.stop()


@pytest.fixture(scope="module")
def binary(tmp_path_factory):
    return build_binary(tmp_path_factory.mktemp("srw-bin"))


@pytest.fixture
def srw(binary, installation, tmp_path):
    return Srw(binary, installation, tmp_path)


@pytest.fixture
def user(installation):
    return installation.user()


def test_no_cli_source_file_is_ignored_by_git():
    """The binary below is built from the working directory, so a source file
    that ``.gitignore`` hides still compiles here and is missing from every
    clean checkout. It happened: a package named ``target`` was ignored by the
    repository-wide ``target/`` rule."""
    import subprocess

    from tests._srw_cli_harness import REPO_ROOT

    listed = subprocess.run(
        ["git", "ls-files", "--others", "--ignored", "--exclude-standard", "--", "cli"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if listed.returncode != 0:
        pytest.skip("not a git checkout")
    hidden = [
        path
        for path in listed.stdout.splitlines()
        if path.endswith((".go", "go.mod", "go.sum"))
    ]
    assert not hidden, (
        "git ignores these Go files; rename the directory or the file: "
        + ", ".join(hidden)
    )


def write(tmp_path, name: str, document) -> str:
    path = tmp_path / name
    text = document if isinstance(document, str) else json.dumps(document)
    path.write_text(text)
    return str(path)


def test_whoami_and_version_read_the_real_routes(srw, installation, user):
    token = installation.token(user, *WRITE, name="pipeline")
    who = srw(token, "whoami")
    assert who.code == 0, who.err
    assert "Wire test user" in who.out
    assert "Token name:  pipeline" in who.out
    assert "Scopes:      jobs:read, resources:write" in who.out
    assert "Admin:       no (this token has no admin scope)" in who.out
    assert "Context:     none" in who.out
    version = srw(token, "version")
    assert version.code == 0, version.err
    assert "manifest API srw/v1alpha1" in version.out


def test_a_token_that_is_not_known_exits_3(srw):
    result = srw("ak_" + "0" * 40, "whoami")
    assert result.code == 3
    assert "the token is unknown, revoked or expired" in result.err


def test_validate_names_the_file_of_an_error(srw, installation, user, tmp_path):
    token = installation.token(user, *READ)
    good = write(tmp_path, "a.json", expert("alpha"))
    bad = write(tmp_path, "b.yaml", "kind: Expert\nkind: Twice\n")
    ok = srw(token, "validate", "-f", good)
    assert ok.code == 0, ok.err
    assert json.loads(ok.out)["documents"] == [{"kind": "Expert", "name": "alpha"}]

    both = srw(token, "validate", "-f", good, "-f", bad)
    assert both.code == 1
    assert json.loads(both.err) == {
        "error": {
            "code": "DuplicateKey",
            "file": bad,
            "document": 1,
            "message": "Duplicate object key.",
            "path": "/",
            "status": 422,
        }
    }
    # One file: the server names no source, the client knows the file.
    alone = srw(token, "validate", "-f", bad)
    assert json.loads(alone.err)["error"]["file"] == bad

    # Mixed YAML and JSON, and a directory, are one bundle.
    yaml_file = write(
        tmp_path,
        "c.yaml",
        "apiVersion: srw/v1alpha1\nkind: Expert\nmetadata:\n  name: gamma\n"
        "spec:\n  runtime:\n    image: example/custom:1\n",
    )
    mixed = srw(token, "validate", "-f", good, "-f", yaml_file, "-o", "name")
    assert mixed.code == 0, mixed.err
    assert mixed.out == "expert/alpha\nexpert/gamma\n"


def test_apply_get_export_and_delete_round_trip(srw, installation, user, tmp_path):
    reader = installation.token(user, *READ)
    writer = installation.token(user, *WRITE)
    path = write(tmp_path, "reviewer.json", expert("reviewer", customTool=None))

    # A reader may preview and may not apply.
    preview = srw(reader, "preview", "-f", path, "--account")
    assert preview.code == 0, preview.err
    assert json.loads(preview.out)["planRevision"].startswith("sha256:")
    refused = srw(reader, "apply", "-f", path)
    assert refused.code == 3
    error = json.loads(refused.err)["error"]
    assert error["message"] == "Insufficient token scope: requires resources:write"
    assert "also has `resources:write`" in error["hint"]

    applied = srw(writer, "apply", "-f", path)
    assert applied.code == 0, applied.err
    resource = json.loads(applied.out)["resources"][0]
    uid = resource["uid"]
    assert (resource["changed"], resource["resourceVersion"]) == (True, 1)
    assert "Expert/reviewer changed (version 1)" in applied.err

    # Applying an edited file again needs no version from the user: the
    # client takes it from its own preview.
    edited = write(tmp_path, "reviewer.json", expert("reviewer", customTool=1))
    again = srw(writer, "apply", "-f", edited)
    assert again.code == 0, again.err
    assert "Expert/reviewer changed (version 2)" in again.err
    same = srw(writer, "apply", "-f", edited, "-o", "name")
    assert same.code == 0 and same.out == f"expert/{uid}\n"
    assert "Expert/reviewer unchanged (version 2)" in same.err

    one = srw(reader, "get", uid)
    assert one.code == 0, one.err
    view = json.loads(one.out)
    assert view["resourceVersion"] == 2 and view["redacted"] is False

    listed = srw(reader, "get", "--kind", "Expert")
    assert listed.code == 0, listed.err
    answer = json.loads(listed.out)
    assert [r["uid"] for r in answer["resources"]] == [uid]
    assert answer["truncated"] is False
    assert srw(reader, "get", "--kind", "Nope").code == 2

    exported = srw(reader, "export", uid)
    assert exported.code == 0, exported.err
    assert "kind: Expert" in exported.out and "name: reviewer" in exported.out
    # What was exported can be applied again and changes nothing.
    round_trip = srw(writer, "apply", "-f", "-", stdin=exported.out)
    assert round_trip.code == 0, round_trip.err
    assert "Expert/reviewer unchanged (version 2)" in round_trip.err

    stale = srw(writer, "delete", uid, "--expected-version", "1")
    assert stale.code == 5, stale.err
    assert srw(reader, "delete", uid, "--expected-version", "2").code == 3
    # Without a terminal a delete needs --yes or the version.
    assert srw(writer, "delete", uid).code == 2
    deleted = srw(writer, "delete", uid, "--yes")
    assert deleted.code == 0, deleted.err
    assert json.loads(deleted.out) == {"deleted": True, "uid": uid}
    assert srw(reader, "get", uid).code == 4


def test_an_apply_that_lost_a_race_exits_5(srw, installation, user, tmp_path):
    writer = installation.token(user, *WRITE)
    path = write(tmp_path, "racer.json", expert("racer"))
    first = srw(writer, "apply", "-f", path)
    assert first.code == 0, first.err
    plan = write(tmp_path, "plan.json", srw(writer, "preview", "-f", path).out)
    # Someone else changes the resource after the plan was reviewed.
    assert (
        srw(
            writer, "apply", "-f", write(tmp_path, "racer2.json", expert("racer", x=1))
        ).code
        == 0
    )
    late = srw(writer, "apply", "-f", path, "--plan", plan)
    assert late.code == 5, late.err


def test_a_job_and_a_project_need_their_own_scope(srw, installation, user, tmp_path):
    writer = installation.token(user, *WRITE)
    job = write(tmp_path, "job.json", [expert("reviewer"), JOB])
    refused = srw(writer, "apply", "-f", job)
    assert refused.code == 3
    assert (
        json.loads(refused.err)["error"]["message"]
        == "Insufficient token scope: requires jobs:write"
    )
    project = write(tmp_path, "project.json", PROJECT)
    refused = srw(writer, "apply", "-f", project)
    assert refused.code == 3
    assert (
        json.loads(refused.err)["error"]["message"]
        == "Insufficient token scope: requires projects:write"
    )
    # Each refusal left a security event, and nothing was stored.
    events = installation.run(
        installation.db.fetch(
            "SELECT detail FROM security_events WHERE user_id=$1::uuid "
            "AND event_type='token_scope_denied' ORDER BY created_at",
            user,
        )
    )
    assert [e["detail"].split(" (")[0] for e in events] == [
        "Insufficient token scope: requires jobs:write",
        "Insufficient token scope: requires projects:write",
    ]
    assert json.loads(srw(writer, "get").out)["resources"] == []
    # With the scope the check passes. This harness has no job admission, so
    # the apply then stops at the server's own 503, not at a 403.
    with_jobs = installation.token(user, *WRITE, "jobs:write")
    admitted = srw(with_jobs, "apply", "-f", job)
    assert admitted.code == 1
    assert "admission is unavailable" in admitted.err


def test_the_shared_catalog_stays_with_admins(srw, installation, user, tmp_path):
    writer = installation.token(user, *WRITE)
    path = write(tmp_path, "shared.json", expert("shared-one"))
    refused = srw(writer, "apply", "-f", path, "--catalog")
    assert refused.code == 3, refused.err
    admin = installation.user(admin=True, name="Wire admin")
    # An admin's token without the admin scope arrives without the admin flag.
    scoped = installation.token(admin, *WRITE)
    assert srw(scoped, "apply", "-f", path, "--catalog").code == 3
    full = installation.token(admin, "admin")
    applied = srw(full, "apply", "-f", path, "--catalog")
    assert applied.code == 0, applied.err


def test_api_reaches_any_route_and_exits_by_status(srw, installation, user):
    token = installation.token(user, *READ)
    schema = srw(token, "api", "/api/manifests/schema")
    assert schema.code == 0 and '"srw/v1alpha1"' in schema.out
    missing = srw(token, "api", "/api/resources/00000000-0000-0000-0000-000000000009")
    assert missing.code == 4
    assert "Resource does not exist." in missing.out
