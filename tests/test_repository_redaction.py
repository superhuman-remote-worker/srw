"""Unit tests for ``security.access`` project-repository redaction.

Pure functions, no DB. Mirrors ``TestRedactDatasource`` in
``tests/test_datasource_access.py`` and ``test_config_override_redaction.py``.

Guards two fixes visible on the project Repos tab:

* **A (credential leak):** ``repo_url`` embeds the shared Gitea admin
  ``user:password@`` — it must never leave the orchestrator over REST.
* **C (unusable link):** the stored host is the cluster-internal
  ``srw-gitea:3000``; it must be rewritten to the ingress-routable ``GITEA_URL``.

The credential strip is a hard security guarantee and must hold even when the
host-rewrite is a no-op (env unset, or an external source repo).
"""

import pytest

from orchestrator.security import access

INTERNAL = "http://srw-gitea:3000"
EXTERNAL = "https://git.h4ll.net"
# A realistic stored row: managed jobs repo with the admin creds baked in.
CREDENTIALED = "http://srw:s3cr3t-P4ss@srw-gitea:3000/srw/project-abc-jobs.git"


@pytest.fixture
def gitea_env(monkeypatch):
    monkeypatch.setenv("GITEA_INTERNAL_URL", INTERNAL)
    monkeypatch.setenv("GITEA_URL", EXTERNAL)


# --- externalize_gitea_url ---------------------------------------------------


def test_externalize_swaps_host_scheme_and_removes_credentials(gitea_env):
    out = access.externalize_gitea_url(CREDENTIALED)
    assert out == "https://git.h4ll.net/srw/project-abc-jobs.git"


def test_externalize_preserves_external_port(monkeypatch):
    monkeypatch.setenv("GITEA_INTERNAL_URL", INTERNAL)
    monkeypatch.setenv("GITEA_URL", "https://git.h4ll.net:8443")
    out = access.externalize_gitea_url(CREDENTIALED)
    assert out == "https://git.h4ll.net:8443/srw/project-abc-jobs.git"


@pytest.mark.parametrize("internal, url, expected", [
    (INTERNAL, CREDENTIALED, "https://192.0.2.10:30443/git/srw/project-abc-jobs.git"),
    (INTERNAL + "/gitea", INTERNAL + "/gitea/team/repo.git?ref=main#readme", "https://192.0.2.10:30443/git/team/repo.git?ref=main#readme"),
    (INTERNAL + "/gitea", INTERNAL + "/gitea-other/team/repo.git", INTERNAL + "/gitea-other/team/repo.git"),
    (INTERNAL, "http://srw-gitea:3001/team/repo.git", "http://srw-gitea:3001/team/repo.git"),
    (INTERNAL, "https://srw-gitea:3000/team/repo.git", "https://srw-gitea:3000/team/repo.git"),
])
def test_externalize_matches_exact_internal_base_and_preserves_public_prefix(monkeypatch, internal, url, expected):
    monkeypatch.setenv("GITEA_INTERNAL_URL", internal)
    monkeypatch.setenv("GITEA_URL", "https://192.0.2.10:30443/git")
    assert access.externalize_gitea_url(url) == expected


def test_externalize_leaves_foreign_host_untouched(gitea_env):
    gh = "https://x-token:ghp_SECRET@github.com/acme/repo.git"
    assert access.externalize_gitea_url(gh) == gh


def test_externalize_noop_when_env_unset(monkeypatch):
    monkeypatch.delenv("GITEA_INTERNAL_URL", raising=False)
    monkeypatch.delenv("GITEA_URL", raising=False)
    assert access.externalize_gitea_url(CREDENTIALED) == CREDENTIALED


def test_externalize_noop_when_internal_equals_external(monkeypatch):
    monkeypatch.setenv("GITEA_INTERNAL_URL", EXTERNAL)
    monkeypatch.setenv("GITEA_URL", EXTERNAL)
    url = "https://srw:p@git.h4ll.net/srw/x.git"
    assert access.externalize_gitea_url(url) == url


def test_externalize_handles_none_and_empty(gitea_env):
    assert access.externalize_gitea_url(None) is None
    assert access.externalize_gitea_url("") == ""


# --- redact_repository -------------------------------------------------------


def _row(**over):
    row = {
        "id": "11111111-1111-1111-1111-111111111111",
        "name": "project-abc-jobs",
        "role": "jobs",
        "repo_url": CREDENTIALED,
        "read_only": False,
        "is_managed": True,
        "branch": "main",
    }
    row.update(over)
    return row


def test_redact_strips_creds_and_externalizes(gitea_env):
    out = access.redact_repository(_row())
    assert out["repo_url"] == "https://git.h4ll.net/srw/project-abc-jobs.git"
    assert "@" not in out["repo_url"]
    assert "s3cr3t" not in out["repo_url"]


def test_redact_preserves_non_secret_fields(gitea_env):
    out = access.redact_repository(_row())
    assert out["name"] == "project-abc-jobs"
    assert out["role"] == "jobs"
    assert out["read_only"] is False
    assert out["is_managed"] is True
    assert out["branch"] == "main"


def test_redact_does_not_mutate_input(gitea_env):
    row = _row()
    access.redact_repository(row)
    assert row["repo_url"] == CREDENTIALED  # original untouched


def test_redact_drops_credentials_blob(gitea_env):
    out = access.redact_repository(_row(credentials={"password": "x"}))
    assert "credentials" not in out


def test_redact_strips_creds_even_without_env(monkeypatch):
    # Security guarantee must not depend on the rewrite env being set.
    monkeypatch.delenv("GITEA_INTERNAL_URL", raising=False)
    monkeypatch.delenv("GITEA_URL", raising=False)
    out = access.redact_repository(_row())
    assert "s3cr3t" not in out["repo_url"]
    assert "@" not in out["repo_url"]


def test_redact_strips_token_from_foreign_repo(gitea_env):
    gh = "https://x-token:ghp_SECRET@github.com/acme/repo.git"
    out = access.redact_repository(_row(role="source", repo_url=gh, is_managed=False))
    assert out["repo_url"] == "https://github.com/acme/repo.git"
    assert "ghp_SECRET" not in out["repo_url"]


def test_redact_handles_missing_url(gitea_env):
    out = access.redact_repository(_row(repo_url=None))
    assert out["repo_url"] is None


def test_redact_repositories_list_variant(gitea_env):
    rows = [_row(), _row(name="p2", repo_url=CREDENTIALED)]
    out = access.redact_repositories(rows)
    assert len(out) == 2
    assert all("@" not in r["repo_url"] for r in out)
