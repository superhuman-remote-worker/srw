"""Publication uses the normal CI environment and immutable migration policy."""

import hashlib
from pathlib import Path
import tomllib

import yaml


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (
    "src/orchestrator/database/migrations/app/0323_nonquota_retained_vm_resume.sql"
)


def test_canvas_ci_installs_its_conftest_dependency():
    workflow = yaml.safe_load(
        (ROOT / ".github/workflows/db-migrations.yml").read_text()
    )
    commands = [s.get("run", "") for s in workflow["jobs"]["dry-run"]["steps"]]
    install = next(c for c in commands if "pip install --quiet asyncpg" in c)
    assert "testcontainers[postgres]" in install


def test_frozen_nonquota_migration_has_exact_documented_lint_exception():
    config = tomllib.loads((ROOT / ".squawk.toml").read_text())
    assert MIGRATION in config["excluded_paths"]
    sql = (ROOT / MIGRATION).read_bytes()
    assert b"SET LOCAL lock_timeout = '2s'" in sql
    assert b"vm_thread_cleanup_optional_reservation_pair" in sql
    assert (
        hashlib.sha256(sql).hexdigest()
        == "2859c2488a807331e1d9ccf7df73ce56695d22b9cbcf52e1160c2f48c6454c4b"
    )
    assert "ban-drop-not-null" not in config["excluded_rules"]
    assert "constraint-missing-not-valid" not in config["excluded_rules"]
