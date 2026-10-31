from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _active_requirements() -> list[str]:
    return [
        line.split("#", 1)[0].strip()
        for line in (ROOT / "requirements.txt").read_text().splitlines()
        if line.split("#", 1)[0].strip()
    ]


def test_shared_postgres_checkpointer_dependencies_are_exactly_pinned() -> None:
    requirements = _active_requirements()

    assert [
        requirement
        for requirement in requirements
        if requirement.startswith("langgraph==")
    ] == ["langgraph==1.2.10"]
    assert [
        requirement
        for requirement in requirements
        if requirement.startswith("langgraph-checkpoint==")
    ] == ["langgraph-checkpoint==4.3.0"]
    assert [
        requirement
        for requirement in requirements
        if requirement.startswith("langgraph-checkpoint-postgres")
    ] == ["langgraph-checkpoint-postgres==3.1.3"]
    assert [
        requirement
        for requirement in requirements
        if requirement.startswith("psycopg-pool")
    ] == ["psycopg-pool==3.3.1"]
