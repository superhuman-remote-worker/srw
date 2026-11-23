"""A job can't name a file on the agent's own filesystem.

``document_path`` / ``document_dir`` used to be copied from the agent pod into
the job's workspace, so a caller could read the agent's environment and tokens
(``/proc/self/environ``). Job creation refuses the fields, admission strips
them from ``context``, and the agent no longer reads either.
"""

import ast
import inspect
import textwrap

import pytest
from pydantic import ValidationError

from orchestrator.schemas.job_create import JobCreate, public_job_create_schema


@pytest.mark.parametrize("field", ["document_path", "document_dir"])
def test_job_creation_refuses_a_path_on_the_agent(field):
    with pytest.raises(ValidationError) as exc:
        JobCreate(description="x", **{field: "/proc/self/environ"})
    assert "upload_id" in str(exc.value)


def test_job_context_cannot_carry_a_path_on_the_agent():
    job = JobCreate(
        description="x",
        context={
            "document_path": "/proc/self/environ",
            "document_dir": "/var/run/secrets",
            "kept": 1,
        },
    )
    assert job.context == {"kept": 1}


def test_public_schema_no_longer_offers_the_fields():
    properties = public_job_create_schema()["properties"]
    assert "document_path" not in properties
    assert "document_dir" not in properties


def test_agent_reads_no_file_a_job_names():
    """No call in workspace setup takes ``document_path`` / ``document_dir``.

    The function is the one place a start request's metadata (``context``
    included) turns into workspace files; every start path funnels into it.
    """
    from agent.agent import UniversalAgent

    tree = ast.parse(
        textwrap.dedent(inspect.getsource(UniversalAgent._setup_job_workspace))
    )
    named = {"document_path", "document_dir"}

    def names_a_document_key(node):
        return any(
            isinstance(inner, ast.Constant) and inner.value in named
            for inner in ast.walk(node)
        )

    offenders = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "Path":
            if any(names_a_document_key(arg) for arg in node.args):
                offenders.append(ast.unparse(node))
    assert offenders == []
