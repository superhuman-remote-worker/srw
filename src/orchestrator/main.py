"""Orchestrator entrypoint.

Run with:
    uvicorn orchestrator.main:app --reload --port 8085

This module only prepares the process (environment, license gate, logging)
and builds the application; composition lives in ``orchestrator.application``.
Nothing imports this module.
"""

import os

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

if os.environ.get("LICENSE_TERMS_ACCEPTED", "").strip().lower() != "true":
    raise SystemExit(
        "License terms not accepted. Set LICENSE_TERMS_ACCEPTED=true to run. "
        "See https://github.com/superhuman-remote-worker/srw/blob/main/LICENSE"
    )

# Configure application-level logging (Uvicorn only configures its own loggers).
# JSON when LOG_FORMAT=json (cluster), text otherwise (local/dev). When DEBUG,
# only app namespaces get DEBUG; third-party stays at INFO (DEBUG_ALL=1 to
# include it). See knowledge-base/knowledge/features/centralized_logging.md.
from orchestrator.logging_config import configure_logging  # noqa: E402

configure_logging(
    component="orchestrator",
    app_namespaces=("orchestrator", "shared"),
    disable_uvicorn_access=True,
)

from orchestrator.application import create_app  # noqa: E402

app = create_app()
