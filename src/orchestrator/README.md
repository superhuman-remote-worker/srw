# Orchestrator

Backend API for the Superhuman Remote Worker system. Provides:
- Monitoring data to the Cockpit (Angular frontend)
- Agent registration and orchestration
- APIs consumed by the separate MCP service

## Quick Start

```bash
# From the repository root, after creating and activating a virtual environment
pip install -r src/orchestrator/requirements.txt
pip install --no-deps -e .
uvicorn orchestrator.main:app --reload --port 8085
```

The service uses the cluster's databases. See the root
[development guide](../../README.md) for local setup. `src/` is a
source root; imports use `orchestrator`, with common support in `shared`.

## Architecture

```
src/orchestrator/
├── main.py              # FastAPI application and routes
├── graph_routes.py      # Neo4j graph visualization endpoints
├── database/
│   ├── postgres.py      # PostgreSQL service (jobs, requirements)
│   ├── audit_store.py   # Postgres audit store (audit trail, LLM logs)
│   └── migrations/      # Immutable application/vector/audit migrations
├── services/            # Dispatch, workspace, and application services
├── operator_cli/        # Operational commands, run as orchestrator.operator_cli.*
└── requirements.txt

src/mcp_server/
├── server.py            # MCP tools and server
├── job_adapter.py       # MCP adapter for shared job actions
├── __main__.py          # python -m mcp_server
└── requirements.txt

src/shared/orch_surface/
└── client.py            # Shared authenticated HTTP client for orchestrator API
```

## API Endpoints

### Monitoring (Current)

| Endpoint | Description |
|----------|-------------|
| `GET /api/tables` | List PostgreSQL tables |
| `GET /api/tables/{name}` | Get paginated table data |
| `GET /api/jobs` | List jobs with audit counts |
| `GET /api/jobs/{id}/audit` | Get audit trail entries |
| `GET /api/jobs/{id}/chat` | Get chat history |
| `GET /api/jobs/{id}/todos` | Get workspace todos |
| `GET /api/graph/changes/{id}` | Get Neo4j mutations |
| `GET /api/requests/{doc_id}` | Get full LLM request |

### Agent Orchestration

| Endpoint | Description |
|----------|-------------|
| `POST /api/agents/register` | Agent registers itself |
| `POST /api/agents/{id}/heartbeat` | Agent status update |
| `GET /api/agents` | List registered agents |
| `PUT /api/agents/{id}/assign` | Assign job to agent |

See `knowledge-base/knowledge/angular_migration_plan.md` for the full orchestration architecture.

## Environment Variables

```bash
# PostgreSQL (required)
DATABASE_URL=postgresql://user:pass@localhost:5432/srw

# Audit store (optional - Postgres audit trail; AUDIT_POSTGRES_* or AUDIT_DB_URL)
AUDIT_DB_URL=postgresql://user:pass@localhost:5432/srw_audit

# Neo4j (optional knowledge graph; unset = the KB runs on Postgres alone).
# The chart sets these only when databases.neo4j.enabled.
NEO4J_URL=bolt://localhost:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=password
```

## MCP Server

The MCP server provides Claude Code with debugging tools.

```bash
# From the repository root
pip install -r src/mcp_server/requirements.txt
pip install --no-deps -e .

# HTTP transport (default), MCP endpoint at http://localhost:8055/mcp/
python -m mcp_server

# Direct process transport
MCP_TRANSPORT=stdio python -m mcp_server
```

### Available Tools

- `list_jobs` - List jobs with status filter
- `get_job` - Get job details
- `get_audit_trail` - Get paginated audit entries
- `get_chat_history` - Get conversation turns
- `get_todos` - Get workspace todos
- `get_graph_changes` - Get Neo4j mutations
- `get_llm_request` - Get full LLM request/response
- `search_audit` - Search audit entries
