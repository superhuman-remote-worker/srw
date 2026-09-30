# Development guide

SRW deploys to Kubernetes in every environment. Local Python and frontend
processes are useful for a fast edit/debug cycle, but they connect to services
in a running cluster; they do not replace the cluster.

For contribution scope and pull-request expectations, read
[CONTRIBUTING.md](../CONTRIBUTING.md).

## Repository map

| Path | Responsibility |
|---|---|
| `src/orchestrator/` | FastAPI API, authentication, dispatch, provisioning, persistence, and migrations |
| `src/agent/` | LangGraph worker and persistent-session runtime, tools, and graph execution |
| `src/mcp_server/` | MCP API client and service |
| `src/vm_controller/` | Workspace VM lifecycle controller |
| `src/shared/` | Framework-free contracts and helpers; `runtime/` holds common configuration, LLM, memory, and transport support for agent and orchestrator |
| `cockpit/` | Angular web application |
| `config/` | Expert, role, prompt, tool, and skill configuration |
| `helm/` | Kubernetes deployment artifact and values schema |
| `deployment/`, `scripts/` | Local overlays and operational helpers |
| `tests/` | Python unit, integration, chart-contract, and end-to-end tests |

Nested projects in the checkout are separate scopes unless a task explicitly
targets them.

## Start the local cluster

Complete [Local Kubernetes with k3d](local-kubernetes.md) first. Keep the
cluster running while using host-side Python tools or the Cockpit development
server.

## Python environment

CI uses Python 3.12.

```bash
python -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install -r src/orchestrator/requirements.txt
python -m pip install -r requirements-dev.txt
python -m pip install --no-deps -e .
playwright install chromium
```

Dependency sets remain separate. For MCP development, install
`src/mcp_server/requirements.txt`; for the VM controller, install
`src/vm_controller/requirements.txt`. The editable install exposes whichever
application packages are present in the checkout or image. `src/` is a source
root, not an import package; imports use `agent`, `orchestrator`, `mcp_server`,
`vm_controller`, or `shared` without adding `PYTHONPATH` entries.

For an environment matching the deployed orchestrator or controller, use the
[role dependency locks](../requirements/README.md), including their Python and
platform targets. The combined Python 3.12 developer/test environment above uses
the declarations and shared constraints; it does not combine the role locks.

PDF processing also needs Poppler:

```bash
# Fedora
sudo dnf install poppler-utils

# Debian or Ubuntu
sudo apt-get install poppler-utils
```

Copy the host-side environment template and add only the credentials needed for
your work:

```bash
cp .env.example .env
$EDITOR .env
```

Do not commit `.env`, copied values files, kube Secrets, tokens, or private
deployment overlays.

## Reach cluster databases

Application database Services are ClusterIP-only. Forward them to the host:

```bash
KUBE_CONTEXT=k3d-srw KUBE_NAMESPACE=srw scripts/port-forward-dbs.sh
```

The default local ports are application PostgreSQL on 5432, pgvector on 5433,
and audit PostgreSQL on 5434. Keep the forwarding process open while running
host-side initialization, orchestrator, or agent processes.

When a local environment needs a database credential, read it from the local
cluster Secret instead of copying a value from documentation:

```bash
kubectl --context k3d-srw --namespace srw get secret srw \
  --output jsonpath='{.data.POSTGRES_PASSWORD}' | base64 --decode
```

## Initialization helpers

```bash
python init.py
python init.py --only-orchestrator
python init.py --only-agent
python init.py --create-backup before_experiment
python init.py --restore-backup backups/<backup-directory>
```

`python init.py --force-reset` deletes initialized application and workspace
data. Use it only against a confirmed disposable development environment.

## Run services on the host

Start the orchestrator:

```bash
uvicorn orchestrator.main:app --reload --port 8085
```

The agent is an HTTP service driven by the orchestrator; job descriptions and
feedback are not command-line inputs.

```bash
# Jobs and persistent sessions (default)
python -m agent --port 8001 --loop

# Jobs only
python -m agent --mode worker --port 8001 --loop

# Interactive sessions only
python -m agent --mode persistent --port 8002 --loop

# Resolve a different expert configuration
python -m agent --config scholar --port 8001 --loop

# Verbose logs
LOG_LEVEL=DEBUG python -m agent --port 8001 --loop
```

`--loop` keeps the manually started process alive after one job. In Kubernetes,
pod lifecycle normally provides that behavior.

Even when the harness runs on the host, a shell-capable workspace must still be
remote. The orchestrator injects its job-scoped workspace connection at
dispatch; production code has no local-filesystem fallback.

## Cockpit

CI uses Node.js 22 and npm.

```bash
cd cockpit
npm ci
npm start
```

The Angular development server prints its local URL. Backend and authentication
behavior still depend on the configured cluster or host-side orchestrator.

Useful checks:

```bash
npm test
npm run i18n:check
npm run build
```

User-facing copy must be added to both
`cockpit/src/assets/i18n/en.json` and
`cockpit/src/assets/i18n/de-DE.json`.

## Fast inner loop with Tilt

Tilt live-syncs source into the local Kubernetes workloads and rebuilds images
when a live update is not safe.

Install the pinned development version without root privileges:

```bash
mkdir -p "$HOME/.local/bin"
TILT_VERSION=0.37.3
curl -fsSL \
  "https://github.com/tilt-dev/tilt/releases/download/v${TILT_VERSION}/tilt.${TILT_VERSION}.linux.x86_64.tar.gz" \
  | tar -xz -C "$HOME/.local/bin" tilt
chmod +x "$HOME/.local/bin/tilt"
tilt version
```

Create `deployment/values-local.yaml` first, then run the idempotent wrapper:

```bash
./scripts/local-dev-tilt-up.sh
```

For later sessions, start the retained cluster before Tilt:

```bash
k3d cluster start srw
tilt up
```

The Tilt UI is served at <https://localhost:10350>. Ctrl-C stops its watcher
and leaves the deployed release and cluster running. Use `tilt ci` for a
single apply without a persistent watcher.

### Edit-to-effect behavior

| Component | Development loop | Expected signal |
|---|---|---|
| Orchestrator | Live sync plus `uvicorn --reload` | WatchFiles reload in the pod log; health endpoint returns 200 |
| Cockpit | Live sync plus Angular/Vite HMR | Browser refresh or component update after the compile finishes |
| MCP | Live sync plus the watchfiles wrapper | FastMCP restarts and `/health` returns 200 |
| Agent | Image rebuild, Helm fan-out, and orchestrator rollout | The next job or session pod reports the new image identity |
| Dependencies or Dockerfiles | Full image rebuild | Tilt resource rebuilds and the owning workload rolls |

Existing agent pods retain the image and imported code with which they started.
End the disposable session or job and create another when validating an agent
image change.

Common commands:

```bash
tilt trigger srw-agent
tilt args -- --port 10351
tilt down
```

`tilt down` removes the Helm release managed by Tilt. Disabling the `srw`
resource also runs its Helm delete command; it does not merely pause source
sync. These actions do not delete the k3d cluster or every retained PVC.

## Develop and verify

For a substantial behavior change, open an issue or proposal that records the
problem, constraints, intended behavior, and acceptance evidence before the
implementation grows. Keep pull requests narrowly scoped.

Use the smallest relevant checks during iteration, then broaden them in
proportion to risk.

### Job HTTP contracts

The public create request is described in
[`schemas/job_create.py`](../src/orchestrator/schemas/job_create.py); the job-list
envelope and its rows are described in
[`schemas/job_list.py`](../src/orchestrator/schemas/job_list.py). List response
models are documentation only: the route retains its dictionary serializer and
existing redaction. Do not install a filtering response model as a schema cleanup.

Cockpit's `JobListPage`/`JobSummary` projections live in `core/models/audit.model.ts`;
`Job` lives in `core/models/api.model.ts`. The shared synthetic
[`job-list-page.json`](../cockpit/src/app/core/models/fixtures/job-list-page.json)
is checked against the mounted ASGI route and the real Cockpit HTTP service.
Schema tests guard field names and nullable values. Current server responses
always include `as_of` and `filters`; Cockpit retains optional metadata for
compatibility and its synthetic empty result on errors.

List counts and offsets refer to display roots, with matching child rows riding
along. `total=null` means counting was skipped; `total_is_capped=true` marks a
numeric lower bound. Carry `as_of` between pages to exclude newer inserts; it
does not freeze status changes or deletions. Real PostgreSQL pagination coverage
lives in `tests/test_job_subjob_roster_real_postgres.py`. Keep the existing
agent/MCP bare-array compatibility adapter when extending this contract.

### Python

Run from the repository root with the project virtual environment active and
`requirements-dev.txt` installed:

```bash
python -m pytest tests/test_<area>.py -x -q --tb=short
ruff check src/ tests/
ruff format --check src/ tests/

# Full parallel suite used by CI; stops on first failure
./scripts/pytest-fast.sh

# Full results for acceptance and timing, even when some tests fail
./scripts/pytest-fast.sh tests/ -q --tb=short --maxfail=0
```

The runner defaults to the smaller of the detected CPU count and eight workers.
It uses `--dist loadfile`, keeping all tests from a file on the same worker so
module-scoped fixtures retain their normal lifetime. Both `main` and `develop`
Python CI jobs use this runner; `develop` can select affected test files.

The runner uses the active `python`. Select an interpreter explicitly with
`SRW_PYTHON`, and override the worker count with `SRW_PYTEST_WORKERS` when needed:

```bash
SRW_PYTHON=.venv/bin/python ./scripts/pytest-fast.sh
SRW_PYTEST_WORKERS=4 ./scripts/pytest-fast.sh
```

Use the path of your actual virtual environment (`venv/bin/python` if following
the setup above). Keep worker counts bounded; avoid `-n auto`, since each worker
imports the substantial agent and orchestrator stacks.

Fast Postgres settings are automatic. Before collection in every pytest process,
including each xdist worker, `tests/conftest.py` gives disposable
`PostgresContainer` instances `fsync=off`, `synchronous_commit=off`, and
`full_page_writes=off`. No extra flag or plugin is needed, and explicit container
commands are preserved. Direct `python -m pytest` receives the same database
defaults but runs serially unless parallelism is requested. Database integration
tests still need a working Docker-compatible container runtime; compare skipped
tests as well as failures when measuring performance.

Async tests require `@pytest.mark.asyncio`; mock awaitable collaborators with
`AsyncMock`.

### Cockpit

```bash
cd cockpit
npm test
npm run i18n:check
npm run build
```

For user-visible changes, exercise the running Cockpit as well as the unit
test. Angular signal mocks must remain callable and expose `.set()` or
`.update()` when production code uses those methods.

### Helm

```bash
helm lint helm/ -f helm/ci/test-values.yaml
helm lint helm/ -f helm/ci/customer-external-values.yaml
```

Chart values, templates, and Fleet/deployment overlays are the source of truth;
do not treat a manual cluster patch as the completed fix.

### Cross-component smoke test

For changes crossing orchestrator, agent, and workspace boundaries, repeat the
[local smoke test](local-kubernetes.md#smoke-test): sign in, create a session,
send a message, create a job, and inspect the resulting pod and job states.

## Debugging state

The exact storage location depends on the configured backend:

- shared worker checkpoints normally live in PostgreSQL;
- legacy/local SQLite checkpoints use `workspace/checkpoints/` on the agent
  runtime filesystem;
- local runtime logs use `workspace/logs/`;
- phase snapshots use `workspace/phase_snapshots/` when enabled;
- agent-authored inputs, notes, plans, and outputs live in the selected remote
  workspace; and
- persistent conversation records are authoritative in application storage,
  with workspace message artifacts available only in configurations that write
  them.

Use the Cockpit and orchestrator APIs for resume, recovery, freeze approval,
feedback, and cancellation. The orchestrator, not a local file operation, owns
the final lifecycle state.
