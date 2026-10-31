# Runtime dependency locks

The orchestrator (Linux amd64 CPython 3.11) and VM controller (Linux amd64
CPython 3.12) install separate, fully pinned dependency sets from `locks/`.
Production and development orchestrator images use the same lock. Their
Dockerfiles and both CI workflows require the recorded distribution hashes,
check installed versions, run `pip check`, and exercise SDK authentication.

Author direct dependencies in `src/<role>/requirements.txt`. These declarations
include `constraints.txt`, which records shared compatibility decisions; it is
not a union of all runtime dependencies. The current Kubernetes SDK selection is
36.0.3. Upstream documents the config-loader fix in 36.0.1 and legacy auth-key
compatibility repair in 36.0.2. The installed gate tests both configuration paths,
all three API families used here, and refreshed tokens. See the
[upstream changelog](https://raw.githubusercontent.com/kubernetes-client/python/v36.0.3/CHANGELOG.md)
and [SDK check](../scripts/kubernetes-sdk-auth.md).

## Updating

Use Python 3.11+ on a Linux host with Docker. The generator runs a clean
`python:3.11-slim` or `python:3.12-slim` container, installs the exact resolver
versions in `lock-tools.txt`, and compiles against PyPI. Only the declared inputs
and existing role lock enter this container; no host environment, credentials,
installed packages or private index configuration are inherited.

```bash
# Reconcile changed declarations/constraints while retaining compatible pins.
python scripts/lock_dependencies.py compile orchestrator
python scripts/lock_dependencies.py compile vm-controller

# Deliberately refresh one package or the entire role; review the resulting diff.
python scripts/lock_dependencies.py compile orchestrator --upgrade-package httpx
python scripts/lock_dependencies.py compile vm-controller --upgrade

# Offline check of input digests and generated lock contents.
python scripts/lock_dependencies.py check
```

Changes to common constraints, resolver versions or the generator require
regenerating both locks. The manifest alongside each lock records the target
environment and input/output digests. Do not edit it or lock entries by hand.
The check refuses stale inputs and changed lock contents. Normal compilation
retains compatible versions; scheduled dependency maintenance must use explicit
`--upgrade` or `--upgrade-package` so fixes are reviewed rather than frozen out.
This follows pip-tools' [update and environment-specific resolution workflow](https://pip-tools.readthedocs.io/en/stable/).

Review direct/transitive version changes, removals and hashes. Run focused tests
for changed libraries and the combined Python suite. Build all affected image
variants, check their installed versions/imports/resources and SDK authentication,
then validate affected operations on local k3d, including job/session creation
and normal cleanup. SDK changes need actual provisioning and cluster API checks;
the offline header probe does not prove TLS, RBAC or server compatibility.

CI role checks install into clean virtual environments. For a local equivalent,
use the role's Python version and an empty environment:

```bash
python3.11 -m venv /tmp/srw-orchestrator-env
/tmp/srw-orchestrator-env/bin/python -m pip install --require-hashes \
  -r requirements/locks/orchestrator-py311.txt
/tmp/srw-orchestrator-env/bin/python -m pip check
/tmp/srw-orchestrator-env/bin/python scripts/lock_dependencies.py check orchestrator --installed
/tmp/srw-orchestrator-env/bin/python -m pip install --no-deps -e .
```

Use Python 3.12 and `locks/vm-controller-py312.txt` for the controller.

The voice gateway (`voice-gateway`, Python 3.12, `docker/Dockerfile.voice-gateway`)
is a third role with one difference: `docopt`, which `num2words` declares for
its command-line script only, ships no wheel, so the generator resolves it from
source and leaves it out of the lock (`EXCLUDED` in the script). The role
installs its lock with `--no-deps` and checks consistency with
`python scripts/lock_dependencies.py pip-check voice-gateway`, which tolerates
exactly that missing requirement. Its CI job is `test-voice-gateway`.
Do not install these two platform-specific locks together or apply the 3.11
lock to the combined 3.12 unit environment. That environment still resolves the
root, orchestrator, MCP and development declarations, including the shared
constraints, and runs `pip check`. Agent/MCP/tooling lock coverage remains future
work; CPU-only Torch, browser and checkpoint policies stay in their existing
recipes. Exact runtime pins do not make OS packages, mutable base-image tags or
the editable project's build backend reproducible. Other platforms/Python
versions require their own resolution and validation before support is promised.

All files under `requirements/` and the generator participate in both roles'
Docker/Tilt inputs and develop component image identity. Tilt must rebuild these
images for dependency changes; it cannot live-sync an installed environment.
