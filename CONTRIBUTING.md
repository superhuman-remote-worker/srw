# Contributing to Superhuman Remote Worker

Thank you for helping improve SRW. Focused bug fixes, tests, documentation,
accessibility work, and well-scoped features are welcome.

## Before starting

- Search the [issue tracker](https://github.com/superhuman-remote-worker/srw/issues)
  for existing work.
- Open an issue before a large feature, schema change, new dependency, or
  architectural refactor. Explain the problem and acceptance criteria before
  proposing a large solution.
- Report vulnerabilities privately through the process in
  [SECURITY.md](SECURITY.md), never in a public issue.
- Keep nested or external projects outside the change unless the issue
  explicitly includes them.

Maintainers may have additional private planning material, but contributing to
the public repository must not require access to it. Put the context needed to
review a public change in its issue, pull request, committed public
documentation, tests, or code comments.

## Set up the repository

SRW runs on Kubernetes. Follow the
[local k3d guide](docs/local-kubernetes.md) for the full application, then use
the [development guide](docs/development.md) for host-side Python, Angular,
Tilt, and test workflows.

At minimum, install the development dependencies:

```bash
python -m venv venv
source venv/bin/activate
python -m pip install -r requirements.txt
python -m pip install -r requirements-dev.txt
playwright install chromium

cd cockpit
npm ci
```

CI uses Python 3.12, Node.js 22, npm, and the versions pinned by the repository.

## Make a change

- Keep the edit narrowly tied to the issue or stated goal.
- Follow the existing architecture and reuse shared services and UI components
  before adding parallel implementations.
- Add or update tests for changed behavior.
- Never commit `.env`, copied local values, cluster Secrets, tokens, customer
  data, or private deployment details.
- Put database changes in a new migration under
  `src/orchestrator/database/migrations/app/` or `vector/`; do not edit schema
  snapshots as the implementation.
- Keep final job-state authority in the orchestrator. Agent code reports
  completion data and control flags but does not persist its own terminal state.
- Keep shell-capable workspaces separate from the agent harness. Test-only
  filesystem backends must remain under `tests/`.
- Update both English and German locale files when adding user-visible Cockpit
  copy.

## Verify

Run checks proportional to the change. Common commands are:

```bash
# Python
pytest tests/test_<area>.py -x -q --tb=short
ruff check src/ tests/
ruff format --check src/ tests/

# Cockpit
cd cockpit
npm test
npm run i18n:check
npm run build

# Helm
helm lint helm/ -f helm/ci/test-values.yaml
helm lint helm/ -f helm/ci/customer-external-values.yaml
```

Use `./scripts/pytest-fast.sh` for the bounded full Python suite. A UI or
cross-component change should also be exercised in the running local
application.

## Pull requests

A useful pull request:

- explains the user-visible or operational problem;
- describes the chosen behavior and important tradeoffs;
- links the issue or design discussion when one exists;
- lists the exact verification performed;
- calls out migrations, configuration changes, compatibility limits, and
  follow-up work; and
- avoids unrelated formatting or refactoring.

Reviewers should be able to understand and verify the change using only public
repository context.

## Documentation

The root README is the product landing page. Stable user, operator, security,
architecture, and development material belongs under [`docs/`](docs/README.md)
or the relevant component README. Do not link public instructions to a private
working copy or deployment-specific runbook.

Internal design drafts, implementation plans, investigations, validation records,
and development handoffs belong in the separate knowledge-base repository.
Follow its existing taxonomy and keep note filename stems unique across the
vault. This project convention overrides agent workflow defaults such as
`docs/superpowers/`; do not recreate that directory in the application repository.
Public changes must still include enough context for review in the issue, pull
request, stable public documentation, tests, or code comments.

If code and documentation disagree, update both in the same pull request when
the correct behavior is known.
