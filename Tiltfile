# -*- mode: python -*-
# vim: set ft=python:
# =============================================================================
# Tiltfile — inner-loop dev for the SRW stack on local k3d.
#
# Design doc: knowledge-base/knowledge/features/tilt_inner_loop_dev.md
#
# Prerequisite: ./scripts/local-dev-tilt-up.sh has been run once. That script:
#   - creates the k3d cluster `srw` with an embedded registry on localhost:5005
#   - installs cert-manager + the mkcert ClusterIssuer in multi-host mode, or
#     maps localhost:8443 to the chart-owned gateway in single-origin mode
#   - creates the srw namespace + srw-vm-ssh-key + srw-session-jwt Secrets
#   - drops a values-local.yaml from the example template
#
# After that, `tilt up` brings the chart online and watches your code.
#
# Scope (Slice 1): orchestrator only. Cockpit/agent/mcp follow in later slices.
# =============================================================================

# The full chart's first Helm install can exceed Tilt's 30-second custom-deploy
# default even on a healthy local k3d cluster. Let Helm finish recording the
# release instead of killing it after resources have already been applied and
# leaving a `pending-install` revision behind.
#
# This only narrows the window — Tilt still kills the apply on a superseding
# build or a Ctrl-C, which strands the release the same way. Closing it is
# scripts/tilt-helm-apply.sh's preflight; see the `srw` resource below.
# Raised 180 -> 900 on 2026-08-30. 180s covers a normal incremental upgrade but
# NOT a from-scratch install: after a `helm uninstall` or a database reset the
# next apply must create every object and wait on them, which overran 180s and
# stranded the release in `pending-install` at revision 1. This value is a
# maximum wait, not a delay -- a fast upgrade still returns in seconds -- so
# raising it costs the inner loop nothing and only widens the window before
# Tilt gives up.
update_settings(k8s_upsert_timeout_secs=900)

# -----------------------------------------------------------------------------
# Global watch exclusions — paths Tilt must never treat as a code change.
#
# `eval/` holds memory-eval harness output (eval/memory/runs/*.log, etc.). An
# active eval streams log lines several times a second; because the
# orchestrator/cockpit/mcp docker_builds use context='.', each write looked
# like an in-context source change with no matching sync() and forced a full
# image rebuild per log line. Those Dockerfiles don't COPY eval/, so every
# rebuild was a cache-identical no-op (no pod roll) — but it pinned the inner
# loop in a perpetual rebuild and masked real edits behind the churn.
# Excluding it at the watcher level stops the loop for every resource at once
# without touching any build context. (agent/workspace use only=[…] allowlists
# and were already immune.)
# -----------------------------------------------------------------------------
watch_settings(ignore=['eval/'])

# -----------------------------------------------------------------------------
# Registry — k3d created via `--registry-create srw-registry:0.0.0.0:5005`.
#   docker push targets        : localhost:5005    (host-side mapped port)
#   kubelet image references   : srw-registry:5000 (k3d internal DNS + port)
# Tilt auto-rewrites image refs in the K8s manifest from one to the other.
# -----------------------------------------------------------------------------
default_registry('localhost:5005', host_from_cluster='srw-registry:5000')

# -----------------------------------------------------------------------------
# Orchestrator — uvicorn --reload picks up sync'd Python files without a
# container restart. fall_back_on triggers a full image rebuild for dependency,
# migration, and metering-package changes that must be present in every new
# replica rather than only in Tilt's current live-sync target.
# -----------------------------------------------------------------------------
docker_build(
    'srw-orchestrator',
    context='.',
    dockerfile='docker/Dockerfile.orchestrator.dev',
    only=[
        'src/orchestrator/',
        'src/shared/',
        'config/',
        'pyproject.toml',
        '.dockerignore',
        'docker/Dockerfile.orchestrator.dev',
        'scripts/check_kubernetes_sdk_auth.py',
        'requirements/',
        'scripts/lock_dependencies.py',
    ],
    live_update=[
        fall_back_on([
            'docker/Dockerfile.orchestrator.dev',
            'scripts/check_kubernetes_sdk_auth.py',
            'requirements/',
            'scripts/lock_dependencies.py',
            'src/orchestrator/requirements.txt',
            'pyproject.toml',
            '.dockerignore',
            # Applied migrations must remain present in every new replica.
            'src/orchestrator/database/migrations/',
            # main.py and new metering modules must be baked together, even
            # when live sync currently targets only one rolling-update Pod.
            'src/orchestrator/services/infrastructure_metering/',
        ]),
        sync('src/orchestrator/', '/app/src/orchestrator/'),
        sync('src/shared/', '/app/src/shared/'),
        sync('config/', '/app/config/'),
    ],
    ignore=[
        '**/__pycache__',
        '**/*.pyc',
        '**/*.pyo',
        '**/.pytest_cache',
        '**/.mypy_cache',
        '**/.ruff_cache',
        '**/*.egg-info',
    ],
)

# -----------------------------------------------------------------------------
# Cockpit — Angular dev image runs `ng serve` so live_update sync into
# /app/src triggers Angular's incremental rebuild + HMR push. ~1–3 s loop.
#
# Build context is the cockpit/ subdirectory; the Dockerfile inside that
# context expects `cockpit/` paths because docker_build's `context` lops
# off the path prefix.
# -----------------------------------------------------------------------------
docker_build(
    'srw-cockpit',
    context='.',
    dockerfile='cockpit/Dockerfile.cockpit.dev',
    live_update=[
        fall_back_on([
            'cockpit/Dockerfile.cockpit.dev',
            'cockpit/package.json',
            'cockpit/package-lock.json',
            'cockpit/angular.json',
            'cockpit/tsconfig.json',
            'cockpit/tsconfig.app.json',
            'cockpit/ngsw-config.json',
        ]),
        sync('cockpit/src/', '/app/src/'),
        sync('cockpit/public/', '/app/public/'),
    ],
    ignore=[
        'cockpit/node_modules/',
        'cockpit/dist/',
        'cockpit/.angular/',
        'cockpit/coverage/',
        # Chart ConfigMap mounts the rendered env.js at /app/src/assets/env.js
        # (gated by cockpit.envJs.mountPath in values-tilt.yaml). Excluding
        # the source-side file from sync keeps the ConfigMap-mounted version.
        'cockpit/src/assets/env.js',
        # Monaco assets are built once into the image; don't sync over them.
        'cockpit/public/monaco/',
        # Cross-component dirs the cockpit build doesn't care about.
        'src/',
        'config/',
        'pyproject.toml',
        'init.py',
        'helm/',
        'knowledge-base/',
        'knowledge-history/',
        'deployment/',
        'scripts/',
        'tests/',
        'docker/',
        'workspace/',
        '*.md',
        '.git/',
        '.playwright-mcp/',
        '.tilt-state/',
    ],
)

# -----------------------------------------------------------------------------
# Agent — live_update is impossible (per-job pods with restartPolicy: Never,
# spawned by AgentProvisioner; see Non-goals in tilt_inner_loop_dev.md). What
# Tilt CAN do is automate the rebuild loop end-to-end:
#
#   1. file save under src/agent/, src/shared/, config/, or packaging inputs
#   2. docker_build rebuilds srw-agent:tilt-<hash>
#   3. push to localhost:5005 (k3d pulls via srw-registry:5000)
#   4. the `srw` custom deploy re-renders the srw-config ConfigMap with the
#      new PERSISTENT_AGENT_IMAGE tag
#   5. Stakater Reloader (already running in cluster; chart's orchestrator
#      Deployment carries `reloader.stakater.com/auto: "true"`) detects the
#      ConfigMap change and rolls the orchestrator
#   6. AgentProvisioner re-reads PERSISTENT_AGENT_IMAGE at __init__ time and
#      provisions the next agent pod with the new tag
#
# The `only=` list pins the file watcher to exactly the paths that go into
# the final image's source and packaging layers. Unrelated application,
# documentation and test edits do not rebuild this image. Cold rebuild is ~30-60 s;
# warm cache (only src/ changed) is ~10-15 s.
# -----------------------------------------------------------------------------
docker_build(
    'srw-agent',
    context='.',
    dockerfile='docker/Dockerfile.agent.dev',
    only=[
        'src/agent/',
        'src/shared/',
        'config/',
        'requirements.txt',
        'pyproject.toml',
        '.dockerignore',
        'docker/Dockerfile.agent.dev',
        'docker/prepare-tokenizer-cache.py',
    ],
    ignore=[
        '**/__pycache__',
        '**/*.pyc',
        '**/*.pyo',
        '**/.pytest_cache',
        '**/.mypy_cache',
        '**/.ruff_cache',
        '**/*.egg-info',
    ],
)

# -----------------------------------------------------------------------------
# MCP — FastMCP HTTP server bridging Claude Code → orchestrator REST API.
# Small, stateless, restart-tolerant. We use live_update sync into /app
# plus a `watchfiles` wrapper in the dev Dockerfile so source edits
# trigger a process restart without rebuilding the image. ~3-5 s loop.
#
# fall_back_on covers the cases where live_update can't help (requirements
# bump, Dockerfile change, or package/build-context metadata changes).
# -----------------------------------------------------------------------------
docker_build(
    'srw-mcp',
    context='.',
    dockerfile='docker/Dockerfile.mcp.dev',
    only=[
        'src/mcp_server/',
        'src/shared/',
        'pyproject.toml',
        '.dockerignore',
        'docker/Dockerfile.mcp.dev',
    ],
    live_update=[
        fall_back_on([
            'docker/Dockerfile.mcp.dev',
            'src/mcp_server/requirements.txt',
            'pyproject.toml',
            '.dockerignore',
        ]),
        # Both namespaces match the editable installation. The recursive
        # watchfiles process restarts for either application or shared edits.
        sync('src/mcp_server/', '/app/src/mcp_server/'),
        sync('src/shared/', '/app/src/shared/'),
    ],
    ignore=[
        '**/__pycache__',
        '**/*.pyc',
        '**/*.pyo',
        '**/.pytest_cache',
        '**/.mypy_cache',
        '**/.ruff_cache',
        '**/*.egg-info',
    ],
)

# -----------------------------------------------------------------------------
# Workspace — dynamically-created SSH/code-server workspaces. These pods are
# spawned by the orchestrator, so Helm must receive the Tilt-built image tag via
# image_keys just like the agent image.
# -----------------------------------------------------------------------------
docker_build(
    'srw-workspace',
    context='.',
    dockerfile='docker/Dockerfile.workspace',
    only=[
        'docker/Dockerfile.workspace',
        'docker/workspace-entrypoint.sh',
        'docker/browser-exec',
        'docker/check-browser-stream.py',
        'docker/assert-browser-stack.sh',
    ],
    ignore=[
        '.git/',
        '.playwright-mcp/',
        '.tilt-state/',
    ],
)

# -----------------------------------------------------------------------------
# Vendored chart dependencies. `helm/charts/` is gitignored (*.tgz), so a fresh
# clone has no collabora-online tarball and `helm upgrade` refuses to run at all
# — the dependency-presence check fires before `collabora.enabled` is evaluated,
# so this bites even though Collabora is off in every local values file.
#
# CI does the same repo-add + build before each helm invocation. Doing it here
# rather than in local-dev-tilt-up.sh covers a bare `tilt up` too, which is the
# documented path for every session after the first.
#
# `helm dependency list` is offline and instant, so the common case (deps
# already vendored) adds nothing to Tiltfile evaluation.
# -----------------------------------------------------------------------------
local(
    """
    if helm dependency list ./helm | grep -q 'missing'; then
        helm repo add collabora https://collaboraonline.github.io/online --force-update
        helm dependency build ./helm
    fi
    """,
    quiet=True,
)

# -----------------------------------------------------------------------------
# Helm chart — same chart as production. Values stack (last wins):
#   1. helm/values.yaml                      chart defaults
#   2. deployment/values-local.yaml          gitignored — dev secrets +
#                                            sessionRouter + opencloud
#                                            hostAliases (Traefik ClusterIP)
#   3. deployment/values-tilt.yaml           committed — imagePullPolicy
#                                            IfNotPresent + prewarm disabled +
#                                            cockpit envJs mountPath redirect
#
# The image list below auto-substitutes the Tilt-built images for all runtime
# components, including the dynamically-spawned workspace image.
#
# This is a hand-rolled k8s_custom_deploy rather than the `helm_resource`
# extension. The extension's apply helper is a plain `helm upgrade --install`,
# and Tilt kills that subprocess whenever it cancels an in-flight deploy — a
# superseding build, a Ctrl-C, or the k8s_upsert_timeout_secs deadline above.
# Helm writes its release secret as `pending-upgrade` before applying and flips
# it to `deployed` only at the end, so a killed helm wedges every later upgrade
# with "another operation (install/upgrade/rollback) is in progress" until
# someone clears the lock by hand — restarting Tilt does not help, because the
# lock lives in the cluster. scripts/tilt-helm-apply.sh runs the identical helm
# command with a preflight that clears a stale pending revision first. See
# knowledge-base/knowledge/features/tilt_inner_loop_dev.md "Risks and known gotchas".
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# VM controller (same-cluster KubeVirt tier). Only rendered by the chart when
# `vm.mode: same-cluster`; built here unconditionally so the local k3d gate
# always runs the controller from the working tree, not from GHCR
# (knowledge-base/knowledge/features/single_cluster_vm_deployment.md §6 Lane D).
# No live_update: the image is three Python files and rebuilds in seconds.
# -----------------------------------------------------------------------------
docker_build(
    'srw-vm-controller',
    context='.',
    dockerfile='docker/Dockerfile.vm-controller',
    only=[
        'src/vm_controller/',
        'src/shared/__init__.py',
        'src/shared/vm_lifecycle_auth.py',
        'src/shared/workspace_initialization.py',
        'src/shared/vm_workspace_storage.py',
        'src/shared/workspace_preparation.py',
        'src/shared/workspace_preparation_settings.py',
        'src/shared/workspace_preparation_network.py',
        'pyproject.toml',
        '.dockerignore',
        'docker/Dockerfile.vm-controller',
        'scripts/check_kubernetes_sdk_auth.py',
        'requirements/',
        'scripts/lock_dependencies.py',
    ],
    ignore=['**/__pycache__', '**/*.pyc'],
)

# (image name, chart repository key, chart tag key)
docker_build(
    'srw-vm-preparer',
    context='.',
    dockerfile='docker/Dockerfile.vm-preparer',
    only=[
        'src/vm_controller/__init__.py',
        'src/vm_controller/preparation_builder.py',
        'src/vm_controller/preparation_firewall.py',
        'src/shared/__init__.py',
        'src/shared/workspace_initialization.py',
        'src/shared/workspace_preparation.py',
        'src/shared/workspace_preparation_network.py',
        'pyproject.toml', '.dockerignore', 'docker/Dockerfile.vm-preparer',
    ],
    ignore=['**/__pycache__', '**/*.pyc'],
)

_srw_images = [
    ('srw-orchestrator', 'image.orchestrator.repository', 'image.orchestrator.tag'),
    ('srw-cockpit', 'image.cockpit.repository', 'image.cockpit.tag'),
    ('srw-agent', 'image.agent.repository', 'image.agent.tag'),
    ('srw-mcp', 'image.mcp.repository', 'image.mcp.tag'),
    ('srw-workspace', 'image.workspace.repository', 'image.workspace.tag'),
    ('srw-vm-controller', 'vmController.image.repository', 'vmController.image.tag'),
    ('srw-vm-preparer', 'vmController.preparation.image.repository', 'vmController.preparation.image.tag'),
]

# Tilt fills in TILT_IMAGE_<i> (the freshly built+pushed ref) per image_deps
# entry; these tell the apply script which chart keys each half maps to.
_srw_helm_env = {
    'CHART': './helm',
    'RELEASE_NAME': 'srw',
    'NAMESPACE': 'srw',
    'TILT_IMAGE_COUNT': '%s' % len(_srw_images),
}
for i in range(len(_srw_images)):
    _srw_helm_env['TILT_IMAGE_KEY_REPO_%s' % i] = _srw_images[i][1]
    _srw_helm_env['TILT_IMAGE_KEY_TAG_%s' % i] = _srw_images[i][2]
    # These chart images also accept a digest, which outranks the tag. Tilt
    # owns the local image selection, including a pin saved by an earlier gate.
    if _srw_images[i][0] in ['srw-mcp', 'srw-vm-preparer']:
        _srw_helm_env['TILT_IMAGE_KEY_DIGEST_%s' % i] = _srw_images[i][2][:-4] + '.digest'

_srw_exposure_mode = os.getenv('SRW_EXPOSURE_MODE') or 'multi-host'
_srw_values_args = [
    '--values=deployment/values-local.yaml',
]
if _srw_exposure_mode == 'single-origin':
    _srw_values_args.append('--values=deployment/values-local-single-origin.yaml')
_srw_values_args.append('--values=deployment/values-tilt.yaml')

k8s_custom_deploy(
    'srw',
    apply_cmd=[
        os.path.abspath('scripts/tilt-helm-apply.sh'),
        # A custom-deploy Force Update (including `tilt trigger srw`) runs
        # delete_cmd first. The chart-managed Secret is resource-policy=keep;
        # reclaim that same object on reinstall so generated encryption/Garage
        # keys do not rotate.
        '--take-ownership',
        # Tilt observes application Pods below; Helm still gates readiness of
        # the complete release, including databases and immutable collectors.
        '--wait',
        '--timeout=14m',
    ] + _srw_values_args,
    apply_env=_srw_helm_env,
    delete_cmd=['helm', 'uninstall', '--namespace', 'srw', 'srw'],
    # Values edits DO trigger a redeploy (changed 2026-08-30). This was `deps=[]`,
    # inherited from the `helm_resource` default this replaced, which meant an
    # edit to either values file was silently a no-op: Tilt reported the resource
    # green while the cluster kept the old rendering, and the only way to notice
    # was to read the deployed object's env. That cost a full debug cycle on the
    # 0185 maintenance-gate cutover, where values-local.yaml was edited, Tilt did
    # nothing, and the orchestrator kept crash-looping on the value it had.
    # The chart directory is deliberately NOT here: it holds hundreds of files and
    # rendering is driven by the values above.
    deps=[
        'deployment/values-local.yaml',
        'deployment/values-local-single-origin.yaml',
        'deployment/values-tilt.yaml',
        'scripts/tilt-helm-apply.sh',
        'scripts/tilt-image-digest.py',
    ],
    image_deps=[img[0] for img in _srw_images],
)

# Collectors and seed Jobs also use the orchestrator image, but run with an
# immutable filesystem. Image matching alone would copy application edits into
# those containers. Restrict discovery to the chart's application components;
# dependency/migration/collector changes still rebuild the shared image above.
k8s_resource(
    'srw',
    discovery_strategy='selectors-only',
    extra_pod_selectors=[
        {
            'app.kubernetes.io/instance': 'srw',
            'app.kubernetes.io/component': component,
        }
        for component in ['orchestrator', 'cockpit', 'mcp', 'agent-stateless', 'vm-controller']
    ],
)

# -----------------------------------------------------------------------------
# Object store: the bundled single-node Garage provides S3 for local dev — the
# same path self-hosters get. The chart deploys Garage + an idempotent bootstrap
# Job and auto-wires both seams (snapshots + virtual tier) to it, so there is no
# separate fixture to apply here. The old MinIO manifest
# (deployment/tilt-minio.yaml) was removed.
#
# Nothing here enables it: `garage.enabled` defaults to auto (run the bundled
# store unless `s3.endpoint` is set), and deployment/values-tilt.yaml keeps that
# input blank rather than forcing the flag on — so local dev resolves the store
# exactly the way a fresh `helm install` does. Canvas durability depends on this
# being wired (knowledge-base/knowledge/features/canvas_durable_presentation.md §11.1); if the
# orchestrator logs "Snapshot service disabled" at startup, a stale
# deployment/values-local.yaml is setting s3.endpoint.
#
# For a pure no-store smoke test set virtualWorkspace.rclone.type=memory.
# -----------------------------------------------------------------------------
