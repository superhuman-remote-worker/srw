# Single-origin HTTPS Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Let users accept one self-signed HTTPS origin and use SRW, including login, direct session WebSockets, bundled Git and cloud.

**Architecture:** A dedicated namespaced Traefik controller terminates TLS and routes by path. A canonical public origin is separate from internal service addresses and Kubernetes ingress matching. Existing multi-host mode remains the default.

**Tech Stack:** Helm, Kubernetes Ingress, Traefik 3.x, Python/FastAPI, Angular, Keycloak, Gitea, Nextcloud, Bash/k3d.

**Spec:** `docs/superpowers/specs/2026-09-19-single-origin-https.md` (snapshot of the approved knowledge-base feature).

## Global Constraints

- Browser traffic remains HTTPS/WSS; no client trust-store installation, cert-manager, mkcert or DNS required in this mode.
- `exposure.mode` defaults to `multi-host`; opt-in value is `single-origin`.
- `exposure.singleOrigin.address` is localhost or IPv4, publicPort defaults 30443, service.nodePort defaults 30443, tls.mode is self-signed, tls.validityDays defaults 365.
- Local mode uses publicPort 8443 and k3d `127.0.0.1:8443:30443@server:0`.
- Gateway uses a pinned supported Traefik 3.x >=3.1, container port 8443, Service port 443, namespaced RBAC, annotation-only unique ingress class, hostless rules, file-provider default certificate, no CRDs or cluster-scoped discovery.
- Preserve authentication, CSRF, agent ownership/lifecycle, anti-framing and existing deployment behavior.
- Angular service workers and external MCP/SSH/JetBrains connection setup are unavailable in the preset. Top-level browser IDE stays available; embedded IDE webviews are excluded.
- Generate certificate once per render, use IP/DNS SAN correctly, and roll gateway on its checksum. Regeneration during upgrade is acceptable.
- No commits, publication, or changes to an existing cluster during implementation. Disposable test resources are owned and cleaned up by this task.

## Task 1: Chart gateway and canonical configuration

**Files:** `helm/values.yaml`, `helm/values.schema.json`, `helm/templates/_helpers.tpl`, new `_exposure.tpl` and `single-origin-gateway.yaml`, `ingress.yaml`, optional legacy ingress templates, `configmap.yaml`, `orchestrator/deployment.yaml`, relevant NetworkPolicies, `tests/test_single_origin_helm.py`.

**Interfaces:** Produce `srw.singleOrigin` (true/empty), `srw.publicOrigin` (O), `srw.singleOriginIngressClass`, `srw.singleOriginGatewayName`; adapt existing cockpit/api/auth/git/cloud/internal-Keycloak URL helpers. Produce `SESSION_PUBLIC_ORIGIN=O`, `SESSION_INGRESS_HOST=""`, unique `SESSION_INGRESS_CLASS`, `SESSION_INGRESS_SINGLE_ORIGIN=1`, `SESSION_INGRESS_TLS_SECRET=""`. Public URLs preserve non-default ports; port 443 is omitted to match the browser canonical origin and Nextcloud Host validation. The runtime task consumes these env names.

- [x] Add rendered-manifest tests, starting with this failing assertion:
  ```python
  docs = render({"exposure": {"mode": "single-origin", "singleOrigin": {"address": "192.0.2.10"}}})
  gateway = next(d for d in docs if d["kind"] == "Deployment" and d["metadata"]["name"].endswith("single-origin"))
  assert gateway["spec"]["template"]["spec"]["containers"][0]["ports"][0]["containerPort"] == 8443
  ```
- [x] Run `PYTHONPATH=src .venv/bin/pytest -q tests/test_single_origin_helm.py`; confirm missing gateway/contracts.
- [x] Implement helper validation, schema, unique gateway/class names, minimal controller/RBAC/Service, generated TLS and file middleware. Emit separate static routes for orchestrator, identity, Git/cloud strip prefixes, exact slash redirects and Cockpit; dynamic `/p` is omitted.
- [x] Derive effective BFF host-only Secure/Lax cookies and URLs; suppress old public ingresses and admit exact gateway labels through applicable policies. Reject unsupported legacy namespaces and unsupported integrations with actionable render errors or derive preset defaults where unambiguous.
- [x] Extend tests for localhost/IP SAN, key/checksum/validity, namespace/release collisions, invalid values, absent cert-manager/CRDs, rendered public/internal URLs, default multi-host regression. Run focused Helm tests and lint.

## Task 2: Runtime session routes and Git links

**Files:** `src/orchestrator/services/session_router.py`, `src/orchestrator/routers/sessions.py`, `src/orchestrator/main.py`, `src/orchestrator/security/access.py`, `tests/test_session_router.py`, `tests/test_sessions_router_prepare.py`, focused Git URL tests.

**Interfaces:** Consume Task 1 env names. Add optional `single_origin=False` to SessionRouterService without changing old callers. Preserve all existing route ownership checks. Session URL authority comes from `SESSION_PUBLIC_ORIGIN` when configured.

- [x] Add tests proving hostless ingress creation AND reconciliation, including class annotation and entrypoint; stale hostful/class-field routes must be refused by the existing fenced lifecycle, without weakening owner checks.
- [x] Add connection-response case with `SESSION_PUBLIC_ORIGIN=https://192.0.2.10:30443`; assert `wss://192.0.2.10:30443/p/<thread>/ws` and existing query/token contract.
- [x] Run targeted tests red, implement optional shape and normalized public-origin handling, then run both session suites green.
- [x] Add a Git link regression with internal base `http://gitea:3000`, external `https://192.0.2.10:30443/git` and credential-bearing internal repository URL. Assert `/git/owner/repo` survives, credentials disappear, unrelated hosts are not rewritten. Implement exact base replacement and run related access tests.

## Task 3: Bundled identity, Git and cloud integrations

**Files:** `helm/templates/services/keycloak.yaml`, `services/gitea.yaml`, `services/nextcloud.yaml`, `keycloak/bootstrap-configmap.yaml`, owned Nextcloud startup config/hook, `tests/test_single_origin_services_helm.py`.

**Interfaces:** Consume Task 1 public/internal URL helpers. Public Keycloak is O/identity, internal Keycloak includes /identity. Gitea ROOT_URL is O/git/ with internal root unchanged. Nextcloud external base is O/cloud, internal root unchanged.

- [x] Add rendered assertions for `KC_HTTP_RELATIVE_PATH=/identity`, `KC_HTTP_MANAGEMENT_RELATIVE_PATH=/`, `KC_HOSTNAME_BACKCHANNEL_DYNAMIC=true`, issuer/callback/origin values, Git ROOT_URL, cloud overwrite host/protocol/webroot and internal discovery. Run them red.
- [x] Update Keycloak environment, fresh realm import and existing-client reconciliation, using exact webOrigins O and prefixed callbacks. Preserve port-9000 health checks and internal BFF logout.
- [x] Update Gitea configuration and reconciliation to internal OIDC discovery/token/userinfo and public authorization; retain managed internal Git.
- [x] Add idempotent Nextcloud configuration for existing PVCs (trusted_domains and clearing preset-owned trusted_proxies); use official `TRUSTED_PROXIES`, fixed overwrite settings, internal discovery; update existing OIDC provider without deleting identities. Apply to Apache/FPM/protected-effect modes.
- [x] Run new chart tests plus `tests/test_nextcloud_protected_effect_helm.py`. Probe actual bundled Keycloak discovery in a disposable service when tools permit.

## Task 4: Cockpit runtime capabilities

**Files:** `helm/templates/cockpit/deployment.yaml`, `cockpit/src/app/core/environment.ts`, `cockpit/src/app/app.config.ts`, affected MCP/SSH connection panels and browser tests.

**Interfaces:** Render and consume boolean `serviceWorkerEnabled=false` and `externalClientsEnabled=false` in single-origin mode (both true by default); use existing URL helpers for same-origin browser API/auth.

- [x] Add environment tests for false, true, absent and string boolean inputs. Add component coverage showing external client configuration unavailable while browser IDE remains.
- [x] Run tests red; implement boolean parsing without losing false and `enabled: !isDevMode() && environment.serviceWorkerEnabled`.
- [x] Gate unsupported external MCP/SSH/JetBrains panels, retain browser IDE, crypto/media features and existing anti-framing. Run focused Vitest cases, production build and Python anti-framing/auth/CSRF suites.

## Task 5: Installer, presets, generator and docs

**Files:** `scripts/local-dev-up.sh`, `scripts/local-dev-tilt-up.sh`, `helm/values.local*.yaml`, `helm/ci`, `.github/workflows/main.yml`, `develop.yml`, `README.md`, `helm/README.md`, `docs/local-kubernetes.md`; external `srw-cloud/www/generator.mjs` and drift tests if accessible.

**Interfaces:** Local CLI selects single-origin before prerequisite checks. `SRW_EXPOSURE_MODE=single-origin` selects canonical localhost:8443, skipping certificate/DNS bootstrap, preserving required storage/runtime/image preparation.

- [x] Add shell harness tests asserting missing mkcert does not block new mode, generated k3d mapping, wrong existing mappings produce an actionable error, printed/Tilt origin is correct, and legacy mode keeps its bootstrap.
- [x] Run tests red; implement mode branch and mode-specific values. Add a standalone server-IP example requiring only normal application credentials/storage choices and the exposure values.
- [x] Locate accessible srw-cloud repository through authenticated repository metadata. If accessible, add generated server/local cases and regenerate fixtures; otherwise record exact missing delivery work, never hand-edit generated fixtures as a substitute.
- [x] Extend CI Helm matrices with explicitly maintained self-signed profiles and assert absence of certificate/Traefik CRDs. Document install, NodePort/NAT distinction, certificate regeneration and browser/client support boundaries.
- [x] Run shell tests, `bash -n`, Helm profile renders/lint and kubeconform.

## Task 6: Browser/deployment acceptance and final review

**Files:** new focused `cockpit/e2e/single-origin` project, acceptance documentation and feature implementation status.

- [x] Add a separate Playwright flow with `ignoreHTTPSErrors:false`, no certificate bypass, service workers allowed, fresh persistent Chromium/Firefox profiles and actual warning clicks. Make credentials/target explicit environment inputs; never run against an existing cluster by default.
- [x] Record origins for navigation/fetch/SSE/WSS, assert one origin, cookie/crypto/media behavior, no Angular SW registration and direct session socket route. Capture versions, trace and screenshot on failure.
- [x] Where runtime resources permit, deploy a disposable chart profile and verify no-SNI served certificate, API-server ingress admission, controller startup/RBAC and login/session/Git/cloud paths. Verify cert replacement on upgrade. Do not substitute fixture success for a full SRW acceptance claim.
- [x] Run all impacted tests once after integration, default and new profile schema/render checks, production build and whitespace check. Request independent whole-diff review, fix actionable findings and document any unverified live acceptance gates precisely.

Integration decision (2026-09-19): the user requested committing and pushing the
implementation and generator changes to `develop` in their respective
repositories. Keep unrelated checkout changes intact; release/main integration
and university-server deployment follow separately.

Completed implementation and disposable-cluster validation on 2026-09-19. See the
[validation report](../specs/2026-09-19-single-origin-https-validation.md) for exact
results, environment deviations, and deployment boundaries.
