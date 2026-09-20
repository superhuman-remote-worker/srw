{{/*
Chart name, truncated to 63 chars.
*/}}
{{- define "srw.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Fully qualified app name: release-name + chart-name, truncated to 63 chars.
If release name contains chart name, don't repeat it.
*/}}
{{- define "srw.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Orchestrator Kubernetes-mutation authority epoch. Only the ServiceAccount
named by the current chart receives the namespace RoleBinding. Advancing the
epoch therefore revokes predecessor replicas before their replacement image
can own workspace lifecycle mutations. Reserve the suffix length explicitly:
long release names must not truncate the authority epoch away.
*/}}
{{- define "srw.orchestratorServiceAccountName" -}}
{{- $epoch := .Values.orchestrator.workspaceLifecycleServiceAccountGeneration | default "0197" | toString -}}
{{- $base := include "srw.fullname" . | trunc 43 | trimSuffix "-" -}}
{{- printf "%s-ows%s" $base $epoch | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{/*
Temporary cross-namespace pinned adoption RBAC. Preserve the semantic suffix
while keeping every legal long Helm release name within DNS-1123's 63 bytes.
*/}}
{{- define "srw.pinnedLegacyAuthorityName" -}}
{{- $base := include "srw.fullname" . | trunc 39 | trimSuffix "-" -}}
{{- printf "%s-pinned-legacy-authority" $base -}}
{{- end }}

{{/*
Common labels applied to all resources.
*/}}
{{- define "srw.labels" -}}
helm.sh/chart: {{ include "srw.chart" . }}
{{ include "srw.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels (subset of common labels — used in matchLabels).
*/}}
{{- define "srw.selectorLabels" -}}
app.kubernetes.io/name: {{ include "srw.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Chart label value.
*/}}
{{- define "srw.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Secret name — resolves across the three secret modes:
  1. existingSecret set → use that name
  2. externalSecrets.enabled → ESO creates secret with fullname
  3. secrets.create → chart creates secret with fullname
All templates reference this single helper.
*/}}
{{- define "srw.secretName" -}}
{{- if .Values.secrets.existingSecret }}
{{- .Values.secrets.existingSecret }}
{{- else }}
{{- include "srw.fullname" . }}
{{- end }}
{{- end }}

{{/*
VM SSH key secret name.
*/}}
{{- define "srw.vmSshKeySecretName" -}}
{{- if .Values.secrets.existingVmSshKeySecret }}
{{- .Values.secrets.existingVmSshKeySecret }}
{{- else }}
{{- printf "%s-vm-ssh-key" (include "srw.fullname" .) }}
{{- end }}
{{- end }}

{{/*
ConfigMap name (used by all deployments that read from the shared configmap).
*/}}
{{- define "srw.configMapName" -}}
{{- printf "%s-config" (include "srw.fullname" .) }}
{{- end }}

{{/*
First-party image reference. A real digest pin takes precedence over the
display/update tag; an empty digest preserves the existing repository:tag
behavior.
Usage: {{ include "srw.imageRef" (dict "image" .Values.image.agent) }}
*/}}
{{- define "srw.imageRef" -}}
{{- $digest := default "" .image.digest -}}
{{- if $digest -}}
{{- printf "%s@%s" .image.repository $digest -}}
{{- else -}}
{{- printf "%s:%s" .image.repository .image.tag -}}
{{- end -}}
{{- end }}

{{/*
Bounded public deployment provenance consumed by the orchestrator and
dynamically provisioned agents. The image digest comes only from the same
value that srw.imageRef uses, so a tag can never be reported as an artifact
digest. MCP is omitted when its deployment is disabled.
*/}}
{{- define "srw.deploymentProvenanceJson" -}}
{{- $root := . -}}
{{- $components := dict -}}
{{- range $name := list "orchestrator" "agent" "cockpit" "workspace" -}}
  {{- $image := index $root.Values.image $name -}}
  {{- $declaration := index $root.Values.provenance.components $name -}}
  {{- $_ := set $components $name (dict
      "source_revision" (default "" $declaration.sourceRevision)
      "artifact_digest" (default "" $image.digest)
      "release_version" (default "" $declaration.releaseVersion)
    ) -}}
{{- end -}}
{{- if $root.Values.mcp.enabled -}}
  {{- $image := $root.Values.image.mcp -}}
  {{- $declaration := $root.Values.provenance.components.mcp -}}
  {{- $_ := set $components "mcp" (dict
      "source_revision" (default "" $declaration.sourceRevision)
      "artifact_digest" (default "" $image.digest)
      "release_version" (default "" $declaration.releaseVersion)
    ) -}}
{{- end -}}
{{- dict
    "source_url" (default "" $root.Values.provenance.sourceUrl)
    "documentation_url" (default "" $root.Values.provenance.documentationUrl)
    "components" $components
  | toJson -}}
{{- end }}

{{/*
VM controller — resource names + URLs. The controller can run in the same
namespace as the orchestrator (vmController.namespace = .Release.Namespace)
or in a dedicated namespace (the typical case). When enabled, the controller
exposes an HTTP API on Service `srw.vmControllerServiceName` port
`vmController.service.port`.
*/}}
{{- define "srw.vmControllerName" -}}
{{- printf "%s-vm-controller" (include "srw.fullname" .) }}
{{- end }}

{{- define "srw.vmControllerServiceName" -}}
{{- include "srw.vmControllerName" . }}
{{- end }}

{{- define "srw.vmControllerNamespace" -}}
{{- .Release.Namespace }}
{{- end }}

{{/* Resolve the one-release vmController.enabled alias. */}}
{{- define "srw.vmMode" -}}
{{- $mode := .Values.vm.mode | default "off" -}}
{{- if and .Values.vmController.enabled (eq $mode "off") -}}
same-cluster
{{- else -}}
{{- $mode -}}
{{- end -}}
{{- end }}

{{- define "srw.vmSameCluster" -}}
{{- if eq (include "srw.vmMode" .) "same-cluster" -}}true{{- end -}}
{{- end }}

{{- define "srw.orchestratorClusterUrl" -}}
{{- printf "http://%s-orchestrator.%s.svc.cluster.local:8085" (include "srw.fullname" .) .Release.Namespace -}}
{{- end }}

{{- define "srw.vmLifecycleAuthSecretName" -}}
{{- $legacy := .Values.infrastructureMetering.vmLifecycleAuthSecretName | default "" -}}
{{- $name := .Values.vm.lifecycleAuthSecretName | default $legacy -}}
{{- $name -}}
{{- end }}

{{/*
URL the orchestrator uses to reach the controller's HTTP API. Empty when
vmController.enabled is false or transport=nats — orchestrator falls back
to NATS / direct K8s in those cases. When transport=both, the URL is
exported so the orchestrator's HTTP transport is available even though
NATS takes priority.
*/}}
{{- define "srw.vmControllerUrl" -}}
{{- if eq (include "srw.vmMode" .) "same-cluster" }}
{{- printf "http://%s.%s.svc.cluster.local:%d"
    (include "srw.vmControllerServiceName" .)
    (include "srw.vmControllerNamespace" .)
    (int .Values.vmController.service.port) }}
{{- end }}
{{- end }}

{{/*
Component labels — extends common labels with a component identifier.
Usage: {{ include "srw.componentLabels" (dict "context" . "component" "orchestrator") }}
*/}}
{{- define "srw.componentLabels" -}}
{{ include "srw.labels" .context }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/*
Component selector labels.
Usage: {{ include "srw.componentSelectorLabels" (dict "context" . "component" "orchestrator") }}
*/}}
{{- define "srw.componentSelectorLabels" -}}
{{ include "srw.selectorLabels" .context }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/*
Internal service hostname for a component.
Usage: {{ include "srw.serviceName" (dict "context" . "component" "postgres") }}
*/}}
{{- define "srw.serviceName" -}}
{{- printf "%s-%s" (include "srw.fullname" .context) .component }}
{{- end }}

{{/*
Canonical, non-secret identity for the bundled Nextcloud protected-effect
lane. The bundle digests make a verifier/runtime change a new configuration
authority even when every timing knob remains unchanged.
*/}}
{{- define "srw.nextcloudProtectedEffectConfigJson" -}}
{{- $effect := .Values.nextcloud.protectedEffect -}}
{{- dict
      "version" 1
      "queue_bound_seconds" (int $effect.queueBoundSeconds)
      "handler_bound_seconds" (int $effect.handlerBoundSeconds)
      "clock_skew_bound_seconds" (int $effect.clockSkewBoundSeconds)
      "safety_margin_seconds" (int $effect.safetyMarginSeconds)
      "capability_max_age_seconds" (int $effect.capabilityMaxAgeSeconds)
      "max_children" (int $effect.maxChildren)
      "max_body_bytes" 65536
      "common_sha256" (.Files.Get "files/nextcloud-protected-effect/common.php" | sha256sum)
      "prepend_sha256" (.Files.Get "files/nextcloud-protected-effect/prepend.php" | sha256sum)
      "capability_sha256" (.Files.Get "files/nextcloud-protected-effect/capability.php" | sha256sum)
      "fpm_launcher_sha256" (.Files.Get "files/nextcloud-protected-effect/start-fpm.sh" | sha256sum)
      "nginx_sha256" (.Files.Get "files/nextcloud-protected-effect/nginx.conf" | sha256sum)
    | toJson -}}
{{- end }}

{{- define "srw.nextcloudProtectedEffectConfigSha256" -}}
{{- include "srw.nextcloudProtectedEffectConfigJson" . | sha256sum -}}
{{- end }}

{{- define "srw.nextcloudProtectedEffectName" -}}
{{- $base := include "srw.fullname" . | trunc 46 | trimSuffix "-" -}}
{{- printf "%s-protected-effect" $base -}}
{{- end }}

{{- define "srw.nextcloudProtectedEffectSecretName" -}}
{{- if .Values.nextcloud.protectedEffect.hmacSecretName -}}
{{- .Values.nextcloud.protectedEffect.hmacSecretName -}}
{{- else -}}
{{- include "srw.nextcloudProtectedEffectName" . -}}
{{- end -}}
{{- end }}

{{/*
Whether the bundled protected-effect lane is deployed.

`nextcloud.protectedEffect.enabled` is deliberately tri-state:

  unset (default) — derive it. Protected cloud mode has exactly one valid
                    server topology, so a bundled+internal Nextcloud follows
                    agent.protectedCloudModeEnabled rather than making the
                    operator assert the same decision twice.
  true            — deploy the lane regardless of the feature flag. This is the
                    staged rollout: stand the lane (and its adoption-once HMAC
                    root) up first, verify it, then flip the flag.
  false           — refuse to deploy it. Combined with protected cloud mode
                    this is a contradiction and still fails the render, because
                    the mode is inert without the lane (nextcloud.py raises
                    NOT_SUPPORTED on every capability request).
*/}}
{{- define "srw.nextcloudProtectedEffectEnabled" -}}
{{- $effect := .Values.nextcloud.protectedEffect -}}
{{- if kindIs "invalid" $effect.enabled -}}
{{- $protectedCloud := eq (lower (toString .Values.agent.protectedCloudModeEnabled)) "true" -}}
{{- if and $protectedCloud .Values.nextcloud.enabled .Values.nextcloud.internal -}}true{{- end -}}
{{- else if $effect.enabled -}}true{{- end -}}
{{- end }}

{{- define "srw.nextcloudProtectedEffectValidate" -}}
{{- $effect := .Values.nextcloud.protectedEffect -}}
{{- $effectEnabled := eq (include "srw.nextcloudProtectedEffectEnabled" .) "true" -}}
{{- $protectedCloud := eq (lower (toString .Values.agent.protectedCloudModeEnabled)) "true" -}}
{{- if and $protectedCloud .Values.nextcloud.enabled (not $effectEnabled) -}}
  {{- if kindIs "invalid" $effect.enabled -}}
    {{- fail "agent.protectedCloudModeEnabled=true requires a bundled nextcloud.internal=true deployment; the protected-effect lane cannot be derived for an external Nextcloud" -}}
  {{- else -}}
    {{- fail "agent.protectedCloudModeEnabled=true with bundled Nextcloud contradicts nextcloud.protectedEffect.enabled=false; unset it to derive the lane from the feature flag, or turn the flag off" -}}
  {{- end -}}
{{- end -}}
{{- if and $protectedCloud (eq (default "" .Values.cloud.externalBackend) "nextcloud") -}}
  {{- fail "agent.protectedCloudModeEnabled=true is not supported for external Nextcloud without an attested server-enforced protected-effect lane" -}}
{{- end -}}
{{- if $effectEnabled -}}
  {{- if not (and .Values.nextcloud.enabled .Values.nextcloud.internal) -}}
    {{- fail "nextcloud.protectedEffect.enabled requires bundled nextcloud.enabled=true and nextcloud.internal=true; external Nextcloud is ineligible without an equivalent server-enforced effect lane" -}}
  {{- end -}}
  {{- if and .Values.externalSecrets.enabled (not $effect.hmacSecretName) (not $effect.hmacVaultPath) -}}
    {{- fail "nextcloud.protectedEffect.enabled with External Secrets requires nextcloud.protectedEffect.hmacVaultPath (a dedicated path that is never imported into agent pods) or hmacSecretName" -}}
  {{- end -}}
  {{- if and (not .Values.externalSecrets.enabled) (not .Values.secrets.create) (not $effect.hmacSecretName) -}}
    {{- fail "nextcloud.protectedEffect.enabled requires a dedicated hmacSecretName unless secrets.create or externalSecrets is managing the protected-effect Secret" -}}
  {{- end -}}
  {{- $timings := dict
        "queueBoundSeconds" $effect.queueBoundSeconds
        "handlerBoundSeconds" $effect.handlerBoundSeconds
        "clockSkewBoundSeconds" $effect.clockSkewBoundSeconds
        "safetyMarginSeconds" $effect.safetyMarginSeconds
        "capabilityMaxAgeSeconds" $effect.capabilityMaxAgeSeconds -}}
  {{- range $name, $value := $timings -}}
    {{- if or (le (int $value) 0) (gt (int $value) 86400) -}}
      {{- fail (printf "nextcloud.protectedEffect.%s must be an integer between 1 and 86400" $name) -}}
    {{- end -}}
  {{- end -}}
  {{- if gt (int $effect.handlerBoundSeconds) 60 -}}
    {{- fail "nextcloud.protectedEffect.handlerBoundSeconds must not exceed the protected Nginx lane's 65-second read ceiling" -}}
  {{- end -}}
  {{- if or (le (int $effect.maxChildren) 0) (gt (int $effect.maxChildren) 64) -}}
    {{- fail "nextcloud.protectedEffect.maxChildren must be an integer between 1 and 64" -}}
  {{- end -}}
{{- end -}}
{{- end }}

{{/*
Domain-derived URLs. Each component host is `global.hostnames.<key>` when
set, otherwise `<subdomain>.<global.domain>`. Centralised in srw.host so a
single override propagates to ingress, configmap, OIDC redirects, and the
cockpit env-init script.

Usage:
  {{ include "srw.host" (dict "context" . "key" "git" "default" "git") }}
  {{ include "srw.host" (dict "context" . "key" "cockpit" "default" "") }}  # root
*/}}
{{- define "srw.host" -}}
{{- $ctx := .context -}}
{{- $hostnames := default (dict) $ctx.Values.global.hostnames -}}
{{- $override := index $hostnames .key -}}
{{- if $override -}}
{{- $override -}}
{{- else if eq .default "" -}}
{{- required "global.domain is required" $ctx.Values.global.domain -}}
{{- else -}}
{{- printf "%s.%s" .default (required "global.domain is required" $ctx.Values.global.domain) -}}
{{- end -}}
{{- end }}

{{/*
Strip the scheme from an https:// URL so URL helpers can be reused as
ingress / TLS host strings without duplicating per-component logic.
*/}}
{{- define "srw.urlHost" -}}
{{- . | trimPrefix "https://" | trimPrefix "http://" -}}
{{- end }}

{{/*
URL scheme — "https" when TLS is enabled on the ingress, "http" otherwise.
Centralized so every URL helper picks up local-dev (no-TLS) deployments.
*/}}
{{- define "srw.urlScheme" -}}
{{- if or (include "srw.singleOrigin" .) .Values.ingress.tls.enabled -}}https{{- else -}}http{{- end -}}
{{- end }}

{{- define "srw.cockpitUrl" -}}
{{- if include "srw.singleOrigin" . -}}
{{- include "srw.publicOrigin" . -}}
{{- else -}}
{{- printf "%s://%s" (include "srw.urlScheme" .) (include "srw.host" (dict "context" . "key" "cockpit" "default" "")) }}
{{- end -}}
{{- end }}

{{- define "srw.apiUrl" -}}
{{- if include "srw.singleOrigin" . -}}
{{- include "srw.publicOrigin" . -}}
{{- else -}}
{{- printf "%s://%s" (include "srw.urlScheme" .) (include "srw.host" (dict "context" . "key" "api" "default" "api")) }}
{{- end -}}
{{- end }}

{{/*
The origin cockpit's own SPA actually dials for the API — NOT always
"srw.apiUrl". When `auth.bff.sameOriginApi` is on, cockpit's served env.js
points `apiUrl` at the cockpit's own origin (`srw.cockpitUrl`) instead, so
same-origin path routing on the cockpit ingress can carry `/api`, `/auth`,
`/ws` without a cross-site cookie (see cockpit/deployment.yaml's env.js
ternary, which this mirrors exactly — keep the two in sync).

Any ingress rule that must match wherever the BROWSER's own JS will dial —
as opposed to the REST api ingress, which is deliberately host-pinned to
`srw.apiUrl` regardless of this flag — needs to key off THIS helper, not
`srw.apiUrl` directly. Ported to ssh-gateway/ingress.yaml after a live gate
(task-7-brief.md, controller correction C1) found the two host names had
drifted apart: cockpit dialled `apiHost` (`new URL(environment.apiUrl)
.hostname`, persistent-chat.component.ts) on the cockpit origin, but the
gateway's WSS ingress was still pinned to the bare `srw.apiUrl` host, so a
generated ProxyCommand routed to the orchestrator's own ASGI app instead of
the gateway pod and came back a bare 403 with nothing in the gateway's logs.
*/}}
{{- define "srw.cockpitFacingApiUrl" -}}
{{- ternary (include "srw.cockpitUrl" .) (include "srw.apiUrl" .) .Values.auth.bff.sameOriginApi }}
{{- end }}

{{- define "srw.authUrl" -}}
{{- if include "srw.singleOrigin" . -}}
{{- printf "%s/identity" (include "srw.publicOrigin" .) -}}
{{- else if and .Values.keycloak.enabled (not .Values.keycloak.internal) .Values.keycloak.externalIssuerUrl }}
{{- .Values.keycloak.externalIssuerUrl }}
{{- else }}
{{- printf "%s://%s" (include "srw.urlScheme" .) (include "srw.host" (dict "context" . "key" "auth" "default" "auth")) }}
{{- end }}
{{- end }}

{{/*
Internal cluster URL for the orchestrator. Used by other in-cluster pods
that POST to the orchestrator without round-tripping through the public
ingress (Keycloak's backchannel-logout callback, future webhook receivers).
Bypassing the ingress avoids two foot-guns: pods can't always resolve the
public hostname (split-DNS, the `localhost` loopback-hijack on k3d, etc.),
and even when they can, the public path is slower and may pin TLS certs
the in-cluster CA bundle doesn't trust.
*/}}
{{- define "srw.orchestratorInternalUrl" -}}
{{- printf "http://%s-orchestrator:8085" (include "srw.fullname" .) -}}
{{- end }}

{{/*
Replicate the aliased official subchart's fullname helper so the orchestrator
can fetch discovery over the exact generated ClusterIP Service name.
*/}}
{{- define "srw.collaboraServiceName" -}}
{{- if .Values.collabora.fullnameOverride -}}
{{- .Values.collabora.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default "collabora" .Values.collabora.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end }}

{{- define "srw.collaboraInternalUrl" -}}
{{- printf "http://%s:9980" (include "srw.collaboraServiceName" .) -}}
{{- end }}

{{/*
Internal cluster URL for Keycloak — used by orchestrator for back-channel JWKS fetches.
*/}}
{{- define "srw.keycloakInternalUrl" -}}
{{- if .Values.keycloak.internal }}
{{- printf "http://%s-keycloak:8080%s" (include "srw.fullname" .) (ternary "/identity" "" (ne (include "srw.singleOrigin" .) "")) }}
{{- else if .Values.keycloak.externalInternalUrl }}
{{- .Values.keycloak.externalInternalUrl }}
{{- else }}
{{- include "srw.authUrl" . }}
{{- end }}
{{- end }}

{{/*
JDBC URL the bundled Keycloak uses to talk to its own Postgres. Resolves to the
in-cluster `srw-keycloakdb` Service when databases.keycloak.internal is true,
or to the operator-supplied externalUrl otherwise (e.g. managed Postgres).
*/}}
{{/*
Host of the Keycloak database, for anything that needs a hostname rather than
a JDBC URL -- notably the wait-for-db init container, which polls it with nc.
Internal only; in external mode the URL is opaque and there is nothing to wait
for that the chart deployed.
*/}}
{{- define "srw.keycloakDbHost" -}}
{{- $suffix := "" -}}
{{- if eq (include "srw.dbEngine" (dict "context" . "db" .Values.databases.keycloak)) "cnpg" -}}
{{- $suffix = "-rw" -}}
{{- end -}}
{{- printf "%s-keycloakdb%s" (include "srw.fullname" .) $suffix -}}
{{- end }}

{{- define "srw.keycloakDbJdbcUrl" -}}
{{- if .Values.databases.keycloak.internal -}}
{{- $suffix := "" -}}
{{- if eq (include "srw.dbEngine" (dict "context" . "db" .Values.databases.keycloak)) "cnpg" -}}
{{- $suffix = "-rw" -}}
{{- end -}}
jdbc:postgresql://{{ include "srw.fullname" . }}-keycloakdb{{ $suffix }}:5432/keycloak
{{- else -}}
{{- required "databases.keycloak.externalUrl is required when databases.keycloak.internal is false" .Values.databases.keycloak.externalUrl -}}
{{- end -}}
{{- end }}

{{/*
Connection parts the bundled Gitea uses for its metadata database when
gitea.database.type is "postgres". Resolves to the in-cluster `srw-giteadb`
Service when databases.gitea.internal is true, or to the operator-supplied
external server otherwise. Credentials never appear here — Gitea reads
GITEA__database__USER / __PASSWD from the Secret.
*/}}
{{- define "srw.giteaDbHost" -}}
{{- if .Values.databases.gitea.internal -}}
{{- $suffix := "" -}}
{{- if eq (include "srw.dbEngine" (dict "context" . "db" .Values.databases.gitea)) "cnpg" -}}
{{- $suffix = "-rw" -}}
{{- end -}}
{{- printf "%s-giteadb%s" (include "srw.fullname" .) $suffix -}}
{{- else -}}
{{- required "databases.gitea.externalHost is required when databases.gitea.internal is false" .Values.databases.gitea.externalHost -}}
{{- end -}}
{{- end }}

{{- define "srw.giteaDbPort" -}}
{{- if .Values.databases.gitea.internal -}}
5432
{{- else -}}
{{- .Values.databases.gitea.externalPort | default 5432 -}}
{{- end -}}
{{- end }}

{{- define "srw.giteaDbName" -}}
{{- if .Values.databases.gitea.internal -}}
gitea
{{- else -}}
{{- .Values.databases.gitea.externalDb | default "gitea" -}}
{{- end -}}
{{- end }}

{{/*
True when the bundled Gitea should use Postgres for its metadata DB. Any value
other than "sqlite3" is rejected loudly rather than silently falling back —
a typo here would otherwise point a populated Gitea at a fresh database.
*/}}
{{- define "srw.giteaUsesPostgres" -}}
{{- $type := .Values.gitea.database.type | default "postgres" -}}
{{- if eq $type "postgres" -}}
true
{{- else if eq $type "sqlite3" -}}
{{- else -}}
{{- fail (printf "gitea.database.type must be \"postgres\" or \"sqlite3\", got %q" $type) -}}
{{- end -}}
{{- end }}

{{/*
Resources for the Keycloak bootstrap Job.

  - srw.keycloakBootstrapServer  — URL kcadm authenticates against.
      Resolution: bootstrap.serverUrl override → externalInternalUrl
      (stripped of any /realms/... suffix) → externalIssuerUrl (same
      strip). Errors if none are set, since the Job has nothing to talk to.
  - srw.keycloakBootstrapRealm   — target realm. Defaults to keycloak.realm.
  - srw.keycloakBootstrapImage   — image with kcadm.sh. Defaults to
      keycloak.image so the Job re-uses what's already pulled.
*/}}
{{- define "srw.keycloakBootstrapServer" -}}
{{- $b := .Values.keycloak.bootstrap -}}
{{- if $b.serverUrl -}}
{{- $b.serverUrl -}}
{{- else if .Values.keycloak.externalInternalUrl -}}
{{- regexReplaceAll "/realms/.*$" .Values.keycloak.externalInternalUrl "" -}}
{{- else if .Values.keycloak.externalIssuerUrl -}}
{{- regexReplaceAll "/realms/.*$" .Values.keycloak.externalIssuerUrl "" -}}
{{- else -}}
{{- fail "keycloak.bootstrap.enabled requires keycloak.bootstrap.serverUrl, keycloak.externalInternalUrl, or keycloak.externalIssuerUrl" -}}
{{- end -}}
{{- end }}

{{- define "srw.keycloakBootstrapRealm" -}}
{{- default .Values.keycloak.realm .Values.keycloak.bootstrap.realm -}}
{{- end }}

{{- define "srw.keycloakBootstrapImage" -}}
{{- default .Values.keycloak.image .Values.keycloak.bootstrap.image -}}
{{- end }}

{{- define "srw.keycloakBootstrapName" -}}
{{- printf "%s-keycloak-bootstrap" (include "srw.fullname" .) -}}
{{- end }}

{{/*
NATS hub names + URL resolver. When nats.internal is true, the chart
deploys a StatefulSet + Service named <fullname>-nats and the orchestrator's
NATS_URL points at the in-cluster Service. When false, NATS_URL is the
user-supplied .Values.nats.url (empty disables VM lifecycle).

Setting both nats.internal=true and nats.url is a chart-render error
(enforced in helm/templates/nats/configmap.yaml at top).
*/}}
{{- define "srw.natsName" -}}
{{- printf "%s-nats" (include "srw.fullname" .) -}}
{{- end }}

{{- define "srw.natsUrl" -}}
{{- if .Values.nats.internal -}}
{{- printf "nats://%s.%s.svc.cluster.local:4222" (include "srw.natsName" .) .Release.Namespace -}}
{{- else -}}
{{- .Values.nats.url -}}
{{- end -}}
{{- end }}

{{/*
Per-orchestrator id used to scope vm.lifecycle.* NATS subjects. Defaults to
the chart fullname (srw, srw-prod, …) so single-cluster installs Just Work;
override .Values.orchestratorId when sharing a NATS hub across orchestrators
so each one binds disjoint vm.lifecycle.*.{id} subjects. Must match the paired
VM controller chart's orchestratorId.
*/}}
{{- define "srw.orchestratorId" -}}
{{- default (include "srw.fullname" .) .Values.orchestratorId -}}
{{- end }}

{{/*
Effective Vault path the bootstrap ExternalSecret reads from.
  - If keycloak.bootstrap.adminCredentialsVaultPath is set explicitly, use it.
  - Otherwise fall back to externalSecrets.vaultPath (the main bundle).
The fallback lets a single Vault entry serve both runtime and bootstrap, which
is the common case — KC_ADMIN_*, MCP_OIDC_CLIENT_SECRET, and CLOUD_SERVICE_*
live alongside the rest of the runtime config.
Returns "" when neither path is set.
*/}}
{{- define "srw.keycloakBootstrapVaultPath" -}}
{{- coalesce .Values.keycloak.bootstrap.adminCredentialsVaultPath .Values.externalSecrets.vaultPath -}}
{{- end }}

{{/*
Name of the K8s Secret holding the bootstrap admin credentials (KC_ADMIN_USER,
KC_ADMIN_PASSWORD, MCP_OIDC_CLIENT_SECRET, and optionally CLOUD_SERVICE_USER /
CLOUD_SERVICE_PASSWORD). Resolves to either:
  - the user-provided pre-existing Secret (.Values.keycloak.bootstrap.adminCredentialsSecret), or
  - the chart-managed Secret synced by the bootstrap ExternalSecret pre-install
    hook (when an effective Vault path resolves):
    `<fullname>-keycloak-bootstrap-creds`
*/}}
{{- define "srw.keycloakBootstrapAdminSecretName" -}}
{{- if .Values.keycloak.bootstrap.adminCredentialsSecret -}}
{{- .Values.keycloak.bootstrap.adminCredentialsSecret -}}
{{- else if include "srw.keycloakBootstrapVaultPath" . -}}
{{- printf "%s-keycloak-bootstrap-creds" (include "srw.fullname" .) -}}
{{- end -}}
{{- end }}

{{- define "srw.gitUrl" -}}
{{- if include "srw.singleOrigin" . -}}
{{- printf "%s/git" (include "srw.publicOrigin" .) -}}
{{- else if and .Values.gitea.enabled (not .Values.gitea.internal) .Values.gitea.externalUrl }}
{{- .Values.gitea.externalUrl }}
{{- else }}
{{- printf "%s://%s" (include "srw.urlScheme" .) (include "srw.host" (dict "context" . "key" "git" "default" "git")) }}
{{- end }}
{{- end }}

{{/*
Internal cluster URL for Gitea — used by orchestrator/agents for back-end API calls.
*/}}
{{- define "srw.giteaInternalUrl" -}}
{{- if .Values.gitea.internal }}
{{- printf "http://%s-gitea:3000" (include "srw.fullname" .) }}
{{- else if .Values.gitea.internalUrl }}
{{- .Values.gitea.internalUrl }}
{{- else }}
{{- include "srw.gitUrl" . }}
{{- end }}
{{- end }}

{{- define "srw.cloudUrl" -}}
{{- if include "srw.singleOrigin" . -}}
{{- printf "%s/cloud" (include "srw.publicOrigin" .) -}}
{{- else if and (not .Values.opencloud.enabled) (not .Values.nextcloud.enabled) .Values.cloud.externalUrl }}
{{- .Values.cloud.externalUrl }}
{{- else }}
{{- printf "%s://%s" (include "srw.urlScheme" .) (include "srw.host" (dict "context" . "key" "cloud" "default" "cloud")) }}
{{- end }}
{{- end }}

{{/*
Internal URL the agent uses for server-to-server calls to an external cloud.
Falls back to externalUrl if externalServiceUrl is not set.
*/}}
{{- define "srw.cloudServiceUrl" -}}
{{- if .Values.cloud.externalServiceUrl }}
{{- .Values.cloud.externalServiceUrl }}
{{- else }}
{{- .Values.cloud.externalUrl }}
{{- end }}
{{- end }}

{{- define "srw.mcpUrl" -}}
{{- if include "srw.singleOrigin" . -}}
{{- include "srw.publicOrigin" . -}}
{{- else -}}
{{- printf "%s://%s" (include "srw.urlScheme" .) (include "srw.host" (dict "context" . "key" "mcp" "default" "mcp")) -}}
{{- end -}}
{{- end }}

{{- define "srw.headscaleUrl" -}}
{{- if .Values.headscale.url }}
{{- .Values.headscale.url }}
{{- else if include "srw.singleOrigin" . -}}
{{- else }}
{{- printf "%s://%s" (include "srw.urlScheme" .) (include "srw.host" (dict "context" . "key" "headscale" "default" "headscale")) }}
{{- end }}
{{- end }}

{{- define "srw.neo4jBoltHost" -}}
{{- if include "srw.singleOrigin" . -}}
{{- printf "%s-neo4j" (include "srw.fullname" .) -}}
{{- else -}}
{{- include "srw.host" (dict "context" . "key" "neo4jBolt" "default" "bolt-neo4j") }}
{{- end -}}
{{- end }}

{{/*
Hosts for the optional admin UIs and the cockpit deep-link to MinIO. These
have no URL helper because nothing else in the chart uses them as URLs —
they're consumed only as ingress hosts and as window.env.* deep-links.
*/}}
{{- define "srw.neo4jBrowserHost" -}}
{{- if include "srw.singleOrigin" . -}}
{{- include "srw.urlHost" (include "srw.publicOrigin" .) -}}
{{- else -}}
{{- include "srw.host" (dict "context" . "key" "neo4j" "default" "neo4j") }}
{{- end -}}
{{- end }}

{{- define "srw.pgadminHost" -}}
{{- if include "srw.singleOrigin" . -}}
{{- include "srw.urlHost" (include "srw.publicOrigin" .) -}}
{{- else -}}
{{- include "srw.host" (dict "context" . "key" "pgadmin" "default" "pgadmin") }}
{{- end -}}
{{- end }}

{{- define "srw.dozzleHost" -}}
{{- if include "srw.singleOrigin" . -}}
{{- include "srw.urlHost" (include "srw.publicOrigin" .) -}}
{{- else -}}
{{- include "srw.host" (dict "context" . "key" "dozzle" "default" "dozzle") }}
{{- end -}}
{{- end }}

{{- define "srw.minioHost" -}}
{{- if include "srw.singleOrigin" . -}}
{{- include "srw.urlHost" (include "srw.publicOrigin" .) -}}
{{- else -}}
{{- include "srw.host" (dict "context" . "key" "minio" "default" "minio") }}
{{- end -}}
{{- end }}

{{/*
Database connection parts — host/port/db for postgres + vector.

Credentials (user/password) live in the Secret only; the DSN is composed at
runtime by the application (orchestrator/utils/db_url.py and src/utils/
db_url.py). That avoids the redundancy + URL-encoding footgun of also
shipping a DATABASE_URL/VECTOR_DB_URL Vault key.

For external mode, set databases.<which>.externalHost/externalPort/
externalDb in values; the chart still injects the host/port/db via
ConfigMap, and only the credentials come from the Secret.
*/}}
{{/*
"-rw" ONLY at engine `cnpg`. While `migrating`, the import is still reading
from the legacy Service and consumers are still writing to it -- repointing
then would cut over before the data has arrived. External mode ignores the
engine entirely: that hostname belongs to someone else.
*/}}
{{- define "srw.postgresHost" -}}
{{- if .Values.databases.postgres.internal -}}
{{- $suffix := "" -}}
{{- if eq (include "srw.dbEngine" (dict "context" . "db" .Values.databases.postgres)) "cnpg" -}}
{{- $suffix = "-rw" -}}
{{- end -}}
{{- printf "%s-postgres%s" (include "srw.fullname" .) $suffix -}}
{{- else -}}
{{- required "databases.postgres.externalHost is required when internal=false" .Values.databases.postgres.externalHost -}}
{{- end -}}
{{- end }}

{{/*
Read-only-tolerant hostname for the main Postgres — for consumers that only
ever SELECT (today: the KEDA `postgresql` scaler in
agent/stateless-scaledobject.yaml). Same resolution as srw.postgresHost, but at
engine `cnpg` it names the "-r" Service: any instance, primary included. NOT
"-ro" — that Service selects replicas only and has NO endpoints on a
single-instance cluster (the non-HA profile), so a scaler pointed at it would
fail to connect and the pool would sit at its floor without a visible error.
*/}}
{{- define "srw.postgresReadHost" -}}
{{- if .Values.databases.postgres.internal -}}
{{- $suffix := "" -}}
{{- if eq (include "srw.dbEngine" (dict "context" . "db" .Values.databases.postgres)) "cnpg" -}}
{{- $suffix = "-r" -}}
{{- end -}}
{{- printf "%s-postgres%s" (include "srw.fullname" .) $suffix -}}
{{- else -}}
{{- required "databases.postgres.externalHost is required when internal=false" .Values.databases.postgres.externalHost -}}
{{- end -}}
{{- end }}

{{- define "srw.postgresPort" -}}
{{- if .Values.databases.postgres.internal -}}
5432
{{- else -}}
{{- .Values.databases.postgres.externalPort | default 5432 -}}
{{- end -}}
{{- end }}

{{- define "srw.postgresDb" -}}
{{- if .Values.databases.postgres.internal -}}
srw
{{- else -}}
{{- .Values.databases.postgres.externalDb | default "srw" -}}
{{- end -}}
{{- end }}

{{/*
"-rw" ONLY at engine `cnpg`. While `migrating`, the import is still reading
from the legacy Service and consumers are still writing to it -- repointing
then would cut over before the data has arrived. External mode ignores the
engine entirely: that hostname belongs to someone else.
*/}}
{{- define "srw.vectorPostgresHost" -}}
{{- if .Values.databases.vector.internal -}}
{{- $suffix := "" -}}
{{- if eq (include "srw.dbEngine" (dict "context" . "db" .Values.databases.vector)) "cnpg" -}}
{{- $suffix = "-rw" -}}
{{- end -}}
{{- printf "%s-pgvector%s" (include "srw.fullname" .) $suffix -}}
{{- else -}}
{{- required "databases.vector.externalHost is required when internal=false" .Values.databases.vector.externalHost -}}
{{- end -}}
{{- end }}

{{- define "srw.vectorPostgresPort" -}}
{{- if .Values.databases.vector.internal -}}
5432
{{- else -}}
{{- .Values.databases.vector.externalPort | default 5432 -}}
{{- end -}}
{{- end }}

{{- define "srw.vectorPostgresDb" -}}
{{- if .Values.databases.vector.internal -}}
srw_vector
{{- else -}}
{{- .Values.databases.vector.externalDb | default "srw_vector" -}}
{{- end -}}
{{- end }}

{{/*
"-rw" ONLY at engine `cnpg`. While `migrating`, the import is still reading
from the legacy Service and consumers are still writing to it -- repointing
then would cut over before the data has arrived. External mode ignores the
engine entirely: that hostname belongs to someone else.
*/}}
{{- define "srw.auditPostgresHost" -}}
{{- if .Values.databases.audit.internal -}}
{{- $suffix := "" -}}
{{- if eq (include "srw.dbEngine" (dict "context" . "db" .Values.databases.audit)) "cnpg" -}}
{{- $suffix = "-rw" -}}
{{- end -}}
{{- printf "%s-auditdb%s" (include "srw.fullname" .) $suffix -}}
{{- else -}}
{{- required "databases.audit.externalHost is required when internal=false" .Values.databases.audit.externalHost -}}
{{- end -}}
{{- end }}

{{- define "srw.auditPostgresPort" -}}
{{- if .Values.databases.audit.internal -}}
5432
{{- else -}}
{{- .Values.databases.audit.externalPort | default 5432 -}}
{{- end -}}
{{- end }}

{{- define "srw.auditPostgresDb" -}}
{{- if .Values.databases.audit.internal -}}
srw_audit
{{- else -}}
{{- .Values.databases.audit.externalDb | default "srw_audit" -}}
{{- end -}}
{{- end }}

{{/*
Neo4j Bolt URL — internal cluster service or external URL.
*/}}
{{- define "srw.neo4jUrl" -}}
{{- if .Values.databases.neo4j.internal }}
{{- printf "bolt://%s-neo4j:7688" (include "srw.fullname" .) }}
{{- else }}
{{- required "databases.neo4j.externalUrl is required when databases.neo4j.internal=false" .Values.databases.neo4j.externalUrl }}
{{- end }}
{{- end }}

{{/*
Whether the bundled single-node object store runs.

Tri-state on purpose. `true` and `false` are explicit operator decisions and
always win. The default is null = AUTO: bring your own store by setting
`s3.endpoint`, or say nothing and get the bundled one.

Auto exists because every consumer of the object store fails SILENTLY without
it — canvas durability keeps no copy, workspace snapshots never capture, the
virtual tier stays unwired. "Operator said nothing" must therefore resolve to a
working store rather than to a deployment that looks healthy and quietly
forgets things.

Keyed on `s3.endpoint` alone, not on the virtual-tier endpoint: it is the
primary "do you already have an object store" signal, and an operator who has
one can point both consumers at it. This also keeps the rule one comparison
long, so a reader can predict the outcome without tracing the tier config.
*/}}
{{- define "srw.garageEnabled" -}}
{{- if kindIs "invalid" .Values.garage.enabled -}}
{{- if not (.Values.s3).endpoint -}}true{{- end -}}
{{- else if .Values.garage.enabled -}}
true
{{- end -}}
{{- end -}}

{{/*
Effective S3 endpoint for snapshots. External (s3.endpoint) wins; otherwise the
bundled Garage service when enabled; otherwise empty (snapshots disabled).
*/}}
{{- define "srw.effectiveS3Endpoint" -}}
{{- if .Values.s3.endpoint -}}
{{- .Values.s3.endpoint -}}
{{- else if (include "srw.garageEnabled" .) -}}
{{- printf "http://%s:3900" (include "srw.serviceName" (dict "context" . "component" "garage")) -}}
{{- end -}}
{{- end -}}

{{/*
Effective virtual-workspace S3 endpoint. External wins; else bundled Garage.
*/}}
{{- define "srw.effectiveVwEndpoint" -}}
{{- if .Values.virtualWorkspace.s3.endpoint -}}
{{- .Values.virtualWorkspace.s3.endpoint -}}
{{- else if (include "srw.garageEnabled" .) -}}
{{- printf "http://%s:3900" (include "srw.serviceName" (dict "context" . "component" "garage")) -}}
{{- end -}}
{{- end -}}

{{/*
Effective rclone backend type for the virtual tier. Explicit value wins; else
"s3" when Garage is bundled; else "" (tier disabled).
*/}}
{{- define "srw.effectiveVwRcloneType" -}}
{{- if .Values.virtualWorkspace.rclone.type -}}
{{- .Values.virtualWorkspace.rclone.type -}}
{{- else if (include "srw.garageEnabled" .) -}}
{{- "s3" -}}
{{- end -}}
{{- end -}}

{{/*
Effective rclone root (bucket) for the virtual tier. Explicit value wins; else
the bundled Garage workspace bucket.
*/}}
{{- define "srw.effectiveVwRcloneRoot" -}}
{{- if .Values.virtualWorkspace.rclone.root -}}
{{- .Values.virtualWorkspace.rclone.root -}}
{{- else if (include "srw.garageEnabled" .) -}}
{{- .Values.garage.buckets.workspaces -}}
{{- end -}}
{{- end -}}

{{/*
Effective rclone S3 provider profile. When auto-wiring to bundled Garage (no
external vw endpoint), use "Other" (Garage's rclone-compatible profile,
path-style). Otherwise the configured provider (default "Minio").
*/}}
{{- define "srw.effectiveVwProvider" -}}
{{- if and (include "srw.garageEnabled" .) (not .Values.virtualWorkspace.s3.endpoint) -}}
{{- "Other" -}}
{{- else -}}
{{- .Values.virtualWorkspace.s3.provider | default "Minio" -}}
{{- end -}}
{{- end -}}

{{/*
Effective Secret name for the Canvas gateway's restricted PostgreSQL login.
Chart-created and Vault/ESO sources use the stable chart-owned name; an
operator-precreated Secret keeps its explicit name.
*/}}
{{- define "srw.canvasGatewayDatabaseSecretName" -}}
{{- $credentials := .Values.canvas.livePreview.viewer.database.credentials -}}
{{- if or $credentials.create (ne (trim $credentials.vaultPath) "") -}}
{{- printf "%s-canvas-gateway-db" (include "srw.fullname" .) -}}
{{- else -}}
{{- $credentials.existingSecret -}}
{{- end -}}
{{- end -}}

{{/*
Effective Secret name consumed by CloudNativePG's DatabaseRole. Chart-created
and Vault/ESO credentials get a dedicated basic-auth projection so upgrades do
not have to mutate the type of the gateway's long-lived Opaque Secret. An
operator-precreated CNPG-compatible Secret can serve both consumers.
*/}}
{{- define "srw.canvasGatewayDatabaseRoleSecretName" -}}
{{- $credentials := .Values.canvas.livePreview.viewer.database.credentials -}}
{{- if or $credentials.create (ne (trim $credentials.vaultPath) "") -}}
{{- printf "%s-canvas-gateway-db-cnpg" (include "srw.fullname" .) -}}
{{- else -}}
{{- $credentials.existingSecret -}}
{{- end -}}
{{- end -}}

{{/*
Keycloak SMTP port and STARTTLS flag for the realm bootstrap (kcadm).

Both are WHITELISTS, not sanitizers: each emits either a literal drawn from a
closed set, or the default. Nothing else can reach the rendered output, for
any value of email.smtp.port / email.smtp.useTls -- nil, bool, int64,
float64, string, map or slice, which is every Go kind Helm's loaders can
produce (--set, --set-string, --set-json, a values file, --set key=null).

Why a whitelist: four blacklist remedies shipped here and each one closed the
case in front of it while opening another. `default` swallowed a real boolean
false; `toString | default` then forwarded the literal "<nil>"; `kindIs
"invalid"` let an empty string through verbatim; adding `eq (toString .) ""`
let a whitespace-only string through. A blacklist is only ever as complete as
the list of shapes someone thought to try.

Why this is total, argued from structure rather than from a list of cases:
  - `toString` is total: Sprig falls through to fmt's "%v" for every type,
    nil included, so the guard always compares a string, never a bare Go
    value that could error or format surprisingly.
  - Membership is exact equality against a literal set (starttls), or a fully
    anchored ASCII-digit match (port). Go's ^ and $ are start/end of TEXT
    without (?m), so an embedded newline cannot slip past the anchors.
  - The else-branch is the DEFAULT, not the input: an unrecognised value is
    dropped, never forwarded.
The output alphabet is therefore closed -- "true"/"false" for starttls, a
decimal literal or "1025" for port -- under every input, including shapes
nobody has tried yet. `trim` and `lower` only widen which inputs map to an
intended value; they cannot widen that output.

Why the fallback direction matters: Keycloak parses starttls with Java's
Boolean.parseBoolean, which returns false for anything not literally "true"
and does no trimming. Forwarding a meaningless value therefore DISABLES
STARTTLS silently and puts the SMTP password on the wire in cleartext. Only
true/false -- any case, surrounding whitespace tolerated -- count as the
operator asking; every other shape means "did not ask", and gets the default.

The closed alphabet also settles a second exposure FOR THESE TWO FIELDS: the
text is rendered inside a double-quoted shell word in the Keycloak postStart
hook, so before the whitelist a value containing a double quote was command
injection, and one containing a newline broke the rendered manifest outright.

Scope that claim to port and starttls only -- it says nothing about the hook.
The same `$KC update` shell word also interpolates .Values.keycloak.realm and
.Values.email.smtp.from raw, and those are still injectable today (verified:
`--set-string 'email.smtp.from=a"; id; echo "'` renders a closing quote and a
second command). Hardening them is a separate pre-existing item; these two
helpers are the template for how, not evidence that it has been done.

Two deliberate boundaries:
  - `--set-string email.smtp.useTls=null` now yields the default instead of
    the literal it used to forward. Helm's --set-string contract is untouched
    (the value in .Values is still that 4-character string); the guard simply
    does not recognise it as a boolean -- and forwarding it meant
    parseBoolean == false, i.e. STARTTLS off without being asked.
  - The port guard validates FORM, not policy. Any decimal literal passes,
    including 0 (a real, explicitly-set value it must not swallow) and values
    above 65535. Range is Keycloak's to reject, loudly, with the operator's
    own number in the error; quietly substituting the dev mail-catcher port
    would be the very bug this task exists to fix.
*/}}
{{- define "srw.keycloak.smtpStartTls" -}}
{{- $canonical := lower (trim (toString .Values.email.smtp.useTls)) -}}
{{- if has $canonical (list "true" "false") -}}{{ $canonical }}{{- else -}}true{{- end -}}
{{- end -}}

{{- define "srw.keycloak.smtpPort" -}}
{{- $canonical := trim (toString .Values.email.smtp.port) -}}
{{- if regexMatch "^[0-9]+$" $canonical -}}{{ $canonical }}{{- else -}}1025{{- end -}}
{{- end -}}

{{/*
Resolved instance count for one database. Explicit `instances` wins; otherwise
the profile decides. Everything downstream (anti-affinity, PDBs) reads THIS,
never the profile name — a generated values file may set instances directly.
Usage: {{ include "srw.dbInstances" (dict "context" . "db" .Values.databases.postgres) }}
*/}}
{{- define "srw.dbInstances" -}}
{{- if .db.instances -}}
{{- .db.instances -}}
{{- else if eq .context.Values.databases.profile "ha" -}}
2
{{- else -}}
1
{{- end -}}
{{- end }}

{{/*
Which implementation backs a database. Rejects typos loudly rather than
silently falling back to the StatefulSet, which would strand a migrated
Cluster's data behind an empty one.
Usage: {{ include "srw.dbEngine" (dict "context" . "db" .Values.databases.postgres) }}
*/}}
{{- define "srw.dbEngine" -}}
{{- $engine := .db.engine | default "statefulset" -}}
{{- if not (has $engine (list "statefulset" "migrating" "cnpg")) -}}
{{- fail (printf "databases.<name>.engine must be statefulset, migrating or cnpg, got %q" $engine) -}}
{{- end -}}
{{- $engine -}}
{{- end }}

{{/*
Whether a bundled database is deployed by this chart at all, independent of
which engine backs it. Gitea's and Keycloak's databases carry extra
prerequisites (their own service must be bundled too), and those conditions
are needed by four templates -- the StatefulSet, the Cluster, the credential
Secret and the PDB. Duplicating them is how they drift.
Usage: {{ include "srw.dbPrereqs" (dict "context" . "key" "postgres") }}
*/}}
{{- define "srw.dbPrereqs" -}}
{{- $ctx := .context -}}
{{- $db := index $ctx.Values.databases .key -}}
{{- $ok := and $db.enabled $db.internal -}}
{{- if eq .key "gitea" -}}
{{- $ok = and $ok $ctx.Values.gitea.enabled $ctx.Values.gitea.internal (include "srw.giteaUsesPostgres" $ctx) -}}
{{- else if eq .key "keycloak" -}}
{{- $ok = and $ok $ctx.Values.keycloak.enabled $ctx.Values.keycloak.internal -}}
{{- end -}}
{{- if $ok -}}true{{- end -}}
{{- end }}

{{/*
Whether a database renders its CloudNativePG Cluster (engine cnpg or, during
the transition, migrating). Empty string is falsey, so use it in an `if`.
Usage: {{ if (include "srw.dbRendersCnpg" (dict "context" . "key" "postgres")) }}
*/}}
{{- define "srw.dbRendersCnpg" -}}
{{- if include "srw.dbPrereqs" . -}}
{{- $engine := include "srw.dbEngine" (dict "context" .context "db" (index .context.Values.databases .key)) -}}
{{- if has $engine (list "migrating" "cnpg") -}}true{{- end -}}
{{- end -}}
{{- end }}

{{/*
Whether a database renders its bundled StatefulSet. `migrating` renders BOTH,
so Phase 4 can import from the live Service before the StatefulSet goes away.
Usage: {{ if (include "srw.dbRendersStatefulset" (dict "context" . "key" "postgres")) }}
*/}}
{{- define "srw.dbRendersStatefulset" -}}
{{- if include "srw.dbPrereqs" . -}}
{{- $engine := include "srw.dbEngine" (dict "context" .context "db" (index .context.Values.databases .key)) -}}
{{- if has $engine (list "statefulset" "migrating") -}}true{{- end -}}
{{- end -}}
{{- end }}

{{/*
Whether a database's backups are on. Three things must hold: the release backs
up at all, the database renders a CNPG Cluster (a StatefulSet has no plugin to
hook), and the database has not opted out.
Usage: {{ if (include "srw.dbBacksUp" (dict "context" . "key" "postgres")) }}
*/}}
{{/*
True when backups go to an object store (Barman plugin + WAL archiving), as
opposed to CSI volume snapshots. The two are mutually exclusive here on purpose:
running both would archive WAL for a base backup the archive cannot be replayed
onto without also configuring the object store.
*/}}
{{- define "srw.backupIsObjectStore" -}}
{{- if eq .Values.databases.backup.method "objectstore" -}}true{{- end -}}
{{- end }}

{{- define "srw.dbBacksUp" -}}
{{- if has .context.Values.databases.backup.method (list "objectstore" "volumeSnapshot") -}}
{{- if include "srw.dbRendersCnpg" . -}}
{{- $db := index .context.Values.databases .key -}}
{{- if ne $db.backupEnabled false -}}true{{- end -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
Secret holding the S3 credentials for backups. Defaults to the chart's own, so
the keys ride the existing Vault bundle instead of a second ExternalSecret.
*/}}
{{- define "srw.backupSecretName" -}}
{{- .Values.databases.backup.credentialsSecret | default (include "srw.secretName" .) -}}
{{- end }}

{{/*
Whether ANY database backs up -- i.e. whether the ObjectStore is referenced by
something. One that nothing references is litter, and would misreport the
release as backed up.
*/}}
{{- define "srw.anyDbBacksUp" -}}
{{- $any := "" -}}
{{- range $key := list "postgres" "vector" "audit" "gitea" "keycloak" -}}
{{- if include "srw.dbBacksUp" (dict "context" $ "key" $key) -}}
{{- $any = "true" -}}
{{- end -}}
{{- end -}}
{{- $any -}}
{{- end }}

{{/*
Whether ANY database renders a CNPG Cluster, for warnings that only apply once
the data tier has actually moved.
*/}}
{{- define "srw.anyDbRendersCnpg" -}}
{{- $any := "" -}}
{{- range $key := list "postgres" "vector" "audit" "gitea" "keycloak" -}}
{{- if include "srw.dbRendersCnpg" (dict "context" $ "key" $key) -}}
{{- $any = "true" -}}
{{- end -}}
{{- end -}}
{{- $any -}}
{{- end }}

{{/*
Backup warnings for NOTES.txt. A named template because `helm template` cannot
render NOTES.txt at all and `helm install --dry-run` needs a live cluster with
every CRD present -- so this is the only form the warnings can be tested in.
Both warnings exist because the failure they describe is otherwise silent.
*/}}
{{- define "srw.backupNotes" -}}
{{- if (include "srw.anyDbRendersCnpg" .) }}
{{- if ne .Values.databases.backup.method "objectstore" }}

  ⚠  DATABASES ARE RUNNING WITH NO BACKUPS

     One or more databases run on CloudNativePG and databases.backup.method is
     "none". There is no WAL archive and no base backup, so there is no
     point-in-time recovery and no recovery at all from a lost volume.

     Set databases.backup.method=objectstore and databases.backup.destinationPath,
     and install the Barman Cloud plugin — this chart cannot install it, because
     it is a raw manifest pinned to the operator's namespace. See
     knowledge-base/knowledge/operations/cnpg_backup_runbook.md
{{- else if and (include "srw.garageEnabled" .) (contains (printf "%s-garage" (include "srw.fullname" .)) .Values.databases.backup.endpointURL) }}

  ⚠  BACKUPS POINT AT THIS RELEASE'S OWN OBJECT STORE

     databases.backup.endpointURL resolves to the Garage service in this same
     release. Garage here is a single node on a single PVC in the same cluster
     as the databases it would be backing up, so a node loss, a storage-class
     failure or a mistaken namespace delete takes the databases AND their
     backups together.

     This is worse than having no backups, because it looks like having them.
     Point it at storage in a different failure domain.
{{- end }}
{{- end }}
{{- end }}

{{/*
Convert a Kubernetes memory quantity to whole MiB.

Helm has no unit-aware arithmetic, so this parses the suffix by hand. Only the
suffixes Kubernetes actually accepts for memory are handled; anything else is a
bare byte count. Returns an integer, so callers can `mul`/`div` it directly.

PostgreSQL's "MB" is 1024*1024 bytes despite the name, so a MiB figure can be
handed to a GUC as "<n>MB" with no conversion.
*/}}
{{- define "srw.quantityToMi" -}}
{{- $q := . | toString -}}
{{- if hasSuffix "Gi" $q -}}
{{- mulf (trimSuffix "Gi" $q | float64) 1024 | int64 -}}
{{- else if hasSuffix "Mi" $q -}}
{{- trimSuffix "Mi" $q | float64 | int64 -}}
{{- else if hasSuffix "Ki" $q -}}
{{- divf (trimSuffix "Ki" $q | float64) 1024 | int64 -}}
{{- else if hasSuffix "G" $q -}}
{{- divf (mulf (trimSuffix "G" $q | float64) 1e9) 1048576 | int64 -}}
{{- else if hasSuffix "M" $q -}}
{{- divf (mulf (trimSuffix "M" $q | float64) 1e6) 1048576 | int64 -}}
{{- else if hasSuffix "k" $q -}}
{{- divf (mulf (trimSuffix "k" $q | float64) 1e3) 1048576 | int64 -}}
{{- else -}}
{{- divf ($q | float64) 1048576 | int64 -}}
{{- end -}}
{{- end }}

{{/*
The postgresql.parameters map for one CNPG Cluster.

Exists as a helper, rather than inline in cnpg-cluster.yaml, because it MERGES
three sources into one map: the connection budget, the memory GUCs derived from
resources.limits.memory, and the operator's explicit per-database overrides.
Rendering them as three separate blocks would emit duplicate YAML keys whenever
an operator set one of the derived GUCs by hand.

Precedence, lowest to highest: derived, then `parameters`. An explicit value
always wins -- deriving is a floor for people who never think about it, not a
ceiling on people who do.

Why derive at all: CNPG ships a fixed shared_buffers of 128MB and does NOT
scale it with the memory limit. Raising limits.memory alone therefore buys the
database nothing, which is exactly the drift this fleet shipped with -- a 19 GB
pgvector on a 12Gi limit was still running 128MB of buffers at an 84% cache hit
ratio. Tying the two together makes that state unreachable.

Takes: dict "context" $ $ "db" $db
*/}}
{{- define "srw.dbParameters" -}}
{{- $db := .db -}}
{{- $tuning := .context.Values.databases.tuning -}}
{{- $params := dict "max_connections" ($db.maxConnections | default 100 | toString) -}}
{{- $res := $db.cnpgResources | default $db.resources -}}
{{- $lim := "" -}}
{{- if $res -}}
{{- if $res.limits -}}
{{- $lim = $res.limits.memory | default "" -}}
{{- end -}}
{{- end -}}
{{- if $lim -}}
{{- $limMi := include "srw.quantityToMi" $lim | int64 -}}
{{- if gt (int $tuning.sharedBuffersPercent) 0 -}}
{{- $_ := set $params "shared_buffers" (printf "%dMB" (div (mul $limMi (int $tuning.sharedBuffersPercent)) 100)) -}}
{{- end -}}
{{- if gt (int $tuning.effectiveCacheSizePercent) 0 -}}
{{- $_ := set $params "effective_cache_size" (printf "%dMB" (div (mul $limMi (int $tuning.effectiveCacheSizePercent)) 100)) -}}
{{- end -}}
{{- end -}}
{{- range $key, $value := $db.parameters -}}
{{- $_ := set $params $key ($value | toString) -}}
{{- end -}}
{{- if $lim -}}
{{- include "srw.assertMemoryFits" (dict "params" $params "limitMi" (include "srw.quantityToMi" $lim | int64) "name" .name) -}}
{{- end -}}
{{- toYaml $params -}}
{{- end }}

{{/*
Convert a PostgreSQL memory GUC value to whole MiB.

PostgreSQL's own units, which are NOT Kubernetes' units: kB/MB/GB/TB, all
binary, case-insensitive. A unit-less value is rejected rather than guessed --
PostgreSQL reads it as the GUC's own base unit, which is 8kB blocks for
shared_buffers but kB for maintenance_work_mem, so a silent guess here would be
wrong for one of them and there is no way to tell which from the value alone.
*/}}
{{- define "srw.pgMemToMi" -}}
{{- $q := . | toString | lower | replace " " "" -}}
{{- if hasSuffix "tb" $q -}}
{{- mulf (trimSuffix "tb" $q | float64) 1048576 | int64 -}}
{{- else if hasSuffix "gb" $q -}}
{{- mulf (trimSuffix "gb" $q | float64) 1024 | int64 -}}
{{- else if hasSuffix "mb" $q -}}
{{- trimSuffix "mb" $q | float64 | int64 -}}
{{- else if hasSuffix "kb" $q -}}
{{- divf (trimSuffix "kb" $q | float64) 1024 | int64 -}}
{{- else -}}
{{- fail (printf "PostgreSQL memory setting %q has no unit. Give it an explicit kB/MB/GB suffix: a bare number means 8kB blocks for shared_buffers but kB for maintenance_work_mem, and this chart will not guess which you meant." $q) -}}
{{- end -}}
{{- end }}

{{/*
Refuse a memory configuration that cannot fit in its own limit.

The failure this prevents is not a slow query, it is an OOM kill. shared_buffers
is allocated for the life of the postmaster; maintenance_work_mem is allocated
again by each CREATE INDEX/REINDEX/manual VACUUM; and autovacuum_work_mem --
which DEFAULTS TO maintenance_work_mem when unset -- is allocated by each of
autovacuum_max_workers workers simultaneously. The last of those is the one that
surprises people: setting maintenance_work_mem to 2GB on a cluster with the
default three autovacuum workers quietly authorises 6GB of autovacuum on top of
shared_buffers, with nothing in the config that says 6GB anywhere.

Deriving shared_buffers from the limit makes this reachable by editing only the
LIMIT, so the invariant has to be enforced rather than documented.

Only the certain-failure case fails the render: if the floor alone meets or
exceeds the limit, no query has run yet and the budget is already gone.

Takes: dict "params" $params "limitMi" $limitMi "name" <cluster name>
*/}}
{{- define "srw.assertMemoryFits" -}}
{{- $p := .params -}}
{{- $limitMi := .limitMi | int64 -}}
{{- $sb := 0 -}}
{{- if hasKey $p "shared_buffers" -}}
{{- $sb = include "srw.pgMemToMi" (get $p "shared_buffers") | int64 -}}
{{- end -}}
{{- $mwm := 0 -}}
{{- if hasKey $p "maintenance_work_mem" -}}
{{- $mwm = include "srw.pgMemToMi" (get $p "maintenance_work_mem") | int64 -}}
{{- end -}}
{{- $workers := 3 -}}
{{- if hasKey $p "autovacuum_max_workers" -}}
{{- $workers = get $p "autovacuum_max_workers" | int -}}
{{- end -}}
{{- $avwm := $mwm -}}
{{- if hasKey $p "autovacuum_work_mem" -}}
{{- $avwm = include "srw.pgMemToMi" (get $p "autovacuum_work_mem") | int64 -}}
{{- end -}}
{{- $floor := add $sb $mwm (mul $avwm $workers) -}}
{{- if ge (int $floor) (int $limitMi) -}}
{{- fail (printf "database %s: shared_buffers (%dMi) + maintenance_work_mem (%dMi) + autovacuum_work_mem (%dMi x %d workers) = %dMi, which is at or above the memory limit of %dMi. That is an OOM kill during the first index build or autovacuum, not a slow one. Raise cnpgResources.limits.memory, lower databases.tuning.sharedBuffersPercent, or set autovacuum_work_mem explicitly -- it defaults to maintenance_work_mem, once per worker." .name $sb $mwm $avwm $workers $floor $limitMi) -}}
{{- end -}}
{{- end }}

{{/*
=============================================================================
SSH gateway (templates/ssh-gateway/*, plus the orchestrator's host-key
publication mount).
=============================================================================
*/}}

{{- define "srw.sshGatewayName" -}}
{{- printf "%s-ssh-gateway" (include "srw.fullname" .) -}}
{{- end }}

{{/*
Where the gateway's host-key Secret is projected. The SAME path in both pods:
the gateway mounts the private halves here, the orchestrator mounts only the
`.pub` halves here, and both environment variables below are built from this
one string so a moved mount cannot leave a stale path behind.
*/}}
{{- define "srw.sshGatewayHostKeyDir" -}}/run/secrets/ssh-gateway/host{{- end }}
{{- define "srw.sshGatewayCaDir" -}}/run/secrets/ssh-gateway/ca{{- end }}

{{/*
`SSH_GATEWAY_HOST_KEYS` — the PRIVATE key paths the gateway process loads.
*/}}
{{- define "srw.sshGatewayHostKeyPaths" -}}
{{- $dir := include "srw.sshGatewayHostKeyDir" . -}}
{{- $paths := list -}}
{{- range .Values.sshGateway.hostKeyNames -}}
{{- $paths = append $paths (printf "%s/%s" $dir .) -}}
{{- end -}}
{{- join "," $paths -}}
{{- end }}

{{/*
`SSH_GATEWAY_PUBLIC_HOST_KEYS` — the PUBLIC key paths the orchestrator's
`GET /api/ssh/host-keys` reads and publishes.

Rendered from the same `hostKeyNames` list as the private paths above, on
purpose. These are two variables in two different Deployments read by two
different processes, with no runtime cross-check anywhere: when they drift,
an SSH client sees a host-key mismatch that is indistinguishable from an
active MITM. One list is the only thing that makes drift unrepresentable.

Pointing this at the PRIVATE files would also "work" (asyncssh's
import_public_key emits only public material) and is deliberately not done:
it would put the gateway's host private keys in a second pod for no benefit.
*/}}
{{- define "srw.sshGatewayPublicHostKeyPaths" -}}
{{- $dir := include "srw.sshGatewayHostKeyDir" . -}}
{{- $paths := list -}}
{{- range .Values.sshGateway.hostKeyNames -}}
{{- $paths = append $paths (printf "%s/%s.pub" $dir .) -}}
{{- end -}}
{{- join "," $paths -}}
{{- end }}

{{/*
Every precondition `services/ssh_gateway_config.load_config` fails closed on,
checked at render time instead. Included from the gateway Deployment (which
renders whenever the component is enabled), so one `fail` aborts the whole
release rather than shipping a pod that cannot boot.

Emits nothing.
*/}}
{{- define "srw.sshGatewayValidate" -}}
{{- $gw := .Values.sshGateway -}}
{{- if empty $gw.allowedOrigins -}}
{{- fail "sshGateway.enabled requires a non-empty sshGateway.allowedOrigins; an empty list would accept cross-site WebSocket handshakes, and load_config refuses to boot without it" -}}
{{- end -}}
{{- if eq (trim $gw.hostKeySecret) "" -}}
{{- fail "sshGateway.enabled requires sshGateway.hostKeySecret. The chart never generates host keys: a generated key would rotate on upgrade and break every user's known_hosts." -}}
{{- end -}}
{{- if empty $gw.hostKeyNames -}}
{{- fail "sshGateway.enabled requires a non-empty sshGateway.hostKeyNames; with no names neither the gateway's SSH_GATEWAY_HOST_KEYS nor the orchestrator's SSH_GATEWAY_PUBLIC_HOST_KEYS has anything to point at" -}}
{{- end -}}
{{- range $gw.hostKeyNames -}}
{{- if regexMatch "(?i)(rsa|ecdsa|dss|dsa)" . -}}
{{- fail (printf "sshGateway.hostKeyNames entry %q is not an Ed25519 host key. _require_ed25519_host_key (services/ssh_gateway_config.py) raises on any algorithm that is not ssh-ed25519, so the gateway would refuse to start -- a crash-loop three files away from this value. Use ssh_host_ed25519_key. (Naming-convention tripwire only; the load-time check is the real enforcement.)" .) -}}
{{- end -}}
{{- end -}}
{{- if eq (trim $gw.userCaSecret) "" -}}
{{- fail "sshGateway.enabled requires sshGateway.userCaSecret (the user CA the gateway signs inner-hop certificates with)" -}}
{{- end -}}
{{- if and (empty .Values.sessionRouter.jwtSecret) (empty .Values.sessionRouter.jwtSecretName) -}}
{{- fail "sshGateway.enabled requires a configured sessionRouter JWT secret (sessionRouter.jwtSecret for the chart-rendered Secret, or sessionRouter.jwtSecretName for one you own). SESSION_JWT_SECRET is the HMAC key the gateway verifies the attach token the orchestrator mints with; load_config refuses to boot without it. With neither value set no Secret is rendered at all, the secretKeyRef is `optional: true`, and the gateway crash-loops with the reason three files away from the values file." -}}
{{- end -}}
{{- if eq (trim $gw.trustedProxies) "" -}}
{{- fail "sshGateway.enabled requires sshGateway.trustedProxies: the source addresses whose X-Forwarded-For header the gateway may believe (the ingress hop, as an IP/CIDR list), or the literal string \"none\" when nothing proxies it. Left unset behind an ingress every WSS client is rate limited as one source and the seventeenth concurrent user is refused." -}}
{{- end -}}
{{- if or (lt (int $gw.tcp.port) 1024) (gt (int $gw.tcp.port) 65535) -}}
{{- fail (printf "sshGateway.tcp.port must be between 1024 and 65535 (got %v). The gateway runs as uid 999 with every capability dropped, so it cannot bind a privileged port: the accept loop would never come up, /healthz would answer 503 forever, and the pod would never go Ready." $gw.tcp.port) -}}
{{- end -}}
{{- if and $gw.tcp.enabled (empty $gw.tcp.allowedClientCIDRs) -}}
{{- fail "sshGateway.tcp.enabled requires sshGateway.tcp.allowedClientCIDRs; an unscoped SSH LoadBalancer is not a supported default" -}}
{{- end -}}
{{- if and $gw.networkPolicy.enabled (or (empty $gw.networkPolicy.edgeNamespaceSelector) (empty $gw.networkPolicy.edgePodSelector)) -}}
{{- fail "sshGateway.networkPolicy.enabled requires non-empty edgeNamespaceSelector and edgePodSelector; an empty selector matches everything, which is not a policy" -}}
{{- end -}}
{{- end }}
