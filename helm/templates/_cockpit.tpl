{{/* The mounted runtime environment also controls the Cockpit rollout. */}}
{{- define "srw.cockpitEnvironment" -}}
(function(window) {
  window['env'] = window['env'] || {};
  window['env']['apiUrl'] = '{{ include "srw.cockpitFacingApiUrl" . }}/api';
  window['env']['serviceWorkerEnabled'] = {{ if include "srw.singleOrigin" . }}false{{ else }}true{{ end }};
  window['env']['externalClientsEnabled'] = {{ if include "srw.singleOrigin" . }}false{{ else }}true{{ end }};
  window['env']['adminToolsEnabled'] = {{ if include "srw.singleOrigin" . }}false{{ else }}true{{ end }};
  // Null keeps the live-app renderer dark. The suffix is exposed only
  // after the separate viewer gate is explicitly configured.
  window['env']['canvasViewerHostSuffix'] = {{ if and .Values.canvas.livePreview.enabled .Values.canvas.livePreview.viewer.enabled .Values.canvas.livePreview.viewer.hostSuffix }}{{ .Values.canvas.livePreview.viewer.hostSuffix | quote }}{{ else }}null{{ end }};
  // Exact foreign iframe origin; null keeps Office Canvas dark-shipped.
  window['env']['canvasOfficeOrigin'] = {{ if and .Values.collabora.enabled .Values.collabora.publicUrl }}{{ .Values.collabora.publicUrl | quote }}{{ else }}null{{ end }};
  window['env']['giteaUrl'] = '{{ include "srw.gitUrl" . }}/srw';
  window['env']['dozzleUrl'] = '{{ include "srw.urlScheme" . }}://{{ include "srw.dozzleHost" . }}';
  window['env']['minioConsoleUrl'] = '{{ include "srw.urlScheme" . }}://{{ include "srw.minioHost" . }}';
  window['env']['neo4jUrl'] = '{{ include "srw.urlScheme" . }}://{{ include "srw.neo4jBrowserHost" . }}';
  window['env']['pgadminUrl'] = '{{ include "srw.urlScheme" . }}://{{ include "srw.pgadminHost" . }}';
  window['env']['mcpUrl'] = '{{ include "srw.mcpUrl" . }}/mcp';
  window['env']['cloudUrl'] = '{{ include "srw.cloudUrl" . }}';

  // OIDC SSO is brokered by the orchestrator cookie BFF — the cockpit no
  // longer talks to Keycloak directly. The /auth/login redirect on the
  // API origin handles the PKCE handshake server-side. Keycloak URLs are
  // therefore no longer surfaced to the SPA.

  // Model catalog is sourced from GET /api/models (system_api_keys +
  // llm_endpoints). Do not seed window.env.models here — an empty
  // catalog triggers the role-aware "configure a provider" banner.
})(this);
{{ end -}}
