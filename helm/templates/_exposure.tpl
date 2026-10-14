{{/* Opt-in dedicated single-origin HTTPS profile. */}}
{{- define "srw.singleOrigin" -}}
{{- if eq (.Values.exposure.mode | default "multi-host") "single-origin" -}}true{{- end -}}
{{- end }}

{{- define "srw.publicOrigin" -}}
{{- if include "srw.singleOrigin" . -}}
{{- if eq (int .Values.exposure.singleOrigin.publicPort) 443 -}}
{{- printf "https://%s" .Values.exposure.singleOrigin.address -}}
{{- else -}}
{{- printf "https://%s:%d" .Values.exposure.singleOrigin.address (int .Values.exposure.singleOrigin.publicPort) -}}
{{- end -}}
{{- else -}}
{{- include "srw.cockpitUrl" . -}}
{{- end -}}
{{- end }}

{{/*
The controller class is annotation-only, but it is still cluster-visible input
to every Ingress controller. Include namespace and release identity plus a hash
while preserving the semantic suffix inside the 63-byte label-value ceiling.
*/}}
{{- define "srw.singleOriginIngressClass" -}}
{{- $identity := printf "%s/%s" .Release.Namespace .Release.Name -}}
{{- $prefix := printf "%s-%s" .Release.Namespace .Release.Name | trunc 34 | trimSuffix "-" -}}
{{- printf "%s-%s-srw-single-origin" $prefix ($identity | sha256sum | trunc 10) -}}
{{- end }}

{{- define "srw.singleOriginGatewayName" -}}
{{- $identity := printf "%s/%s" .Release.Namespace .Release.Name -}}
{{- printf "%s-%s-single-origin" (include "srw.fullname" . | trunc 38 | trimSuffix "-") ($identity | sha256sum | trunc 10) -}}
{{- end }}

{{- define "srw.singleOriginTlsSecretName" -}}
{{- $identity := printf "%s/%s" .Release.Namespace .Release.Name -}}
{{- printf "%s-%s-single-origin-tls" (include "srw.fullname" . | trunc 34 | trimSuffix "-") ($identity | sha256sum | trunc 10) -}}
{{- end }}

{{/*
The single-origin gateway's self-signed certificate, as JSON {cert, key,
expires}. An upgrade reuses the stored pair while it was issued for the same
address and validity and more than 30 days remain, so browsers and clients
that accepted or pinned it keep working; otherwise a new pair is generated.
Helm cannot read X.509, so the Secret records what was issued in annotations.
A Secret without them (rendered by an older chart) is replaced once. The
caller supplies lookup's result, so the choice is testable offline.
*/}}
{{- define "srw.singleOriginCertificate" -}}
{{- $single := .context.Values.exposure.singleOrigin -}}
{{- $days := int $single.tls.validityDays -}}
{{- $existing := .existing | default dict -}}
{{- $data := get $existing "data" | default dict -}}
{{- $annotations := (get $existing "metadata" | default dict).annotations | default dict -}}
{{- $expires := get $annotations "srw.io/single-origin-expires" | default "" | toString -}}
{{- $reuse := and (get $data "tls.crt") (get $data "tls.key") $expires (eq (get $annotations "srw.io/single-origin-address" | default "" | toString) $single.address) (eq (get $annotations "srw.io/single-origin-validity-days" | default "" | toString) (toString $days)) -}}
{{- if $reuse -}}
  {{- $remaining := sub (unixEpoch (toDate "2006-01-02T15:04:05Z07:00" $expires) | int64) (unixEpoch now | int64) -}}
  {{- if lt $remaining 2592000 -}}
    {{- $reuse = false -}}
  {{- end -}}
{{- end -}}
{{- if $reuse -}}
{{- dict "cert" (get $data "tls.crt" | b64dec) "key" (get $data "tls.key" | b64dec) "expires" $expires | toJson -}}
{{- else -}}
{{- $dnsNames := list -}}
{{- $ipAddresses := list -}}
{{- if eq $single.address "localhost" -}}
  {{- $dnsNames = list $single.address -}}
{{- else -}}
  {{- $ipAddresses = list $single.address -}}
{{- end -}}
{{- $generated := genSelfSignedCert $single.address $ipAddresses $dnsNames $days -}}
{{- $notAfter := dateInZone "2006-01-02T15:04:05Z07:00" (now | dateModify (printf "+%dh" (mul $days 24))) "UTC" -}}
{{- dict "cert" $generated.Cert "key" $generated.Key "expires" $notAfter | toJson -}}
{{- end -}}
{{- end }}

{{- define "srw.singleOriginValidate" -}}
{{- if include "srw.singleOrigin" . -}}
  {{- $single := .Values.exposure.singleOrigin -}}
  {{- $octet := "([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])" -}}
  {{- $ipv4 := printf "^%s\\.%s\\.%s\\.%s$" $octet $octet $octet $octet -}}
  {{- if not (or (eq $single.address "localhost") (regexMatch $ipv4 $single.address)) -}}
    {{- fail "exposure.singleOrigin.address must be localhost or a valid IPv4 address" -}}
  {{- end -}}
  {{- if ne $single.tls.mode "self-signed" -}}
    {{- fail "exposure.singleOrigin.tls.mode must be self-signed" -}}
  {{- end -}}
  {{- if .Values.agent.pinnedLegacyNamespaces -}}
    {{- fail "exposure.mode=single-origin does not support agent.pinnedLegacyNamespaces; migrate pinned sessions into the release namespace first" -}}
  {{- end -}}
  {{- if and .Values.keycloak.enabled (not .Values.keycloak.internal) -}}
    {{- fail "exposure.mode=single-origin requires bundled keycloak.internal=true when Keycloak is enabled" -}}
  {{- end -}}
  {{- if and .Values.gitea.enabled (not .Values.gitea.internal) -}}
    {{- fail "exposure.mode=single-origin requires bundled gitea.internal=true when Gitea is enabled" -}}
  {{- end -}}
  {{- if and .Values.nextcloud.enabled (not .Values.nextcloud.internal) -}}
    {{- fail "exposure.mode=single-origin requires bundled nextcloud.internal=true when Nextcloud is enabled" -}}
  {{- end -}}
  {{- if .Values.opencloud.enabled -}}
    {{- fail "exposure.mode=single-origin requires opencloud.enabled=false; use bundled Nextcloud for the /cloud path in this profile" -}}
  {{- end -}}
  {{- if .Values.cloud.externalBackend -}}
    {{- fail "exposure.mode=single-origin does not support cloud.externalBackend; use bundled Nextcloud for the /cloud path" -}}
  {{- end -}}
  {{- if .Values.sshGateway.enabled -}}
    {{- fail "exposure.mode=single-origin does not support sshGateway.enabled; external SSH client trust and port handling require a separate profile" -}}
  {{- end -}}
  {{- if .Values.canvas.livePreview.viewer.enabled -}}
    {{- fail "exposure.mode=single-origin does not support the hostname-based Canvas viewer ingress" -}}
  {{- end -}}
  {{- if .Values.collabora.enabled -}}
    {{- fail "exposure.mode=single-origin does not support the separate-origin Collabora integration" -}}
  {{- end -}}
  {{- if .Values.databases.neo4j.boltTls.enabled -}}
    {{- fail "exposure.mode=single-origin requires databases.neo4j.boltTls.enabled=false because the preset has no cert-manager dependency" -}}
  {{- end -}}
{{- end -}}
{{- end }}
