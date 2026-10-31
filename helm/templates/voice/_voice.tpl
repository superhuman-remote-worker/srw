{{/*
SRW's local voice path (local_voice_gateway.md §4.4): the voice gateway
(voiceGateway.*) and the Asterisk addon (asterisk.*). Names, the two
rendered configs and the checks both components share.
*/}}

{{- define "srw.voiceGatewayName" -}}
{{- printf "%s-voice-gateway" (include "srw.fullname" .) -}}
{{- end }}

{{- define "srw.voiceGatewayMediaName" -}}
{{- printf "%s-voice-gateway-media" (include "srw.fullname" .) -}}
{{- end }}

{{- define "srw.asteriskName" -}}
{{- printf "%s-asterisk" (include "srw.fullname" .) -}}
{{- end }}

{{- define "srw.asteriskAriName" -}}
{{- printf "%s-asterisk-ari" (include "srw.fullname" .) -}}
{{- end }}

{{/* The Secret with `api-key` and `asterisk-password`. */}}
{{- define "srw.voiceSecretName" -}}
{{- .Values.voiceGateway.existingSecret | default (printf "%s-voice-gateway" (include "srw.fullname" .)) -}}
{{- end }}

{{/* The live_voice endpoint's base URL: the gateway's API Service. */}}
{{- define "srw.voiceGatewayBaseUrl" -}}
{{- printf "http://%s.%s.svc:18440/v1" (include "srw.voiceGatewayName" .) .Release.Namespace -}}
{{- end }}

{{/* "true" when llm.seed's Job seeds the gateway's live_voice rows. */}}
{{- define "srw.voiceCatalogSeeded" -}}
{{- if and .Values.voiceGateway.enabled .Values.voiceGateway.catalog.seed -}}true{{- end -}}
{{- end }}

{{/* The connector driver a phone line runs, as its pods' srw.io/driver label. */}}
{{- define "srw.voiceDriverLabel" -}}
srw.gpt-live-sip_v1
{{- end }}

{{/*
Checks every voice value the two components share; fails the render with
the fix. Called by each component's template, so either alone validates.
*/}}
{{- define "srw.voiceValidate" -}}
{{- $gateway := .Values.voiceGateway -}}
{{- $asterisk := .Values.asterisk -}}
{{- if and $asterisk.enabled (not $gateway.enabled) -}}
{{- fail "asterisk.enabled needs voiceGateway.enabled: the addon hands every call to the gateway" -}}
{{- end -}}
{{- if $gateway.enabled -}}
{{- if not (or $asterisk.enabled $gateway.ari.url) -}}
{{- fail "voiceGateway.enabled needs asterisk.enabled (the chart's addon) or voiceGateway.ari.url (an Asterisk of your own)" -}}
{{- end -}}
{{- if not $gateway.lines -}}
{{- fail "voiceGateway.lines is empty: a gateway serves at least one phone line" -}}
{{- end -}}
{{- if not $gateway.profiles -}}
{{- fail "voiceGateway.profiles is empty: a line's model is one of the gateway's profiles" -}}
{{- end -}}
{{- $names := dict -}}
{{- range $i, $line := $gateway.lines -}}
{{- $label := printf "voiceGateway.lines[%d]" $i -}}
{{- if not (regexMatch "^[a-z0-9][a-z0-9-]{0,30}$" (toString $line.name)) -}}
{{- fail (printf "%s.name must be 1 to 31 lowercase letters, digits or dashes" $label) -}}
{{- end -}}
{{- if hasKey $names $line.name -}}
{{- fail (printf "voiceGateway.lines: the name %s is used twice" $line.name) -}}
{{- end -}}
{{- $_ := set $names $line.name true -}}
{{- $mode := $line.mode | default "register" -}}
{{- if not (has $mode (list "register" "peer")) -}}
{{- fail (printf "%s.mode is register or peer" $label) -}}
{{- end -}}
{{- if not $line.pbx -}}
{{- fail (printf "%s.pbx is the phone system's host[:port]" $label) -}}
{{- end -}}
{{- if not (regexMatch "^[0-9a-fA-F-]{36}$" (toString $line.connectorId)) -}}
{{- fail (printf "%s.connectorId is the id of the SRW phone-line connector this line calls" $label) -}}
{{- end -}}
{{- if not (and $line.webhookSecret $line.webhookSecret.name $line.webhookSecret.key) -}}
{{- fail (printf "%s.webhookSecret {name, key} names the Secret with the connector's webhook secret (whsec_...)" $label) -}}
{{- end -}}
{{- if and (eq $mode "register") (not (and $line.extension $line.passwordSecret)) -}}
{{- fail (printf "%s: a registered line needs extension and passwordSecret {name, key}" $label) -}}
{{- end -}}
{{- with $line.passwordSecret -}}
{{- if not (and .name .key) -}}
{{- fail (printf "%s.passwordSecret needs name and key" $label) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- range $name, $_ := $gateway.catalog.models -}}
{{- if not (hasKey $gateway.profiles $name) -}}
{{- fail (printf "voiceGateway.catalog.models.%s names no profile in voiceGateway.profiles" $name) -}}
{{- end -}}
{{- end -}}
{{- range $i, $mount := $gateway.profileSecrets -}}
{{- if not (and $mount.name $mount.secretName $mount.key (regexMatch "^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$" (toString $mount.name))) -}}
{{- fail (printf "voiceGateway.profileSecrets[%d] needs name (a file name), secretName and key" $i) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if $asterisk.enabled -}}
{{- $network := $asterisk.network -}}
{{- if not (has $network.mode (list "hostNetwork" "loadBalancer")) -}}
{{- fail "asterisk.network.mode is hostNetwork or loadBalancer" -}}
{{- end -}}
{{- if and (eq $network.mode "hostNetwork") (not $asterisk.nodeSelector) -}}
{{- fail "asterisk.network.mode hostNetwork needs asterisk.nodeSelector: the phone system reaches Asterisk at one node's address" -}}
{{- end -}}
{{- if and (eq $network.mode "loadBalancer") (not $network.externalAddress) -}}
{{- fail "asterisk.network.mode loadBalancer needs asterisk.network.externalAddress: the load balancer IP the phone system reaches (and Asterisk advertises)" -}}
{{- end -}}
{{- if lt (sub (int $asterisk.rtp.end) (int $asterisk.rtp.start)) 3 -}}
{{- fail "asterisk.rtp needs room for two ports per call: end at least start + 3" -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
The gateway's config file (python -m voice_gateway --config). Secrets are
files under /run/srw-voice; profiles pass through as they are.
*/}}
{{- define "srw.voiceGatewayConfig" -}}
{{- $gateway := .Values.voiceGateway -}}
listen: 0.0.0.0:18440
api_key_files:
  - /run/srw-voice/api-key
orchestrator_url: {{ printf "http://%s-orchestrator.%s.svc:8085" (include "srw.fullname" .) .Release.Namespace | quote }}
lines:
  {{- range $gateway.lines }}
  - name: {{ .name | quote }}
    connector_id: {{ .connectorId | quote }}
    webhook_secret_file: {{ printf "/run/srw-voice/lines/%s/webhook-secret" .name | quote }}
  {{- end }}
profiles:
  {{- toYaml $gateway.profiles | nindent 2 }}
source:
  kind: asterisk
  ari_url: {{ $gateway.ari.url | default (printf "http://%s.%s.svc:%d" (include "srw.asteriskAriName" .) .Release.Namespace (int .Values.asterisk.ari.port)) | quote }}
  ari_user: {{ .Values.asterisk.ari.user | quote }}
  password_file: /run/srw-voice/asterisk-password
  app: {{ .Values.asterisk.ari.app | quote }}
  media_listen: 0.0.0.0:18441
  media_connection: {{ .Values.asterisk.mediaConnection | quote }}
  media_timeout_s: {{ $gateway.mediaTimeoutSeconds }}
  dial_timeout_s: {{ $gateway.dialTimeoutSeconds }}
{{- end }}

{{/*
The addon's values (python -m voice_gateway.asterisk.render --values),
rendered into /etc/asterisk by an init container. Passwords are files.
*/}}
{{- define "srw.asteriskAddonValues" -}}
{{- $asterisk := .Values.asterisk -}}
{{- $network := $asterisk.network -}}
sip:
  bind: 0.0.0.0
  port: {{ int $asterisk.sip.port }}
  {{- if $network.externalAddress }}
  external_address: {{ $network.externalAddress | quote }}
  {{- $localNets := $network.localNets }}
  {{- if eq $network.mode "loadBalancer" }}
  {{- $localNets = concat .Values.connectors.servicePods.clusterCidrs ($network.localNets | default list) | uniq }}
  {{- end }}
  local_net:
    {{- range $localNets }}
    - {{ . | quote }}
    {{- end }}
  {{- end }}
rtp:
  start: {{ int $asterisk.rtp.start }}
  end: {{ int $asterisk.rtp.end }}
ari:
  bind: {{ printf "127.0.0.1:%d" (int $asterisk.ari.port) | quote }}
  user: {{ $asterisk.ari.user | quote }}
  password_file: /run/srw-voice/asterisk-password
  app: {{ $asterisk.ari.app | quote }}
gateway:
  media_uri: {{ printf "ws://%s.%s.svc:18441/media" (include "srw.voiceGatewayMediaName" .) .Release.Namespace | quote }}
  connection: {{ $asterisk.mediaConnection | quote }}
codecs:
  {{- toYaml $asterisk.codecs | nindent 2 }}
dtmf_mode: {{ $asterisk.dtmfMode | quote }}
lines:
  {{- range .Values.voiceGateway.lines }}
  - name: {{ .name | quote }}
    mode: {{ .mode | default "register" | quote }}
    pbx: {{ .pbx | quote }}
    {{- with .transport }}
    transport: {{ . | quote }}
    {{- end }}
    {{- with .domain }}
    domain: {{ . | quote }}
    {{- end }}
    {{- with .extension }}
    extension: {{ . | toString | quote }}
    {{- end }}
    {{- with .username }}
    username: {{ . | toString | quote }}
    {{- end }}
    {{- if .passwordSecret }}
    password_file: {{ printf "/run/srw-voice/lines/%s/sip-password" .name | quote }}
    {{- end }}
    {{- with .codecs }}
    codecs:
      {{- toYaml . | nindent 6 }}
    {{- end }}
    {{- with .dtmfMode }}
    dtmf_mode: {{ . | quote }}
    {{- end }}
    {{- with .match }}
    match:
      {{- toYaml . | nindent 6 }}
    {{- end }}
  {{- end }}
{{- end }}
