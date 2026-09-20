{{/* One canonical JSON policy for both processes and their rollout checksums. */}}
{{- define "srw.vmResourceInventoryConfig" -}}
{{- dict "mode" (include "srw.vmMode" .) "namespace" (include "srw.vmControllerNamespace" .) "policy" .Values.vm.resourceAdmission | toJson -}}
{{- end -}}

{{- define "srw.vmResourceInventoryValidate" -}}
{{- $policy := .Values.vm.resourceAdmission -}}
{{- if or $policy.shadowEnabled $policy.enforcementEnabled -}}
{{- fail "vm.resourceAdmission: shadow and enforcement are not implemented; keep both disabled" -}}
{{- end -}}
{{- if $policy.observerEnabled -}}
{{- if or (ne (include "srw.vmMode" .) "same-cluster") (not $policy.clusterWidePodReadAcknowledged) (empty (include "srw.vmLifecycleAuthSecretName" .)) (empty $policy.stableClusterId) -}}
{{- fail "vm.resourceAdmission.observerEnabled requires same-cluster mode, lifecycle HMAC, stableClusterId and clusterWidePodReadAcknowledged" -}}
{{- end -}}
{{- range $key := list "publishIntervalSeconds" "staleAfterSeconds" "maxItems" "maxBytes" "requestTimeoutSeconds" "collectionTimeoutSeconds" "publicationTimeoutSeconds" "historyLimit" -}}
{{- if empty (index $policy.inventory $key) -}}
{{- fail (printf "vm.resourceAdmission.inventory.%s must be explicitly configured" $key) -}}
{{- end -}}
{{- end -}}
{{- if not (has "kubernetes.io/hostname" $policy.inventory.nodeLabelKeys) -}}
{{- fail "vm.resourceAdmission.inventory.nodeLabelKeys must explicitly include kubernetes.io/hostname and every placement/topology key" -}}
{{- end -}}
{{- end -}}
{{- end -}}
