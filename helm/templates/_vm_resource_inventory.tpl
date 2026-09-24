{{/* One canonical JSON policy for both processes and their rollout checksums. */}}
{{- define "srw.vmResourceInventoryConfig" -}}
{{- dict "mode" (include "srw.vmMode" .) "namespace" (include "srw.vmControllerNamespace" .) "policy" .Values.vm.resourceAdmission | toJson -}}
{{- end -}}

{{- define "srw.vmResourceInventoryValidate" -}}
{{- $policy := .Values.vm.resourceAdmission | default dict -}}
{{- if or $policy.shadowEnabled $policy.enforcementEnabled -}}
{{- if not (and $policy.observerEnabled $policy.shadowEnabled $policy.enforcementEnabled $policy.clusterWidePodReadAcknowledged) -}}
{{- fail "vm.resourceAdmission requires all four capabilities together; shadow-only and enforcement-only configurations are unsupported" -}}
{{- end -}}
{{- if not .Values.orchestrator.vmProvisioning.creationRetryEnabled -}}
{{- fail "vm.resourceAdmission enforcement requires orchestrator.vmProvisioning.creationRetryEnabled" -}}
{{- end -}}
{{- if or (empty $policy.launcherProfile) (empty $policy.inventory.kubevirtNamespace) (empty $policy.inventory.kubevirtName) -}}
{{- fail "vm.resourceAdmission enforcement requires an explicit whole-launcher profile and exact KubeVirt installation" -}}
{{- end -}}
{{- if or
  (ne (index $policy.launcherProfile "version") (float64 1))
  (ne (index $policy.launcherProfile "architecture") "amd64")
  (ne (index $policy.launcherProfile "kubevirtVersion") "v1.6.6")
  (ne (index $policy.launcherProfile "costAlgorithm") "kubevirt-v1.6.6-amd64-ordinary-pvc-v1")
-}}
{{- fail "vm.resourceAdmission enforcement requires the supported whole-launcher profile" -}}
{{- end -}}
{{- $sections := dict
  "hostCost" (list "version" "cpuMillicoresPerVcpuNumerator"
    "cpuMillicoresPerVcpuDenominator" "launcherCpuOverheadMillicores"
    "fixedMemoryOverheadBytes" "perVcpuMemoryOverheadBytes"
    "memoryOverheadBasisPoints" "ephemeralStorageReserveBytes"
    "kvmDevices" "tunDevices" "vhostNetDevices")
  "nodeHeadroom" (list "cpuMillicores" "memoryBytes" "ephemeralStorageBytes"
    "kvmDevices" "tunDevices" "vhostNetDevices")
  "installationBudget" (list "cpuMillicores" "memoryBytes"
    "ephemeralStorageBytes" "kvmDevices" "tunDevices" "vhostNetDevices")
  "ownerBudget" (list "cpuMillicores" "memoryBytes" "ephemeralStorageBytes"
    "kvmDevices" "tunDevices" "vhostNetDevices")
  "fairness" (list "maxBypasses" "priorityAgingSeconds")
-}}
{{- range $section, $fields := $sections -}}
{{- $values := index $policy $section -}}
{{- if not (kindIs "map" $values) -}}
{{- fail (printf "vm.resourceAdmission.%s must be explicitly configured" $section) -}}
{{- end -}}
{{- range $field := $fields -}}
{{- if or (not (hasKey $values $field)) (eq (index $values $field) nil) -}}
{{- fail (printf "vm.resourceAdmission.%s.%s must be explicitly configured" $section $field) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- if or
  (ne (index $policy.hostCost "version") (float64 2))
  (lt (int (index $policy.hostCost "cpuMillicoresPerVcpuNumerator")) 1)
  (lt (int (index $policy.hostCost "cpuMillicoresPerVcpuDenominator")) 1)
  (lt (int (index $policy.hostCost "ephemeralStorageReserveBytes")) 1)
  (lt (int (index $policy.hostCost "kvmDevices")) 1)
  (lt (int (index $policy.hostCost "tunDevices")) 1)
  (lt (int (index $policy.hostCost "vhostNetDevices")) 1)
  (lt (int (index $policy.fairness "priorityAgingSeconds")) 1)
-}}
{{- fail "vm.resourceAdmission enforcement requires valid whole-launcher host cost and fairness" -}}
{{- end -}}
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
{{- if $policy.launcherProfile -}}
{{- if or (not (has "kubernetes.io/arch" $policy.inventory.nodeLabelKeys)) (empty $policy.inventory.kubevirtNamespace) (empty $policy.inventory.kubevirtName) -}}
{{- fail "vm.resourceAdmission launcher profile requires kubernetes.io/arch and exact KubeVirt namespace/name" -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
