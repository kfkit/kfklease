{{- define "kfklease.name" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "kfklease.labels" -}}
app.kubernetes.io/name: kfklease
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "kfklease.selectorLabels" -}}
app.kubernetes.io/name: kfklease
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "kfklease.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end -}}

{{- /*
kfklease.leases: every lease of the release as a YAML list of
{pfx, topic, healthUrl, scaledObject}, pfx being what error messages
start with, the release's own first, then
extraLeases with their scaledObject merged over the top-level one, all
but targetRef.name, which each lease names itself. Checks
what has to hold across the list: topics and ScaledObject names distinct.
*/ -}}
{{- define "kfklease.leases" -}}
{{- $v := .Values -}}
{{- if hasKey $v.scaledObject "target" }}{{ fail "scaledObject.target was renamed to scaledObject.targetRef.name" }}{{ end -}}
{{- if contains "," $v.topic }}{{ fail "topic must not contain a comma; add more leases with extraLeases" }}{{ end -}}
{{- $all := list (dict "pfx" "" "topic" $v.topic "healthUrl" ($v.healthGate.url | default "") "scaledObject" (deepCopy $v.scaledObject)) -}}
{{- range $i, $e := $v.extraLeases -}}
{{- $path := printf "extraLeases[%d]" $i -}}
{{- if not $e.topic }}{{ fail (printf "%s.topic is required" $path) }}{{ end -}}
{{- if contains "," $e.topic }}{{ fail (printf "%s.topic must not contain a comma" $path) }}{{ end -}}
{{- if hasKey ($e.scaledObject | default dict) "target" }}{{ fail (printf "%s: scaledObject.target is scaledObject.targetRef.name" $path) }}{{ end -}}
{{- $own := deepCopy ($e.scaledObject | default dict) -}}
{{- $so := mergeOverwrite (deepCopy $v.scaledObject) $own -}}
{{- /* The target is the lease's own; only its kind and apiVersion default. */ -}}
{{- $_ := set $so.targetRef "name" (dig "targetRef" "name" "" $own) -}}
{{- $url := "" }}{{ with $e.healthGate }}{{ $url = .url | default "" }}{{ end -}}
{{- $all = append $all (dict "pfx" (printf "%s: " $path) "topic" $e.topic "healthUrl" $url "scaledObject" $so) -}}
{{- end -}}
{{- $topics := dict -}}
{{- $names := dict -}}
{{- range $all -}}
{{- if hasKey $topics .topic }}{{ fail (printf "%stopic %q is already a lease of this release" .pfx .topic) }}{{ end -}}
{{- $_ := set $topics .topic true -}}
{{- if .scaledObject.enabled -}}
{{- if not .scaledObject.targetRef.name }}{{ fail (printf "%sscaledObject.targetRef.name is required when scaledObject.enabled" .pfx) }}{{ end -}}
{{- if hasKey $names .scaledObject.targetRef.name }}{{ fail (printf "%sscaledObject.targetRef.name %q is already scaled by another lease" .pfx .scaledObject.targetRef.name) }}{{ end -}}
{{- $_ := set $names .scaledObject.targetRef.name true -}}
{{- end -}}
{{- end -}}
{{- toYaml $all -}}
{{- end -}}

{{- /* kfklease.apiGroup: the group of an apiVersion, "" for the core group. */ -}}
{{- define "kfklease.apiGroup" -}}
{{- if contains "/" . }}{{ (split "/" .)._0 }}{{ end -}}
{{- end -}}

{{- /* kfklease.targetResource: the plural resource name of a targetRef,
its kind in lower case with an "s": deployments, statefulsets,
kafkaconnects, kafkamirrormaker2s. */ -}}
{{- define "kfklease.targetResource" -}}
{{- printf "%ss" (lower .kind) -}}
{{- end -}}

{{- /* kfklease.handover: whether the release renders the handover's
access: handover.enabled and at least one ScaledObject. */ -}}
{{- define "kfklease.handover" -}}
{{- if .Values.handover.enabled }}{{ range include "kfklease.leases" . | fromYamlArray }}{{ if .scaledObject.enabled }}true{{ end }}{{ end }}{{ end -}}
{{- end -}}
