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
