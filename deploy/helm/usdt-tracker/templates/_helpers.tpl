{{/* Chart name. */}}
{{- define "usdt-tracker.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/* Fully qualified app name. */}}
{{- define "usdt-tracker.fullname" -}}
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

{{- define "usdt-tracker.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "usdt-tracker.labels" -}}
helm.sh/chart: {{ include "usdt-tracker.chart" . }}
{{ include "usdt-tracker.selectorLabels" . }}
app.kubernetes.io/version: {{ .Values.image.tag | default .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "usdt-tracker.selectorLabels" -}}
app.kubernetes.io/name: {{ include "usdt-tracker.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "usdt-tracker.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "usdt-tracker.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "usdt-tracker.image" -}}
{{- if .Values.image.digest }}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- else }}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) }}
{{- end }}
{{- end }}

{{/* Name of the Secret holding app secrets (created or existing). */}}
{{- define "usdt-tracker.secretName" -}}
{{- default (include "usdt-tracker.fullname" .) .Values.secrets.existingSecret }}
{{- end }}

{{/*
Bundled PostgreSQL service/secret name; mirrors Bitnami's common.names.fullname
for the "postgresql" subchart.
*/}}
{{- define "usdt-tracker.postgresql.fullname" -}}
{{- if .Values.postgresql.fullnameOverride }}
{{- .Values.postgresql.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default "postgresql" .Values.postgresql.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "usdt-tracker.postgresql.secretName" -}}
{{- default (include "usdt-tracker.postgresql.fullname" .) .Values.postgresql.auth.existingSecret }}
{{- end }}

{{/* True when a database (bundled or external) is configured. */}}
{{- define "usdt-tracker.databaseEnabled" -}}
{{- if or .Values.postgresql.enabled .Values.externalDatabase.url .Values.externalDatabase.existingSecret -}}
true
{{- end }}
{{- end }}
