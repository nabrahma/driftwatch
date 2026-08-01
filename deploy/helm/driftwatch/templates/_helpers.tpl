{{/*
Expand the name of the chart.
*/}}
{{- define "driftwatch.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
A fully qualified app name, truncated to 63 characters because some Kubernetes
name fields are DNS labels.
*/}}
{{- define "driftwatch.fullname" -}}
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

{{- define "driftwatch.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "driftwatch.labels" -}}
helm.sh/chart: {{ include "driftwatch.chart" . }}
{{ include "driftwatch.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: driftwatch
{{- end }}

{{- define "driftwatch.selectorLabels" -}}
app.kubernetes.io/name: {{ include "driftwatch.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: manager
{{- end }}

{{- define "driftwatch.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "driftwatch.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
The image reference. An empty tag means the chart's appVersion, so upgrading the
chart moves the image with it unless somebody has deliberately pinned one.
*/}}
{{- define "driftwatch.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) }}
{{- end }}

{{/*
The webhook's serving certificate secret.
*/}}
{{- define "driftwatch.webhookCertSecret" -}}
{{- if .Values.webhook.certSecretName }}
{{- .Values.webhook.certSecretName }}
{{- else }}
{{- printf "%s-webhook-cert" (include "driftwatch.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Validation the chart can do that the CRD schema cannot, because it is about the
deployment rather than about one check.

Failing at `helm template` time is the whole point: each of these produces a
manifest that installs cleanly and then misbehaves in a way that is hard to
attribute. Two replicas without leader election is the worst of them — nothing
errors, the checks simply run twice and the metrics disagree with themselves.
*/}}
{{- define "driftwatch.validateValues" -}}
{{- if and (gt (int .Values.replicaCount) 1) (not .Values.manager.leaderElect) }}
{{- fail "driftwatch: replicaCount > 1 requires manager.leaderElect=true, or every replica runs every check: two oracles sweeping one store, both writing metrics under the same check label, and a divergent-key count that alternates between them" }}
{{- end }}
{{- if and (eq .Values.rbac.scope "namespace") (not .Values.manager.watchNamespace) }}
{{- fail "driftwatch: rbac.scope=namespace requires manager.watchNamespace, or the manager watches every namespace with permissions for one and fails on the first DriftCheck outside the release namespace" }}
{{- end }}
{{- if and .Values.webhook.enabled (not .Values.webhook.certManager.enabled) (not .Values.webhook.certSecretName) }}
{{- fail "driftwatch: webhook.enabled needs a certificate — set webhook.certManager.enabled=true or webhook.certSecretName to a secret holding tls.crt and tls.key" }}
{{- end }}
{{- if and .Values.metrics.serviceMonitor.enabled (not .Values.metrics.enabled) }}
{{- fail "driftwatch: metrics.serviceMonitor.enabled requires metrics.enabled" }}
{{- end }}
{{- end }}
