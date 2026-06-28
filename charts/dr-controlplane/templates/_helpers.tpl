{{/* Common names and labels. */}}

{{- define "dr.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "dr.fullname" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "dr.labels" -}}
app.kubernetes.io/name: {{ include "dr.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "dr.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}

{{- define "dr.etcdServiceName" -}}
{{- printf "%s-etcd" (include "dr.fullname" .) -}}
{{- end -}}

{{/*
etcd client endpoints. When etcd.deploy is true, enumerate the StatefulSet pods'
stable DNS. Otherwise use the configured external endpoints.
*/}}
{{- define "dr.etcdEndpoints" -}}
{{- if .Values.etcd.deploy -}}
{{- $svc := include "dr.etcdServiceName" . -}}
{{- $ns := .Values.namespace -}}
{{- $eps := list -}}
{{- range $i := until (int .Values.etcd.replicas) -}}
{{- $eps = append $eps (printf "http://%s-%d.%s.%s.svc:2379" $svc $i $svc $ns) -}}
{{- end -}}
{{- join "," $eps -}}
{{- else -}}
{{- join "," .Values.etcd.externalEndpoints -}}
{{- end -}}
{{- end -}}
