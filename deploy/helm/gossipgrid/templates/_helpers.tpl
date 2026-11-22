{{/*
GossipGrid chart helpers.
*/}}

{{/*
The fully-qualified name of the peer headless Service (used for DNS SRV
discovery and for the peer identity suffix).
*/}}
{{- define "gossipgrid.peerServiceFQDN" -}}
{{- printf "%s.%s.svc.%s" .Values.peerServiceName .Release.Namespace .Values.clusterDomain -}}
{{- end -}}

{{/*
The peer identity suffix: the DNS suffix following the pod name in the
certificate SAN. E.g. "gossipgrid-headless.gossipgrid.svc.cluster.local".
When .Values.peerTLS.identitySuffix is explicitly set, use it; otherwise
derive from the headless Service FQDN.
*/}}
{{- define "gossipgrid.peerIdentitySuffix" -}}
{{- if .Values.peerTLS.identitySuffix -}}
{{- .Values.peerTLS.identitySuffix -}}
{{- else -}}
{{- template "gossipgrid.peerServiceFQDN" . -}}
{{- end -}}
{{- end -}}

{{/*
The DNS discovery name: the headless Service DNS name used by
GOSSIPGRID_DISCOVERY_DNS_NAME. When .Values.discovery.dnsName is explicitly
set, use it; otherwise derive from the headless Service FQDN.
*/}}
{{- define "gossipgrid.discoveryDNSName" -}}
{{- if .Values.discovery.dnsName -}}
{{- .Values.discovery.dnsName -}}
{{- else -}}
{{- template "gossipgrid.peerServiceFQDN" . -}}
{{- end -}}
{{- end -}}

{{/*
The public FQDN the browser reaches via the Cilium Gateway.
*/}}
{{- define "gossipgrid.publicFQDN" -}}
{{- .Values.publicFQDN -}}
{{- end -}}

{{/*
The image reference. When .Values.image.digest is set, use it (immutable);
otherwise use repository:tag.
*/}}
{{- define "gossipgrid.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository .Values.image.tag -}}
{{- end -}}
{{- end -}}

{{/*
The issuer reference for cert-manager: use the ClusterIssuer name if set,
otherwise the namespaced Issuer name.
*/}}
{{- define "gossipgrid.issuerRef" -}}
{{- if .Values.certManagerClusterIssuerName -}}
{{- .Values.certManagerClusterIssuerName -}}
{{- else -}}
{{- .Values.certManagerIssuerName -}}
{{- end -}}
{{- end -}}

{{/*
The browser issuer reference for cert-manager.
*/}}
{{- define "gossipgrid.browserIssuerRef" -}}
{{- if .Values.gateway.browserClusterIssuerName -}}
{{- .Values.gateway.browserClusterIssuerName -}}
{{- else -}}
{{- .Values.gateway.browserIssuerName -}}
{{- end -}}
{{- end -}}
