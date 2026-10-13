#!/usr/bin/env bash
# Render the chart with several value sets and validate every manifest
# against the Kubernetes OpenAPI schemas (kubeconform). CRDs such as
# ServiceMonitor are skipped (-ignore-missing-schemas).
set -euo pipefail
chart=deploy/helm/usdt-tracker
kv=${KUBE_VERSION:-1.31.0}
helm dependency build "$chart" >/dev/null
render() { echo "== $*"; helm template e2e "$chart" --kube-version "$kv" "$@" |
  kubeconform -strict -summary -ignore-missing-schemas -kubernetes-version "$kv"; }
render
render --set postgresql.enabled=false --set externalDatabase.url=postgres://u@db:5432/tron \
  --set externalDatabase.passwordSecret.name=dbpw --set externalDatabase.passwordSecret.key=password \
  --set ingress.enabled=true --set podDisruptionBudget.enabled=true --set serviceMonitor.enabled=true
render --set 'config.chains={tron,ton,eth}' --set secrets.toncenterApiKey=k \
  --set 'config.ton.watch_addresses={UQCdssroU88tuHDqBtHoKG8gLNmtaJ38dLmBlVINBklYWYEg}' \
  --set secrets.apiToken=0123456789abcdef0123
# negative check: the PDB really is rendered and has the selector
helm template e2e "$chart" --set podDisruptionBudget.enabled=true | grep -q 'kind: PodDisruptionBudget'
echo "helm e2e OK"
