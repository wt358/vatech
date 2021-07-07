# !/bin/sh

set -ex
NAMESPACE=${1:-elasticsearch}
RESOURCE_YAML_DIR=${2:-deploy}
OPERATOR_DIR=${3:-operator}

kubectl create namespace ${NAMESPACE} --dry-run=client -o yaml | kubectl apply -f -
kubectl label ns ${NAMESPACE} istio-injection=enabled

for yaml in ${OPERATOR_DIR}/*
do
  kubectl apply ${yaml}
done

for yaml in ${RESOURCE_YAML_DIR}/*
do
  kubectl apply -f ${yaml}
done
