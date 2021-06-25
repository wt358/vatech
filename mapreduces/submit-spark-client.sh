#!/bin/bash

PYTHON_SOURCE=${1:-pisimple.py}
DOCKER_IMAGE=${2:-cleverpms/spark:latest}
K8S_MASTER=${3:-$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')}

spark-submit --master k8s://${K8S_MASTER} \
    --deploy-mode client \
    --name spark-pi \
    --conf spark.executor.instances=3 \
    --executor-memory 1024m \
    --conf spark.kubernetes.namespace=spark \
    --conf spark.kubernetes.container.image=${DOCKER_IMAGE} \
    --conf spark.kubernetes.container.image.pullPolicy=Always \
    ${PYTHON_SOURCE}