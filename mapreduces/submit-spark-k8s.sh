#!/bin/bash

PYTHON_SOURCE=${1:-pisimple.py}
DOCKER_IMAGE=${2:-cleverpms/spark-py:latest}
SPARK_EXECUTORS=${3:-3}
SPARK_MEMORY=${4:-1024m}
K8S_MASTER=${5:-$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')}

BASENAME=$(basename -- ${PYTHON_SOURCE})
FILENAME="${BASENAME%.*}"

spark-submit --master k8s://${K8S_MASTER} \
    --deploy-mode cluster \
    --name ${FILENAME} \
    --conf spark.executor.instances=${SPARK_EXECUTORS} \
    --executor-memory ${SPARK_MEMORY} \
    --conf spark.kubernetes.namespace=spark \
    --conf spark.kubernetes.container.image=${DOCKER_IMAGE} \
    --conf spark.kubernetes.container.image.pullPolicy=Always \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    local:///opt/spark/work-dir/${PYTHON_SOURCE}