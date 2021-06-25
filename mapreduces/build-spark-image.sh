#!/bin/bash

DOCKER_IMAGE=${1:-cleverpms/spark-py:latest}
DOCKER_FILE=${2:-dockerfile.spark}

docker build -t ${DOCKER_IMAGE} -f dockerfile.spark .
docker push ${DOCKER_IMAGE}