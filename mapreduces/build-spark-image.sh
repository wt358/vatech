#!/bin/bash

DOCKER_IMAGE=${1:-cleverpms/myspark}

docker build -t ${DOCKER_IMAGE} -f- . <<EOF
FROM cleverpms/pyspark
COPY *.py /opt/spark/work-dir
EOF

docker push ${DOCKER_IMAGE}