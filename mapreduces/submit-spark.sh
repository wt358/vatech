#!/bin/bash

HADOOP_VERSION=${HADOOP_VERSION:-3.2.0}
DELTALAKE_VERSION=${DELTALAKE_VERSION:-0.8.0}
MONGODB_CONNECTOR_VERSION=${MONGODB_CONNECTOR_VERSION:-3.0.1}
ELASTICSEARCH_HADOOP_VERSION=${ELASTICSEARCH_HADOOP_VERSION:-7.13.2}

spark-submit \
    --packages org.apache.hadoop:hadoop-aws:${HADOOP_VERSION},io.delta:delta-core_2.12:${DELTALAKE_VERSION},org.mongodb.spark:mongo-spark-connector_2.12:${MONGODB_CONNECTOR_VERSION},org.elasticsearch:elasticsearch-hadoop:${ELASTICSEARCH_HADOOP_VERSION} \
    $@