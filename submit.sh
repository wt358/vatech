#!/bin/bash

# Connector versions
MONGODB_CONNECTOR_VERSION=${MONGODB_CONNECTOR_VERSION:-3.0.1}

# Execution
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:${MONGODB_CONNECTOR_VERSION} $@
