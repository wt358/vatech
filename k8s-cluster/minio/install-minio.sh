#!/bin/sh

set -ex
kubectl krew update
kubectl krew install minio
kubectl minio init

for yaml in deploy/*
do
  kubectl apply -f ${yaml}
done
