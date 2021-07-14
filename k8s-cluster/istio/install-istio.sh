#!/bin/sh

set -ex

for yaml in deploy/*
do
  kubectl apply -f ${yaml} -n istio-system
done
