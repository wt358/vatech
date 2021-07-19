#!/bin/sh

set -ex
setup() { 
  helm repo add haproxy-ingress https://haproxy-ingress.github.io/charts
  helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
  helm repo update

  kubectl create ns benchmark
  kubectl config set-context --current --namespace=benchmark
  
}

haproxy_bench() {
  echo "starting haproxy bench.."
  helm install haproxy-ingress haproxy-ingress/haproxy-ingress\
    --namespace benchmark

  helm uninstall haproxy-ingress
}

teardown() {
  helm repo remove haproxy-ingress
  helm repo remove ingress-nginx
  kubectl delete ns benchmark
}

setup
haproxy_bench
teardown

