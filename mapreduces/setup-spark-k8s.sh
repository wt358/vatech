#!/bin/bash

cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Namespace
metadata:
  name: spark
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark
  namespace: spark
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: spark-role
rules:
  - apiGroups: [""]
    resources: ["pods", "services", "configmaps"]
    verbs: ["create", "get", "watch", "list", "post", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: spark-role-binding
subjects:
  - kind: ServiceAccount
    name: spark
    namespace: spark
roleRef:
  kind: ClusterRole
  name: spark-role
  apiGroup: rbac.authorization.k8s.io
EOF