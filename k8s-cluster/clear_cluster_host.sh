INGRESS_HOST="172.26.50.211"

DEPRECATED_HOSTNAME=( "haruband.k8s.com"
                      "minio.k8s.com"
                      "elastic.k8s.com"
                      "kibana.k8s.com"
                      "mongo.k8s.com"
                      "mysql.k8s.com"
                      "metabase.k8s.com"
                    )

for HOSTNAME in ${DEPRECATED_HOSTNAME[@]}; do
  HOST_ENTRY="$INGRESS_HOST $HOSTNAME"

  if [ -n "$(grep "$HOST_ENTRY" /etc/hosts )" ]
  then
    echo "remove $HOSTNAME. (deprecated)"
    sudo sed -i".bak" "/$HOST_ENTRY/d" /etc/hosts
  else
    echo "$HOSTNAME already removed."
  fi
done

echo "done."
