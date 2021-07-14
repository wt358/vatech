INGRESS_HOST="172.26.50.211"

HOSTNAME="haruband.k8s.com"
HOST_ENTRY="$INGRESS_HOST $HOSTNAME"
if [ -n "$(grep "$HOST_ENTRY" /etc/hosts )" ]
then
  echo "remove $HOSTNAME. (deprecated)"
  sudo sed -i".bak" "/$HOST_ENTRY/d" /etc/hosts
else
  echo "$HOSTNAME already removed."
fi


HOSTNAMES=( "minio.k8s.com"
            "elastic.k8s.com"
            "kibana.k8s.com"
            "mongo.k8s.com"
            "mysql.k8s.com"
            "metabase.k8s.com"
          )
for HOSTNAME in ${HOSTNAMES[@]}; do
  HOST_ENTRY="$INGRESS_HOST $HOSTNAME"
  if [ -n "$(grep "$HOST_ENTRY" /etc/hosts )" ]
  then
    echo "$HOSTNAME exists in /etc/hosts, ignore.."
  else
    echo "creating $HOSTNAME in /etc/hosts."
    sudo echo "${INGRESS_HOST} $HOSTNAME" >> /etc/hosts
  fi
done

echo "done."
