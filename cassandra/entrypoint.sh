#!/bin/bash

set -e

# env
CASSANDRA_DIR="/etc/cassandra"
IPADDRESS="$( ip addr | grep -i eth0 | grep -Eo '([0-9]{1,3}\.){3}[0-9]{1,3}')"


# env from docker run
if [[ ! $CLUSTER_NAME ]]; then
	CLUSTER_NAME="cassandra-docker"
else
	CLUSTER_NAME=$CLUSTER_NAME
fi

# configure cassandra
sed -i -e "s/Test\ Cluster/$CLUSTER_NAME/g" $CASSANDRA_DIR/cassandra.yaml
sed -i -e "s/localhost/$IPADDRESS/g" $CASSANDRA_DIR/cassandra.yaml

cassandra -R -f

exec "$@"
