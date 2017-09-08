#!/bin/bash

# setup variables
KAFKA_DIR="/kafka-server"
KREST_DIR="/kafka-rest"
CADDY_DIR="/caddy"
ZOO="localhost:2181"

# docker env
TOPICS=$TOPICS
ADVERTISED=$ADVERTISED

if [[ ! $TOPICS ]]; then
	echo "Please set TOPICS environment variable, -e TOPICS='example01 example02'"
	exit 2
fi

# configure kafka
sed -i -e "s/IPADDRESS/$ADVERTISED/g" $KAFKA_DIR/config/server.properties

# start daemons
$KAFKA_DIR/bin/zookeeper-server-start.sh -daemon $KAFKA_DIR/config/zookeeper.properties
$KAFKA_DIR/bin/kafka-server-start.sh -daemon $KAFKA_DIR/config/server.properties

# time for kafka start
sleep 3

for create_topic in $TOPICS; do

	$KAFKA_DIR/bin/kafka-topics.sh --list \
		--zookeeper $ZOO | grep -i "${create_topic}" > /dev/null

	if [[ $? != 0 ]]; then

		$KAFKA_DIR/bin/kafka-topics.sh --create \
			--zookeeper $ZOO \
			--partitions 2 \
			--replication-factor 1 \
			--topic "${create_topic}"

	else
		echo "Topic ${create_topic} already exist!"
	fi

done

# start kafka-rest
$KREST_DIR/bin/kafka-rest-start -daemon

# configure caddy
cat << EOF > $CADDY_DIR/Caddyfile

0.0.0.0:8000
tls off

root /kafka-topics-ui
log /access.log
proxy /api/kafka-rest-proxy "http://localhost:8082" {
	without /api/kafka-rest-proxy
}
	proxy /api/kafka-rest-proxy "http://localhost:8082" {
	without /api/kafka-rest-proxy
}
EOF

# start landoop-kafka-ui with caddy webserver
$CADDY_DIR/caddy -conf $CADDY_DIR/Caddyfile

exec "$@"
