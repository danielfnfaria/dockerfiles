FROM debian:latest

LABEL maintainer "Daniel Faria <danielfnfaria@gmail.com>"

ENV CASSANDRA_DIR="/etc/cassandra"

RUN apt-get update && apt-get install -y \
	default-jdk \
	dirmngr \
	procps \
	gnupg \
	curl && \
	curl https://www.apache.org/dist/cassandra/KEYS | apt-key add - && \
	echo "deb http://www.apache.org/dist/cassandra/debian 311x main" > /etc/apt/sources.list.d/cassandra.sources.list && \
	apt-get update && \
	apt-get install -y cassandra \
	cassandra-tools

ENV SSL_CERTFILE="/etc/cassandra/certs/client.pem"
ENV SSL="yes"

RUN mkdir -p $CASSANDRA_DIR/certs

COPY config/cassandra.yaml /etc/cassandra/cassandra.yaml
COPY entrypoint.sh /entrypoint.sh

EXPOSE 7000 7001 7199 9042 9160

ENTRYPOINT [ "/entrypoint.sh" ]
