FROM debian:latest

LABEL maintainer "Daniel Faria <danielfnfaria@gmail.com>"

ENV TOPICS=""
ENV ADVERTISED="localhost"

ENV KAFKAREST="https://github.com/confluentinc/kafka-rest.git"
ENV CADDY="https://github.com/mholt/caddy/releases/download/v0.10.6/caddy_v0.10.6_linux_amd64.tar.gz"

RUN apt-get update && apt-get install -y \
	default-jdk \
	git \
	wget

RUN git clone $KAFKAREST /kafka-rest && \
	mkdir /caddy && \
	wget -q -O- $CADDY | tar -xvz -C /caddy

COPY "kafka-server" "/kafka-server"
COPY "kafka-rest" "/kafka-rest"
COPY "kafka-topics-ui" "/kafka-topics-ui"
COPY "entrypoint.sh" "/entrypoint.sh"

EXPOSE 8082 9092 8000

ENTRYPOINT [ "/entrypoint.sh" ]
