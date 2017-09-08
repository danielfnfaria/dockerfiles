FROM debian:jessie

LABEL maintainer "Daniel Faria <danielfnfaria@gmail.com>"

RUN apt-get update && apt-get install -y \
		irssi \
		bitlbee

RUN apt-get install -y  \
	bitlbee-libpurple \
	screen \
	libgcrypt20-dev \
	libpurple-dev \
	libwebp-dev \
	gettext

COPY entrypoint.sh /entrypoint.sh

COPY telegram-purple_1.2.4-2_amd64.deb /telegram-purple_1.2.4-2_amd64.deb

RUN dpkg -i /telegram-purple_1.2.4-2_amd64.deb

ENTRYPOINT [ "/entrypoint.sh" ]
