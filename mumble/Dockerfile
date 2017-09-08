FROM debian:jessie

LABEL maintainer "Daniel Faria <danielfnfaria@gmail.com>"

RUN apt-get update && apt-get install -y \
	mumble \
	alsa-utils

ENV HOME /home/dfaria

RUN useradd --create-home --home-dir $HOME dfaria \
	&& gpasswd -a dfaria audio \
	&& chown -R dfaria:dfaria $HOME

WORKDIR /home/dfaria

USER dfaria

ENTRYPOINT [ "mumble" ]
