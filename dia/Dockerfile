# Diagram
#
# docker run -d \
# -v /etc/localtime:/etc/localtime:ro \
# -v /tmp/.X11-unix:/tmp/.X11-unix \
# -e DISPLAY=unix$DISPLAY \
# -v $HOME/Documents/Diagrams:/root/diagrams \
# --name dia \
# danielfnfaria/dia
#
FROM debian

LABEL maintainer "Daniel Faria <danielfnfaria@gmail.com>"

RUN apt-get update && apt-get install --no-install-recommends -y \
	dia

ENTRYPOINT [ "dia" ]
