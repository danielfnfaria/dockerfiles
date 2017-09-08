# Libre Office
#
# docker run -d \
# -v /etc/localtime:/etc/localtime:ro \
# -v /tmp/.X11-unix:/tmp/.X11-unix \
# -e DISPLAY=unix$DISPLAY \
# -v $HOME/Documents/Slides:/root/slides \
# -e GDK_SCALE \
# -e GDK_DPI_SCALE \
# --name libreoffice \
# danielfnfaria/libreoffice

FROM debian

LABEL maintainer "Daniel Faria <danielfnfaria@gmail.com>"

RUN apt-get update && \
		apt-get install -y libreoffice

ENTRYPOINT [ "libreoffice" ]
