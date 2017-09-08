FROM debian:jessie

LABEL maintainer "Daniel Faria <danielfnfaria@gmail.com>"

RUN apt-get update && apt-get install -y \
		dirmngr \
		gnupg \
		--no-install-recommends \
		&& apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys BBEBDCB318AD50EC6865090613B00F1FD2C19886 \
		&& echo "deb http://repository.spotify.com stable non-free" >> /etc/apt/sources.list.d/spotify.list \
		&& echo "deb http://ftp.debian.org/debian jessie main" >> /etc/apt/sources.list.d/jessie.list \
		&& apt-get update && apt-get install -y \
		alsa-utils \
		spotify-client

ENV HOME /home/spotify

RUN useradd --create-home --home-dir $HOME spotify \
		&& gpasswd -a spotify audio \
		&& chown -R spotify:spotify $HOME

WORKDIR $HOME

USER spotify

RUN echo "QLineEdit { color: #000 }" > /home/spotify/spotify-override.css

ENTRYPOINT [ "spotify" ]

CMD [ "-stylesheet=/home/spotify/spotify-override.css" ]
