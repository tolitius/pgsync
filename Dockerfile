FROM python:3.12

#ARG WORKDIR=/code
#RUN mkdir $WORKDIR

RUN apt update \
 && apt install -y git \
 && apt-get install -y procps \
 && apt install -y vim \
 && apt install -y jq \
 && apt install -y curl \
 && apt install -y tmux \
 && apt clean all

WORKDIR /code

RUN pip install --no-cache-dir -e git+https://github.com/tolitius/pgsync.git@main#egg=pgsync
