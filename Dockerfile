FROM python:3.9

#ARG WORKDIR=/code
#RUN mkdir $WORKDIR

RUN apt update \
 && apt install -y git \
 && apt-get install -y procps \
 && apt install -y vim \
 && apt install -y jq \
 && apt install -y curl

WORKDIR /code

RUN pip install -e git+https://github.com/tolitius/pgsync.git@main#egg=pgsync
