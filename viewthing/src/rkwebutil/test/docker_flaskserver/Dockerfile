FROM rknop/devuan-chimaera-rknop

MAINTAINER Rob Knop <raknop@lbl.gov>

SHELL ["/bin/bash", "-c"]

RUN mkdir -p /flask
WORKDIR /flask

RUN DEBIAN_FRONTEND="noninteractive" TZ="US/Pacific" \
    apt-get update \
    && apt-get -y upgrade \
    && DEBIAN_FRONTEND="noninteractive" TZ="US/Pacific" \
    apt-get -y install -y \
       python3 python3-pip python3-pycryptodome python3-psycopg2 gunicorn tmux netcat curl elinks \
    && apt-get -y autoremove \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip --no-cache install \
  flask \
  flask-sock \
  flask-session \
  apscheduler \
  sqlalchemy \
  python-dateutil \
  pytz \
  requests

RUN mkdir /sessions

ENTRYPOINT [ "gunicorn", "-w", "1", "--threads", "10", "-b", "0.0.0.0:8081", "--timeout", "0", "server:app" ]
