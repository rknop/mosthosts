FROM rknop/devuan-chimaera-rknop
MAINTAINER Rob Knop <raknop@lbl.gov>

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get -y upgrade && \
  apt-get -y install \
    apache2 python3 libapache2-mod-wsgi-py3 python3-pip python3-psycopg2 python3-pycryptodome \
      tmux postgresql-client-13 make git && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

RUN pip3 install \
  sqlalchemy \
  pyyaml \
  python-dateutil \
  pytz \
  web.py \
  requests

COPY ports.conf /etc/apache2/
COPY 000-default.conf /etc/apache2/sites-available/
COPY wsgi.conf.patch /usr/src/
RUN patch /etc/apache2/mods-available/wsgi.conf /usr/src/wsgi.conf.patch

COPY createdb.py /usr/src

RUN apachectl start

ENTRYPOINT [ "apachectl", "-D", "FOREGROUND", "-D", "APACHE_CONFDIR=/etc/apache2" ]
