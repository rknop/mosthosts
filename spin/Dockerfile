FROM rknop/devuan-chimaera-rknop
MAINTAINER Rob Knop <raknop@lbl.gov>

ENV DEBIAN_FRONTEND=noninteractive

# I don't understand the RUNLEVEL=1 thing, but
#  it worked around a problem with the apache2
#  post-install script.
RUN RUNLEVEL=1 apt-get update && apt-get -y upgrade && \
  apt-get -y install \
    apache2 python3 libapache2-mod-wsgi-py3 python3-pip python3-psycopg2 python3-pycryptodome \
      tmux netcat curl postgresql-client-13 && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

RUN pip3 --no-cache install \
  sqlalchemy \
  alembic \
  python-dateutil \
  pytz \
  web.py \
  requests

COPY ports.conf /etc/apache2/
COPY 000-default.conf /etc/apache2/sites-available/
COPY wsgi.conf.patch /usr/src/
RUN patch /etc/apache2/mods-available/wsgi.conf /usr/src/wsgi.conf.patch

RUN ln -s ../mods-available/rewrite.load /etc/apache2/mods-enabled/rewrite.load

# Do scary permissions stuff since we'll have to run
#  as a normal user.  But, given that we're running as
#  a normal user, that makes this less scary.
RUN mkdir -p /var/run/apache2
RUN chmod a+rwx /var/run/apache2
RUN mkdir -p /var/lock/apache2
RUN chmod a+rwx /var/lock/apache2
RUN chmod -R a+rx /etc/ssl/private
RUN mkdir -p /var/log/apache2
RUN chmod -R a+rwx /var/log/apache2

RUN apachectl start

ENTRYPOINT [ "apachectl", "-D", "FOREGROUND", "-D", "APACHE_CONFDIR=/etc/apache2" ]
