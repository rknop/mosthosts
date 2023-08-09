FROM rknop/devuan-chimaera-rknop
MAINTAINER Rob Knop <raknop@lbl.gov>

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get -y upgrade && \
  apt-get -y install \
    python3 python3-pip python3-psycopg2 python3-pytest tmux postgresql-client-13 \
    firefox-esr elinks && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

# postfix

RUN pip3 install \
  sqlalchemy \
  pyyaml \
  python-dateutil \
  pytz \
  requests \
  selenium_firefox

RUN ln -s /usr/bin/python3 /usr/bin/python
RUN ln -s /usr/bin/pytest-3 /usr/bin/pytest
