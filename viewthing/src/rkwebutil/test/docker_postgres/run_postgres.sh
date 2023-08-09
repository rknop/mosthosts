#!/bin/bash

if [ ! -f $POSTGRES_DATA_DIR/PG_VERSION ]; then
    echo "Running initdb in $POSTGRES_DATA_DIR"
    echo $DB_PASS > $HOME/pwfile
    /usr/lib/postgresql/13/bin/initdb -U $DB_USER --pwfile=$HOME/pwfile $POSTGRES_DATA_DIR
    rm $HOME/pwfile
    /usr/lib/postgresql/13/bin/pg_ctl -D $POSTGRES_DATA_DIR start
    psql --command "CREATE DATABASE $DB_NAME OWNER $DB_USER"
    /usr/lib/postgresql/13/bin/pg_ctl -D $POSTGRES_DATA_DIR stop
fi
exec /usr/lib/postgresql/13/bin/postgres -c config_file=/etc/postgresql/13/main/postgresql.conf
