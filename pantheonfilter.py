import sys
import os
import io
import pathlib
import pandas
import psycopg2
import psycopg2.extras

pantheon = pandas.read_csv( 'pantheonplus_20221017.csv' )

with open( pathlib.Path( os.getenv("HOME") ) / "secrets/decatdb_desi_desi" ) as ifp:
    dbuser, dbpasswd = ifp.readline().strip().split()

db = psycopg2.connect( dbname='desidb', host='decatdb.lbl.gov', user=dbuser, password=dbpasswd )
cursor = db.cursor()

sys.stderr.write( "Populating pantheon temp table" )
cursor.execute( "CREATE TEMP TABLE pantheon( name text, ra double precision, dec double precision, "
                "hostra double precision, hostdec double precision, zcmb double precision  )" )

# This would work with psycopg3, doesn't with psycopg2
# with cursor.copy( "COPY pantheon(name,ra,dec) FROM STDIN" ) as copy:
#     for (i, row) in pantheon.iterrows():
#         copy.write_row( ( row['SNID'], row['RA'], row['DEC'] ) )

strio = io.StringIO("")
for (i, row) in pantheon.iterrows():
    strio.write( f"{row['SNID']}\t{row['RA']}\t{row['Dec']}\t"
                 f"{row['RA_host']}\t{row['Dec_host']}\t{row['zcmb']}\n" )
strio.seek( 0 )
cursor.copy_from( strio, 'pantheon', columns=( 'name', 'ra', 'dec', 'hostra', 'hostdec', 'zcmb' ) )
        
sys.stderr.write( "Joining to mosthosts" )
cursor.execute( "SELECT m.snname,p.name,m.ra AS mhra,m.dec AS mhdec,p.ra AS pra,p.dec AS pdec, "
                "  p.hostra AS phostra,p.hostdec AS phostdec,p.zcmb AS pzcmb "
                "INTO TEMP TABLE pantheon_match "
                "FROM pantheon p "
                "LEFT JOIN static.mosthosts m ON q3c_join(p.ra,p.dec,m.ra,m.dec,2./3600.)" )

sys.stderr.write( "Pulling results" )
match = pandas.read_sql_query( "SELECT * FROM pantheon_match", db )
match.to_csv( 'full_match.csv' )

inmosthosts = match[ ~match['snname'].isnull() ]
newsne = match[ match['snname'].isnull() ]

newsne[['name','pra','pdec','phostra','phostdec','pzcmb']].to_csv( 'new_pantheon_sne.csv', index=False )

