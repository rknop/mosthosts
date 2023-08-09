import astropy.table
from astropy.io import fits
import sqlalchemy as sa

columns = [
    'SGA_ID',
    'SGA_GALAXY', 
    'GALAXY', 
    'PGC', 
    'RA_LEDA', 
    'DEC_LEDA', 
    'MORPHTYPE', 
    'PA_LEDA', 
    'D25_LEDA', 
    'BA_LEDA', 
    'Z_LEDA', 
    'SB_D25_LEDA', 
    'MAG_LEDA', 
    'BYHAND', 
    'REF', 
    'GROUP_ID', 
    'GROUP_NAME', 
    'GROUP_MULT', 
    'GROUP_PRIMARY', 
    'GROUP_RA', 
    'GROUP_DEC', 
    'GROUP_DIAMETER', 
    'BRICKNAME', 
    'RA', 
    'DEC', 
    'D26', 
    'D26_REF', 
    'PA', 
    'BA', 
    'RA_MOMENT', 
    'DEC_MOMENT', 
    'SMA_MOMENT'
]


with fits.open( "/global/cscratch1/sd/raknop/SGA-2020.fits" ) as sga:
    tab = astropy.table.Table( sga[1].data )[columns]
df = tab.to_pandas()

lcer = { i: i.lower() for i in columns }
df.rename( lcer, inplace=True, axis=1 )

engine = sa.create_engine( f'postgresql://desidb_admin:kqy1n02rnhg2@decatdb.lbl.gov:5432/desidb' )

df.to_sql( 'sga', schema='static', if_exists='append', con=engine, index=False )
