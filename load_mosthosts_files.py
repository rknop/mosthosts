import sys
import os
import re
import pathlib
import logging
import psycopg2
import psycopg2.extras
import pandas
import numpy

_dbcon = None
_tablename = 'mosthosts';

columns = { 
    "id":  { "type": "uuid", "extra": "DEFAULT public.uuid_generate_v4() PRIMARY KEY" }, 
    "sn_name_sp": { "type": "text", "index": True, "extra": "NOT NULL" },
    "hostnum": { "type": "smallint", "extra": "NOT NULL" },

    "ra": { "type": "double precision", "q3c": True, "extra": "NOT NULL", },
    "dec": { "type": "double precision", "extra": "NOT NULL" },
    "origin": { "type": "text" },
    "sn_type": { "type": "text" },
    "sn_z": { "type": "real" },
    "sn_ra": { "type": "double precision" },
    "sn_dec": { "type": "double precision" },
    "sn_name": { "type": "text", "index": True },
    "sn_name_ptf": { "type": "text", "index": True },
    "sn_name_iau": { "type": "text", "index": True },
    "sn_name_tns": { "type": "text", "index": True },
    "program": { "type": "text" },

    "ls_id_dr9": { "type": "bigint" },
    "dec_dr9": { "type": "double precision", },
    "ra_dr9": { "type": "double precision", "q3c": True },
    "ref_id_dr9": { "type": "bigint" },
    "brickid_dr9": { "type": "int" },
    "ref_cat_dr9": { "type": "text" },
    "type_dr9": { "type": "text" },
    "dist_arcsec_dr9": { "type": "real" },
    "sep_in_radius_dr9": { "type": "real" },
    "sep_in_radius_sigma_dr9": { "type": "real" },
    "fiberflux_g_dr9": { "type": "real" },
    "fiberflux_r_dr9": { "type": "real" },
    "fiberflux_z_dr9": { "type": "real" },
    "fibertotflux_g_dr9": { "type": "real" },
    "fibertotflux_r_dr9": { "type": "real" },
    "fibertotflux_z_dr9": { "type": "real" },
    "dchisq_1_dr9": { "type": "real" },
    "dchisq_2_dr9": { "type": "real" },
    "dchisq_3_dr9": { "type": "real" },
    "dchisq_4_dr9": { "type": "real" },
    "dchisq_5_dr9": { "type": "real" },
    "ra_ivar_dr9": { "type": "real" },
    "dec_ivar_dr9": { "type": "double precision" },
    "dered_flux_g_dr9": { "type": "real" },
    "dered_flux_r_dr9": { "type": "real" },
    "dered_flux_w1_dr9": { "type": "real" },
    "dered_flux_w2_dr9": { "type": "real" },
    "dered_flux_w3_dr9": { "type": "real" },
    "dered_flux_w4_dr9": { "type": "real" },
    "dered_flux_z_dr9": { "type": "real" },
    "flux_ivar_g_dr9": { "type": "real" },
    "flux_ivar_r_dr9": { "type": "real" },
    "flux_ivar_w1_dr9": { "type": "real" },
    "flux_ivar_w2_dr9": { "type": "real" },
    "flux_ivar_w3_dr9": { "type": "real" },
    "flux_ivar_w4_dr9": { "type": "real" },
    "flux_ivar_z_dr9": { "type": "real" },
    "fracflux_g_dr9": { "type": "real" },
    "fracflux_r_dr9": { "type": "real" },
    "fracflux_w1_dr9": { "type": "real" },
    "fracflux_w2_dr9": { "type": "real" },
    "fracflux_w3_dr9": { "type": "real" },
    "fracflux_w4_dr9": { "type": "real" },
    "fracflux_z_dr9": { "type": "real" },
    "fracin_g_dr9": { "type": "real" },
    "fracin_r_dr9": { "type": "real" },
    "fracin_z_dr9": { "type": "real" },
    "fracmasked_g_dr9": { "type": "real" },
    "fracmasked_r_dr9": { "type": "real" },
    "fracmasked_z_dr9": { "type": "real" },

    "sga_id_sga": { "type": "bigint" },
    "sga_galaxy_sga": { "type": "text" },
    "galaxy_sga": { "type": "text" },
    "ra_sga": { "type": "double precision" },
    "dec_sga": { "type": "double precision" },
    "ra_leda_sga": { "type": "double precision" },
    "dec_leda_sga": { "type": "double precision" },
    "morphtype_sga": { "type": "text" },
    "pa_leda_sga": { "type": "text" },
    "d25_leda_sga": { "type": "text" },
    "ba_leda_sga": { "type": "text" },
    "z_leda_sga": { "type": "text" },
    "ref_sga": { "type": "text" },
    "group_id_sga": { "type": "int" },
    "group_name_sga": { "type": "text" },
    "group_ra_sga": { "type":  "double precision" },
    "group_dec_sga": { "type": "double precision" },
    "group_diameter_sga": { "type": "real" },
    "d26_sga": { "type": "real" },
    "d26_ref_sga": { "type": "text" },
}        

_logger = logging.getLogger( __name__ )
if not _logger.hasHandlers():
    _logout = logging.StreamHandler( sys.stderr )
    _logger.addHandler( _logout )
    _logout.setFormatter( logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s' ) )
_logger.setLevel( logging.INFO )

def dbcon():
    global _engine, _dbcon
    if _dbcon is None:
        with open( pathlib.Path( os.getenv("HOME") ) / "secrets/decatdb_desi_admin" ) as ifp:
            passwd = ifp.readline().strip()
        _dbcon = psycopg2.connect( dbname='desidb', host='decatdb.lbl.gov', user='desidb_admin', password=passwd )
    return _dbcon

def create_table():
    conn = dbcon()
    cursor = conn.cursor()
    indices = []
    q3c = []
    q = f"CREATE TABLE static.{_tablename}( ";
    first = True
    for key, val in columns.items():
        if first:
            first = False
        else:
            q += ", "
        q += f"{key} {val['type']}"
        if 'extra' in val:
            q += f" {val['extra']}"
        if ( 'index' in val ) and val['index']:
            indices.append( key )
        if ( 'q3c' in val ) and val['q3c']:
            q3c.append( key )
    q += ")"
    cursor.execute( q )
    for dex in indices:
        cursor.execute(f'CREATE INDEX ON static.{_tablename} USING btree({dex})' )
    raparse = re.compile( "^(.*)ra(.*)$" )
    for racol in q3c:
        match = raparse.search( racol )
        if match is None:
            raise ValueError( f"Can't find 'ra' in {racol} for q3c creation" )
        deccol = f"{match.group(1)}dec{match.group(2)}"
        cursor.execute( f'CREATE INDEX ON static.{_tablename} (q3c_ang2ipix({racol},{deccol}))' )
    conn.commit()
                    

def read_all_files( direc ):
    direc = pathlib.Path( direc )
    files = list( direc.glob( 'df*.csv') )
    files.sort()
    dfs = []
    typedict = { k: lambda x: None if len(x)==0 else numpy.int64(float(x))
                 for k in columns.keys() if columns[k]['type'] == 'bigint' }
    for f in files:
        _logger.info( f"Reading {f}" )
        curdf = pandas.read_csv( f, converters=typedict )
        # Strip out "unnamed" columns,
        # as there seem to be inconsistencies
        drops = []
        for col in curdf.columns:
            if col[0:7] == "Unnamed":
                drops.append( col )
        curdf.drop( columns=drops, inplace=True )
        dfs.append( curdf )

    df = pandas.concat( dfs, axis='rows' )
        
    # Set ra from dr9 if it's there, otherwise sga

    df["ra"] = None
    df["dec"] = None
    df["hostnum"] = None
    w = pandas.isnull( df.ra_dr9 )
    df.loc[~w, 'ra'] = df.loc[~w].ra_dr9
    df.loc[~w, 'dec'] = df.loc[~w].dec_dr9
    df.loc[w, 'ra'] = df.loc[w].ra_sga
    df.loc[w, 'dec'] = df.loc[w].dec_sga
    
    # Assign host numbers right now without
    # trying to match to old mosthosts
    # Pandas is so mysterious
    df.hostnum = df.groupby('sn_name_sp').cumcount().add(1)-1

    # lowercaseize all column names
    df.rename( { i: i.lower() for i in df.columns }, axis='columns', inplace=True )

    # specific renames
    df.rename( { 'z': 'sn_z', 'type': 'sn_type' }, axis='columns', inplace=True )
    df.replace( { numpy.nan: None }, inplace=True )
    df.replace( { 'None': None }, inplace=True )
    
    return df

def load_df( df ):
    _logger.info( f"Loading {len(df)} rows into table" )
    conn = dbcon()
    cursor = conn.cursor()
    q = f"INSERT INTO static.{_tablename}({','.join( [ i for i in columns.keys() if i != 'id' ] )}) "
    q += f"VALUES ({','.join( [ f'%({i})s' for i in columns.keys() if i != 'id' ] )})"
    args = df.where( pandas.notnull(df), None ).to_dict( orient='records' )
    psycopg2.extras.execute_batch( cursor, q, args )
    conn.commit()

def main():
    create_table()
    df = read_all_files( 'files_mosthosts_20240222' )
    load_df( df )

if __name__ == "__main__":
    main()
