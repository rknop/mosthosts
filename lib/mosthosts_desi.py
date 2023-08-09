import os
import sys
import math
import pathlib
import pickle
import logging
import argparse
import ast
import psycopg2
import psycopg2.extras
import numpy as np
import pandas


_mhdlogger = logging.getLogger( "mosthosts_desi" )
_logout = logging.StreamHandler( sys.stderr )
_mhdlogger.addHandler( _logout )
_logout.setFormatter( logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s' ) )
# _mhdlogger.setLevel( logging.INFO )
_mhdlogger.setLevel( logging.DEBUG )

# ======================================================================

class MostHostsDesi(object):
    '''Build and hold a three different Pandas dataframes with info about mosthosts objects with Desi observations.
    
    Build the dataframes by instantiating a MostHostsDesi object; see __init__ for more information.

    In cases where the same targetid/tile/petal is repeated more than
    once, just the values from the *latest* night are kept.

    To get the actual spectra (i.e. the data), see `desi_specfinder.py`.

    ALL MOSTHOSTS DATAFRAME:
    
    Access the dataframe with the df property.  It has one row for each host in the Mosthosts table.
    
    The dataframe has two indexes:
    
    snname — the primary snname from the mosthosts table.  These names are heterogeneous.
    index — a counter starting from 1 for SNe from mosthosts that have more than one host.
    
    It has columns:
    
    spname — the name used for skyportal.  This will be the TNS name if it exists,
             else the IAU name if it exists, else the PTFIPTF name if it exists,
             else snname
    ra — ra of the host (degrees)
    dec — dec of the host (degrees)
    pmra —
    pmdec —
    ref_epoch —
    override —
    index —
    hemisphere — "north" or "south" (or "unknown")
    sn_ra — ra of the SN this is a possible host for (degrees)
    sn_dec — dec of the SN this is a possible host for (degrees)
    sn_z — redshift of the supernova (probably determined in a heterogenous manner...)
    program — a set of /-separated tags indicating where this SN came from
    priority —
    tns_name — Name of the SN on TNS
    iau_name — IAU name for the SN
    ptfiptf_name — Name from the Palomar Transient Factory for this SN

    The remaining columns come from the DESI data, pulled from the tiles_redshifts and cumulative_tiles tables.

    z — variance-weighted average of the nowarn DESI redshifts for this host, or null if none
    dz — uncertainty on z
    zdisp — Max-min of the individual z values observed for this host
    
    If you want to drill into where this z came from, look at the haszdf table


    DATAFRAME OF DESI OBSERVATIONS OF HOSTS

    This one captures which hosts have been observed by DESI.  It does
    not list each and every separate observation (as it uses DESI's
    coadded spectra), but it does list separate obvations of hosts with
    different tile and/or target ids.  (That is: DESI sometimes puts the
    same RA/DEC into different targets and tiles.  This dataframe
    doesn't combine them.  However, it read the DESI cumulative files,
    so multiple observations tagged with the same tile and target for a
    given host had their spectra combined before the redshifts was
    measured.)

    This dataframe omits MostHosts hosts that have no desi observations.

    Access the dataframe with the haszdf property

    This dataframe has *six* indices:

    spname — same as from df
    index — same as from df
    targetid — targetid from the DESI database
    tileid — tileid from the DESI database
    petal — petal from the DESI database
    night — night from the DESI database

    Columns are the basic MostHosts columns from df, plus:

    z — Redshift for this observation
    zerr — Uncertainty on redshift for this observation
    zwarn — If this isn't 0, you probably shouldn't trust the redshift
    chi2 —
    deltachi2 —
    spectype —
    subtype —


    DATAFRAME OF DESI MAIN TARGETS

    Access this with the maintargets property.  Only built if you run the find_main_targets() method.

    The dataframe has *five* indices, because it's possible that the
    same MH target will show up more than once as a DESI target.

    snnamne — same as from df
    index — same as from df
    survey — one of "main", "sv1", "sv2", "sv3", or "backup"
          NOTE --- CURRENTLY ONLY SEARCHES THE MAIN SURVEY, so this will always be "main"
    whenobs — one of "bright" or "dark"
    targetid — DESI target id.

    Columns are the basic MostHosts columns from df, plus:

    spname — same as from df
    desi_target — not sure why this isn't the same as targetid, but whatever
    bgs_target — I *think* 0 means not in BGS
    mws_target — (Same 0 interp)
    scnd_target

    '''
    
    @property
    def df( self ):
        return self._df
    
    @property
    def haszdf( self ):
        return self._haszdf

    @property
    def maintargets( self ):
        return self._maintargets

    
    # ========================================
    
    def __init__( self, release='daily', force_regen=False, latest_night_only=True, logger=None,
                  dbuserpwfile=None, dbuser=None, dbpasswd=None ):
        '''Build and return the a Pandas dataframe with info about Desi observation of mosthosts hosts.
        
        It matches by searching the daily tables by RA/Dec; things
        within 1" of the mosthosts host coordinate are considered a
        match.
        
        release — One of daily, everest, fuji, guadalupe, or fujilupe
                  The DESI release to get info for.  "Daily" looks at
                  the regularly updated databse of what's been done.
                  fujilupe is a special case that combines together fuji
                  and guadalipe into a single dataframe.

        force_regen — by default, just reads
                      "mosthosts_desi_{release}.csv" from the current
                      directory.  This is much faster, as the matching
                      takes some time, but will fall out of date.  Set
                      force_regen to True to force it to rebuild that
                      file from the current contents of the database.
                      If the .csv file doesn't exist, or doesn't have
                      all the expected columns, it will be regenerated
                      from the database even if force_regen is False.
                      
        latest_night_only — If True (default), and there are multiple
                            nights with desi spectra for the same
                            target/tile/petal, only keep the latest one.
                            Since we're looking at coadded spectra,
                            later nights should include the data of
                            earlier nights.  If False, keep all nights
                            as if they were separate things.

        dbuserpwfile — A file that has a single line with two words
                       separated by a single space.  The first is the
                       username for connecting to the desi database, the
                       second is the password.  This file should be kept
                       somewhere in your account that is *not*
                       world-readable.  Defaults to something in Rob's
                       directory that you can't read....
                       
        dbuser, dbpasswd — Instead of using a dbuserpwfile, you can just
                           pass the database user and password directly.
                           Be careful!  Don't leave this password
                           sitting around in files that are accessible
                           by people outside of the DESI collaboration!

        '''
        global _mhdlogger
        self.logger = _mhdlogger if logger is None else logger
        self.release = release

        self._dbuser = dbuser
        self._dbpasswd = dbpasswd
        if dbuserpwfile is None:
            self._dbuserpwfile = "/global/homes/r/raknop/secrets/decatdb_desi_desi"
        else:
            self._dbuserpwfile = dbuserpwfile
            
        cwd = pathlib.Path( os.getcwd() )
        csvfile = cwd / f"mosthosts_desi_{release}.csv"
        pklfile = cwd / f"mosthosts_desi_{release}.pkl"
        haszcsvfile = cwd / f"mosthosts_desi_{release}_desiobs.csv"
        haszpklfile = cwd / f"mosthosts_desi_{release}_desiobs.pkl"

        dbconn = self.connect_to_database()
        self.logger.info( "Loading mosthosts table..." )
        self._mosthosts = self.load_mosthosts( dbconn )
        self.logger.info( "...mosthosts table loaded." )
        dbconn.close()

        mustregen = False
        if force_regen:
            mustregen = True
        else:
            # Try to read what already exists; also make sure the csv files exist
            if csvfile.is_file() and pklfile.is_file() and haszcsvfile.is_file() and haszpklfile.is_file():
                with open( pklfile, "rb" ) as ifp:
                    self._df = pickle.load( ifp )
                with open( haszpklfile, "rb" ) as ifp:
                    self._haszdf = pickle.load( ifp )
                self.logger.info( "Read dataframes from pkl files" )
            else:
                mustregen = True

        if mustregen:
            self.generate_df( release, latest_night_only )

            # with open( pklfile, "wb" ) as ofp:
            #     pickle.dump( self._df, ofp )
            # with open( haszpklfile, "wb") as ofp:
            #     pickle.dump( self._haszdf, ofp )
            self._df.to_pickle( pklfile, protocol=5 )
            self._haszdf.to_pickle( haszpklfile, protocol=5 )
            self._df.to_csv( csvfile )
            self._haszdf.to_csv( haszcsvfile )

            self.logger.info( f"{csvfile.name} and {haszcsvfile.name} written." )

    # ========================================

    def connect_to_database( self ):
        if self._dbuser is None or self._dbpasswd is None:
            with open( self._dbuserpwfile ) as ifp:
                (self._dbuser,self._dbpasswd) = ifp.readline().strip().split()
                
        dbconn = psycopg2.connect( dbname='desidb', host='decatdb.lbl.gov',
                                   user=self._dbuser, password=self._dbpasswd,
                                   cursor_factory=psycopg2.extras.RealDictCursor )
        return dbconn
    
    # =============================================
    
    @staticmethod
    
    # Add the spname column
    def get_spname( row ):
        if row['tns_name'] != 'None':
            return row['tns_name']
        elif row['iau_name'] != 'None':
            return row['iau_name']
        elif row['ptfiptf_name'] != 'None':
            return row['ptfiptf_name']
        else:
            return row['snname']
            
    # ========================================

    def load_mosthosts( self, dbconn ):
        cursor = dbconn.cursor()
        q = "SELECT * FROM static.mosthosts"
        cursor.execute( q )
        mosthosts = pandas.DataFrame( cursor.fetchall() )

        spname = mosthosts.apply( self.get_spname, axis=1 )
        mosthosts['spname'] = spname

        # Change type of index to pandas Int64 so that it can be nullable
        mosthosts['index'] = mosthosts['index'].astype('Int64')

        mosthosts.set_index( ['snname', 'index' ], inplace=True )
        mosthosts = mosthosts.sort_index()

        cursor.close()
        return mosthosts
        
        
    # ========================================

    def query_desiobs_at_radec( self, ra, dec, release ):
        cursor = dbconn.cursor()
        qbase = ( "SELECT c.tileid,c.petal,c.night,r.targetid,r.z,r.zerr,r.zwarn,"
                  "r.chi2,r.deltachi2,r.spectype,r.subtype "
                  "FROM {release}.cumulative_tiles c "
                  "INNER JOIN {release}.tiles_redshifts r ON r.cumultile_id=c.id "
                  "INNER JOIN {release}.tiles_fibermap f ON f.cumultile_id=c.id AND f.targetid=r.targetid "
                  "WHERE q3c_radial_query(f.target_ra,f.target_dec,%(ra)s,%(dec)s,%(radius)s) " )
        # HACK ALERT
        if release == "fujilupe":
            qf = qbase.replace( "{release}", "fuji" )
            qg = qbase.replace( "{release}", "guadalupe" )
            q = f"({qf}) UNION ({qg}) ORDER BY night"
        else:
            if release not in ( 'daily', 'everest', 'fuji', 'guadalupe' ):
                raise ValueError( f'Unknown release {release}' )
            q = qbase.replace( "{release}", release )
            q += " ORDER BY night"
        cursor.execute( q, { "ra": ra, "dec": dec, "radius": 1./3600. } )
        rows = cursor.fetchall()
    
    # ========================================
            
    def generate_df( self, release, latest_night_only ):
        # Get the dataframe of information from the desi tables

        if release == "fujilupe":
            raise RuntimeError( "Fujilupe hack not implemented." )
        
        self.logger.info( f'Rebuilding info for release {release}' )
        
        dbconn = self.connect_to_database()
        desidf = None

        nhavesome = 0
        nredshifts = 0

        cursor = dbconn.cursor()

        # First, build a temporary table matching targetid/tile/petal to mosthosts
        self.logger.info( f'Sending q3c_join query for release {release}' )
        query = ( f"SELECT m.snname,m.index,f.targetid,f.tileid,f.petal_loc "
                  f"INTO TEMP TABLE temp_mosthosts_search1 "
                  f"FROM static.mosthosts m "
                  f"INNER JOIN {release}.tiles_fibermap f "
                  f"  ON q3c_join(m.ra,m.dec,f.target_ra,f.target_dec,%(radius)s) " )
        subs = { 'radius': 1./3600. }
        cursor.execute( query, subs )
        query = ( "SELECT COUNT(*) AS n FROM temp_mosthosts_search1" )
        cursor.execute( query )
        n = cursor.fetchone()['n']
        query = ( "SELECT COUNT(*) AS n FROM temp_mosthosts_search1 WHERE targetid IS NOT NULL" )
        cursor.execute( query )
        nwtarg = cursor.fetchone()['n']
        self.logger.info( f'...temporary table has {n} rows, {nwtarg} including a desi observation.' )

        # Next: get nights from cumulative_tiles redshifts etc. from tiles_redshifts

        self.logger.info( f'Getting night/redshift/type info' )
        query = ( f"SELECT m.snname,m.index,m.targetid,m.tileid,m.petal_loc,c.night,"
                  f"  r.z,r.zerr,r.zwarn,r.chi2,r.deltachi2,r.spectype,r.subtype "
                  f"FROM temp_mosthosts_search1 m "
                  f"INNER JOIN ("
                  f"  {release}.cumulative_tiles c INNER JOIN {release}.tiles_redshifts r ON r.cumultile_id=c.id"
                  f") ON (c.tileid,c.petal)=(m.tileid,m.petal_loc) AND m.targetid=r.targetid" )
        cursor.execute( query )
        desidf = pandas.DataFrame( cursor.fetchall() )
        self.logger.info( f"...done getting night/redshift/type info, got {len(desidf)} rows." )

        dbconn.close()
        
        # Convert some of the columns to pandas Int* so that they can be nullable
        desidf['targetid'] = desidf['targetid'].astype('Int64')
        desidf['tileid'] = desidf['tileid'].astype('Int64')
        desidf['petal_loc'] = desidf['petal_loc'].astype('Int16')
        desidf['night'] = desidf['night'].astype('Int32')
        desidf['zwarn'] = desidf['zwarn'].astype('Int64')
        desidf.rename( { 'petal_loc': 'petal' }, inplace=True, axis=1 )
        
        # Keep only the latest night for a given target/tile/petal

        if latest_night_only:
            prenightcull = len(desidf)
            desidf = desidf.loc[ desidf.groupby( ['targetid', 'tileid', 'petal', 'zwarn'] )['night'].idxmax() ]
            self.logger.info( f'{len(desidf)} of {prenightcull} redshifts left after keeping only latest night' )
            
        # Merge these with the _mosthosts table to make the _haszdf table

        self.logger.info( "Building hazdf..." )
        desidf.set_index( ['snname', 'index', 'targetid', 'tileid', 'petal', 'night' ], inplace=True )
        self._haszdf = self._mosthosts.join( desidf, how="inner" )

        # Combine together redshifts in desidf to make a sort of aggregate redshift
        # Then make the _df table by appending this to the _mosthosts talbe

        self.logger.info( "Building df..." )
        
        def zcomb( row ):
            norm = ( 1. / row['zerr']**2 ).sum()
            row['zdisp'] = row['z'].max() - row['z'].min()
            row['z'] = ( row['z'] / row['zerr']**2 ).sum() / norm
            row['zerr'] = np.sqrt( 1. / norm )
            return row.iloc[0]

        subdf = desidf[ desidf['zwarn'] == 0 ]
        combdf = subdf.reset_index().groupby( ['snname','index'] ).apply( zcomb )
        combdf = combdf.set_index( ['snname', 'index'] )[ [ 'z', 'zerr', 'zdisp' ] ]
        
        self._df = self._mosthosts.join( combdf, how='left' )
        
        self.logger.info( f"Done generating dataframes." )
        
    # ========================================
            
    def find_main_targets( self, radius=1./3600., force_regen=False ):
        """Search the DESI targets tables to match MostHosts to DESI main targets.

        Set force_regen=True to force searching the database.  Otherwise, it will read 
        mosthosts_desi_maintargets.pkl if that file exists.
        """

        cachefile = pathlib.Path( os.getcwd() ) / "mosthosts_desi_maintargets.pkl"
        csvfile = pathlib.Path( os.getcwd() ) / "mosthosts_desi_maintargets.csv"
        if ( not force_regen ) and ( cachefile.is_file() ) and ( csvfile.is_file() ):
            with open( cachefile, "rb" ) as ifp:
                self._maintargets = pickle.load( ifp )
            self.logger.info( f"MainTargets info read from {cachefile.name}" )
            return

        dfnoindex = self._df.reset_index()
        columns = {}
        for col in dfnoindex.columns:
            columns[col] = []
        dbcols = [ 'm.snname', 'm.index', 'm.tns_name', 'm.iau_name', 'm.ptfiptf_name', 
                   't.survey', 't.whenobs', 't.targetid',
                   't.desi_target', 't.bgs_target', 't.mws_target', 't.scnd_target' ]
        for col in dbcols:
            columns[col] = []
            
        dbconn = self.connect_to_database()
        cursor = dbconn.cursor()
        
        self.logger.info( f'Searching DESI targets for mosthosts' )
        q = ( f"SELECT {','.join(dbcols)} "
              f"FROM static.mosthosts m "
              f"INNER JOIN general.maintargets t ON q3c_join(m.ra,m.dec,t.ra,t.dec,%(radius)s)" )
        self.logger.debug( f"Sending query: {cursor.mogrify( q, { 'radius': radius } )}" )
        cursor.execute( q, { 'radius': radius } )
        rows = cursor.fetchall()
        
        self._maintargets = pandas.DataFrame( rows )
        spnames = self._maintargets.apply( self.get_spname, axis=1 )
        self._maintargets['spname'] = spnames
        self._maintargets.set_index( ['snname', 'index', 'survey', 'whenobs', 'targetid'], inplace=True )

        # with open( cachefile, "wb" ) as ofp:
        #     pickle.dump( self._maintargets, ofp )
        self._maintargets.to_pickle( cachefile )
        self._maintargets.to_csv( csvfile )
        self.logger.info( f"Wrote desi target info to {cachefile.name}" )
        
# ======================================================================

def main():
    parser = argparse.ArgumentParser( "Generate/read mosthosts_desi_{release}.csv",
                                      description = ( "This is really intended to be used as a library. "
                                                      "See README.md: pandoc -t plain README.md | less" ) )
    parser.add_argument( "-f", "--dbuserpwfile", default=None,
                         help=( "File with oneline username password for DESI database.  (Make sure this file "
                                "is not world-readable!!)  Must specify either htis, or -u and -p" ) )
    parser.add_argument( "-u", "--dbuser", default=None, help="User for desi database" )
    parser.add_argument( "-p", "--dbpasswd", default=None, help="Password for desi database" )
    parser.add_argument( "-r", "--force-regen", default=False, action="store_true",
                         help=( "Force regeneration of mosthosts_desi.csv from the desi database "
                                " (by default, just read mosthosts_dei.csv, and do nothing)" ) )
    parser.add_argument( "release", help="Release to build the file for (everest or daily)" )
    args = parser.parse_args()

    if ( args.dbuserpwfile is None ) and ( args.dbuser is None or args.dbpasswd is None ):
        sys.stderr.write( "Must specify either -f, or both -u and -p.\n" )
        sys.exit(20)

    mhd = MostHostsDesi( dbuser=args.dbuser, dbpasswd=args.dbpasswd, dbuserpwfile=args.dbuserpwfile,
                         force_regen=args.force_regen, release=args.release )

# ======================================================================

if __name__ == "__main__":
    main()
    