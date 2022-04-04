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


    ALL MOSTHOSTS DATAFRAME:
    
    Access the dataframe with the df property.
    
    The dataframe has two indexes:
    
    spname — the name used for skyportal.  This will be the TNS name if it exists,
             else the IAU name if it exists, else the PTFIPTF name if it exists,
             else snname
    index — a counter starting from 1 for SNe from mosthosts that have more than one host.
    
    It has columns:
    
    snname — the primary snname from the mosthosts table.  These names are heterogeneous.
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

    Access this with the maintargets DataFrame.  Only built if you run the find_main_targets() method.

    The dataframe has *five* indices, because it's possible that the
    same MH target will show up more than once as a DESI target.

    spnamne — same as from df
    index — same as from df
    survey — one of "main", "sv1", "sv2", "sv3", or "backup"
          NOTE --- CURRENTLY ONLY SEARCHES THE MAIN SURVEY, so this will always be "main"
    whenobs — one of "bright" or "dark"
    targetid — DESI target id.

    Columns are the basic MostHosts columns from df, plus:

    desi_target — not sure why this isn't the same as targetid, but whatever
    bgs_target — I *think* 0 means not in BGS
    mws_target — (Same 0 interp)
    scnd_target
    # (this next column is not really there)
    # targetfile — The name of the FITS file that this target information is from;
    #        replace /targets/ with /global/cfs/cdirs/desi/target/ to find on NERSC

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

            with open( pklfile, "wb" ) as ofp:
                pickle.dump( self._df, ofp )
            with open( haszpklfile, "wb") as ofp:
                pickle.dump( self._haszdf, ofp )
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
    
    # ========================================

    def load_mosthosts( self, dbconn ):
        cursor = dbconn.cursor()
        q = "SELECT * FROM static.mosthosts"
        cursor.execute( q )
        mosthosts = pandas.DataFrame( cursor.fetchall() )

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
        spname = mosthosts.apply( get_spname, axis=1 )
        mosthosts['spname'] = spname

        # Change type of index to pandas Int64 so that it can be nullable
        mosthosts['index'] = mosthosts['index'].astype('Int64')

        # # Add a column that indexes the hosts for that one supernova, and then rearrange the table
        mosthosts = mosthosts.set_index( ['snname', 'index' ] )
        mosthosts = mosthosts.sort_index()

        cursor.close()
        return mosthosts
        
        
    # ========================================
            
    def generate_df( self, release, latest_night_only ):
        # Get the dataframe of information from the desi tables

        dbconn = self.connect_to_database()
        desidf = None

        nhavesome = 0
        nredshifts = 0
        for i in range(len(self._mosthosts)):
        # for i in range(1000):
            if (i%1000) == 0:
                self.logger.info( f'Done {i} of {len(self._mosthosts)} hosts; '
                                  f'{nhavesome} hosts have redshifts, found a total of {nredshifts} redshifts' )
            ra = self._mosthosts['ra'][i]
            dec = self._mosthosts['dec'][i]

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
            if len(rows) > 0:
                nhavesome += 1
                nredshifts += len(rows)
                newdf = pandas.DataFrame(rows)
                newdf['snname'] = self._mosthosts.index.get_level_values("snname")[i]
                newdf['index'] = self._mosthosts.index.get_level_values("index")[i]
                if desidf is None:
                    desidf = newdf
                else:
                    desidf = pandas.concat( [ desidf, newdf ] )
        self.logger.info( f'Done {i} of {len(self._mosthosts)} hosts; '
                          f'{nhavesome} hosts have redshifts, found a total of {nredshifts} redshifts' )

        # At the end of this, the index is highly dysfunctional; it will be repeated 0, 1, ... for
        # the inddexes of the dataframes that combined together.  Redo this so that there's an
        # ordinal index.

        desidf = desidf.reset_index().drop( 'level_0', axis=1 )
        
        # We have to muck with datatypes so that they can be nullable, otherwise
        #   integers will get converted to floats and we'll lose information.

        intfields = ( 'tileid', 'petal', 'night', 'targetid' )
        for field in intfields:
            desidf[field] = desidf[field].astype('Int64')

        # Keep only the latest night for a given target/tile/petal

        if latest_night_only:
            prenightcull = len(desidf)
            desidf = desidf.loc[ desidf.groupby( ['targetid', 'tileid', 'petal', 'zwarn'] )['night'].idxmax() ]
            self.logger.info( f'{len(desidf)} of {prenightcull} redshifts left after keeping only latest night' )
            
        # Merge the desi information into mosthosts to make _fulldf
                    
        desidf.set_index( [ 'snname', 'index' ], inplace=True )
        self._fulldf = pandas.merge( self._mosthosts, desidf, how='left',
                                     left_index=True, right_index=True, copy=True )
        self._fulldf.reset_index( inplace=True )

        # _haszdf has all information about redshifts in desi
        
        self._haszdf = self._fulldf[ self._fulldf['z'].notnull() ].copy()
        self._haszdf.set_index( ['snname', 'index', 'targetid', 'tileid', 'petal', 'night'], inplace=True )

        # _df has combined information

        subdf = self._fulldf[ ( self._fulldf['z'].notnull() ) & ( self._fulldf['zwarn'] == 0 ) ]
        def zcomb( row ):
            norm = ( 1. / row['zerr']**2 ).sum()
            row['zdisp'] = row['z'].max() - row['z'].min()
            row['z'] = ( row['z'] / row['zerr']**2 ).sum() / norm
            row['zerr'] = np.sqrt( 1. / norm )
            return row.iloc[0]
        subdf = subdf.groupby( ['snname','index'] ).apply( zcomb )
        subdf = subdf.set_index( ['snname', 'index'] )[ [ 'z', 'zerr', 'zdisp' ] ]

        self._df = pandas.merge( self._mosthosts, subdf, how='left', left_index=True, right_index=True, copy=True )

        self.logger.info( f"Done generating dataframes." )
        dbconn.close()
        
    # ========================================
            
    def find_main_targets( self, force_regen=False ):
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
        dbcols = [ 'survey', 'whenobs', 'targetid', 'desi_target', 'bgs_target', 'mws_target', 'scnd_target' ]
        for col in dbcols:
            columns[col] = []
            
        dbconn = self.connect_to_database()
        cursor = dbconn.cursor()
        did = 0
        for i, row in dfnoindex.iterrows():
            if ( did % 1000 ) == 0:
                self.logger.info( f'Searched DESI targets for {did} of {len(dfnoindex)} mosthosts, '
                                  f'Have {len(columns["survey"])} targets so far' )
            q = ( f"SELECT {','.join(dbcols)} "
                  "FROM general.maintargets WHERE q3c_radial_query(ra,dec,%(ra)s,%(dec)s,1./3600.)" )
            if did == 0:
                self.logger.debug( cursor.mogrify( q, { 'ra': row["ra"], 'dec': row["dec"] } ) )
            cursor.execute( q, { 'ra': row["ra"], 'dec': row["dec"] } )
            results = cursor.fetchall()
            for res in results:
                for col in dfnoindex.columns:
                    columns[col].append( row[col] )
                for col in dbcols:
                    columns[col].append( res[col] )
            did += 1
                    
        self._maintargets = pandas.DataFrame( columns ).set_index( ['spname', 'index', 'survey',
                                                                    'whenobs', 'targetid' ] )
        with open( cachefile, "wb" ) as ofp:
            pickle.dump( self._maintargets, ofp )
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
    
