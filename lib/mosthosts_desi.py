import os
import sys
import math
import pathlib
import pickle
import logging
import argparse
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
    
    MOSTHOSTS TABLE:

    The basic mosthosts table (without any desi observations) is in the mosthosts property.

    ALL MOSTHOSTS DATAFRAME WITH REDSHIFTS:
    
    Access the dataframe with the df property.  It has one row for each host in the Mosthosts table.
    
    DATAFRAME OF DESI OBSERVATIONS OF HOSTS

    Access the dataframe with the haszdf property.  It potentially has
    multiple rows for a given host from the Mosthosts table, but only
    includes hosts that have at least one DESI observation.

    DATAFRAME OF DESI MAIN TARGETS OF MOSTHOSTS HOSTS

    Access this with the maintargets property.

    '''

    _mosthosts_table = 'mosthosts'
    
    @property
    def mosthosts( self ):
        """The MostHosts table as a pandas dataframe.

        Indexed by (sn_name_sp, hostnum).

        Has lots of columns
        """
        return self._mosthosts
    
    @property
    def df( self ):
        """A pandas dataframe with summary information about desi observations of MostHosts hosts.

        Indexed by (sn_name_sp, hostnum) ; these indexes match what's in
        the mosthosts property.

        Has columns:

        ra: ra of host
        dec: dec of host
        sn_ra: ra of sn
        sn_dec: dec of sn
        sn_z: redshift of sn
        z: variance-weighted average of the nowarn DESI redshifts for this host, or null if none
        dz: uncertainty on z (estimated somehow)
        zdisp: Max-min of the individual z values observed for this host

        If you want to drill into where this z came from, look at the
        haszdf table.  Only redshifts with zwarn=0 are included in the
        calculation of z, dz, and zdisp for this table.

        """
        return self._df
    
    @property
    def haszdf( self ):
        """A pandas dataframe summarizing all DESI observations of mosthosts targets.

        Built from the coadded spectra, so it won't have each and every
        separate observation,but it will separately lists different
        observations of the same host with different tile and/or target
        IDs.  (That is: DESI sometimes puts the same RA/DEC into
        different targets and tiles.  This dataframe doesn't combine
        them.  However, it read the DESI cumulative files, so multiple
        observations tagged with the same tile and target for a given
        host had their spectra combined before the redshifts was
        measured.)  MostHosts that don't have DESI observations won't
        appear in this table.

        This dataframe has *six* indices

        sn_name_sp : same as from mosthosts and df
        hostnum : same as from mosthosts and df
        targetid : targetid from the DESI database
        tileid : tileid from the DESI database
        petal : petal from the DESI database
        night : night from the DESI database

        Columns include:

        ra: ra of host
        dec: dec of host
        sn_ra: ra of sn
        sn_dec: dec of sn
        sn_z: redshift of sn
        z: redshift for this observation
        zerr: undertainty on redshift for this observation
        zwarn: If this isn't0, you probably shouldn't trust the redshift
        chi2:
        deltachi2:
        spectype:
        subtype:

        """
        return self._haszdf

    @property
    def maintargets( self ):
        """Pandas dataframe of DESI main targets for MostHosts hosts.

        Access this with the maintargets property.  By default, it will
        read the "mosthosts_desi_maintargets.pkl" file if that exists.
        If you want to rebuild this file, call the find_main_targets()
        method with force_regen=True.

        The dataframe has *five* indices, because it's possible that the
        same MH target will show up more than once as a DESI target.

        sn_name_sp: same as from df
        hostnum: same as from df
        survey: one of "main", "sv1", "sv2", "sv3", or "backup"
          NOTE --- CURRENTLY ONLY SEARCHES THE MAIN SURVEY, so this will always be "main"
        whenobs: one of "bright" or "dark"
        targetid: DESI target id.
        desi_target: not sure why this isn't the same as targetid, but whatever
        bgs_target: I *think* 0 means not in BGS
        mws_target: (Same 0 interp)
        scnd_target:

        """
        if self._maintargets is None:
            self.find_main_targets()
        return self._maintargets

    
    # ========================================
    
    def __init__( self, release='daily', force_regen=False, latest_night_only=True, logger=None,
                  dbuserpwfile=None, dbuser=None, dbpasswd=None ):
        '''Build Pandas dataframes with info about Desi observation of mosthosts hosts.
        
        It matches by searching the daily tables by RA/Dec; things
        within 1" of the mosthosts host coordinate are considered a
        match.
        
        release — One of daily, everest, fuji, guadalupe, fujilupe, or
                  iron.  The DESI release to get info for.  "Daily"
                  looks at the regularly updated databse of what's been
                  done.  fujilupe is a special case that combines
                  together fuji and guadalipe into a single dataframe.

        force_regen — by default, just reads
                      "mosthosts_desi_{release}.pkl" and
                      "mosthosts_desi_{release}_desiobs.pkl" from the
                      current directory.  This is much faster, as
                      matching mosthosts to desi observations is slow
                      (the regen can take 10-20 minutes), but will fall
                      out of date.  Set force_regen to True to force it
                      to rebuild those files (and .csv files with the
                      same information) from the current contents of the
                      database.  If both .pkl and .csv files don't
                      exist, they will be regenerated from the database
                      even if force_regen is False.  Ideally, for
                      releases like iron, you won't need to use
                      force_regen=True, but you might want to if looking
                      at the daily spectra.

                      WARNING : it doesn't check to make sure that the
                      structure of the tables is current when loading
                      the .pkl files.  If you've recently updated this
                      code, run at least once with force_regen=True.
                      
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
                       world-readable.  Defaults to
                       "secrets/decatdb_desi_desi" underneath your home
                       directory.
                       
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
            self._dbuserpwfile = pathlib.Path( os.getenv("HOME") ) / "secrets/decatdb_desi_desi"
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
                self._df = pandas.read_pickle( pklfile )
                self._haszdf = pandas.read_pickle( haszpklfile )
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
        if ( self._dbuser is None ) != ( self._dbpasswd is None ):
            raise ValueError( "Both or neither of dbuser and dbpasswd must be specified, not just one." )
        if self._dbuser is None or self._dbpasswd is None:
            with open( self._dbuserpwfile ) as ifp:
                (self._dbuser,self._dbpasswd) = ifp.readline().strip().split()
                
        dbconn = psycopg2.connect( dbname='desidb', host='decatdb.lbl.gov',
                                   user=self._dbuser, password=self._dbpasswd,
                                   cursor_factory=psycopg2.extras.RealDictCursor )
        return dbconn
    
    # =============================================
    
    # @staticmethod
    
    # # Add the spname column
    # def get_spname( row ):
    #     if row['tns_name'] != 'None':
    #         return row['tns_name']
    #     elif row['iau_name'] != 'None':
    #         return row['iau_name']
    #     elif row['ptfiptf_name'] != 'None':
    #         return row['ptfiptf_name']
    #     else:
    #         return row['snname']
            
    # ========================================

    def load_mosthosts( self, dbconn ):
        cursor = dbconn.cursor()
        q = f"SELECT * FROM static.{self._mosthosts_table}"
        cursor.execute( q )
        mosthosts = pandas.DataFrame( cursor.fetchall() )

        # Change type of hostnum to pandas Int64 so that it can be nullable
        mosthosts['hostnum'] = mosthosts['hostnum'].astype('Int64')

        mosthosts.set_index( ['sn_name_sp', 'hostnum' ], inplace=True )
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

        mosthosts_subset = self.mosthosts[ [ 'ra','dec', 'sn_ra', 'sn_dec', 'sn_z' ] ]
        
        if release == "fujilupe":
            raise RuntimeError( "Fujilupe hack not implemented." )
        
        self.logger.info( f'Rebuilding info for release {release}' )
        
        dbconn = self.connect_to_database()
        desidf = None

        self._maintargets = None
        nhavesome = 0
        nredshifts = 0

        cursor = dbconn.cursor()

        # First, build a temporary table matching targetid/tile/petal to mosthosts
        self.logger.info( f'Sending q3c_join query for release {release}' )
        query = ( f"SELECT m.sn_name_sp,m.hostnum,f.targetid,f.tileid,f.petal_loc "
                  f"INTO TEMP TABLE temp_mosthosts_search1 "
                  f"FROM static.{self._mosthosts_table} m "
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
        query = ( f"SELECT m.sn_name_sp,m.hostnum,m.targetid,m.tileid,m.petal_loc,c.night,"
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
        desidf.set_index( ['sn_name_sp', 'hostnum', 'targetid', 'tileid', 'petal', 'night' ], inplace=True )
        self._haszdf = mosthosts_subset.join( desidf, how="inner" )

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
        combdf = subdf.reset_index().groupby( ['sn_name_sp','hostnum'] ).apply( zcomb )
        combdf = combdf.set_index( ['sn_name_sp', 'hostnum'] )[ [ 'z', 'zerr', 'zdisp' ] ]
        
        self._df = mosthosts_subset.join( combdf, how='left' )
        
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
        dbcols = [ 'm.sn_name_sp', 'm.hostnum', 'm.sn_name_tns', 'm.sn_name_iau', 'm.sn_name_ptf', 
                   't.survey', 't.whenobs', 't.targetid',
                   't.desi_target', 't.bgs_target', 't.mws_target', 't.scnd_target' ]
        for col in dbcols:
            columns[col] = []
            
        dbconn = self.connect_to_database()
        cursor = dbconn.cursor()
        
        self.logger.info( f'Searching DESI targets for mosthosts' )
        q = ( f"SELECT {','.join(dbcols)} "
              f"FROM static.{self._mosthosts_table} m "
              f"INNER JOIN general.maintargets t ON q3c_join(m.ra,m.dec,t.ra,t.dec,%(radius)s)" )
        self.logger.debug( f"Sending query: {cursor.mogrify( q, { 'radius': radius } )}" )
        cursor.execute( q, { 'radius': radius } )
        rows = cursor.fetchall()
        
        self._maintargets = pandas.DataFrame( rows )
        self._maintargets.set_index( ['sn_name_sp', 'hostnum', 'survey', 'whenobs', 'targetid'], inplace=True )

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
    
