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

# ======================================================================

class MostHostsDesi(object):
    '''Build and hold a two different Pandas dataframes with info about mosthosts objects with Desi observations.
    
    Build the dataframe by instantiating a MostHostsDesi object; see __init__ for more information.

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

    '''
    
    @property
    def df( self ):
        return self._df
    
    @property
    def haszdf( self ):
        return self._haszdf
    
    # ========================================
    
    def __init__( self, release='daily', force_regen=False, logger=None,
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

        if logger is None:
            self.logger = logging.getLogger( "mhd" )
            logout = logging.StreamHandler( sys.stderr )
            self.logger.addHandler( logout )
            logout.setFormatter( logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s' ) )
            self.logger.setLevel( logging.INFO )
            # self.logger.setLevel( logging.DEBUG )
        else:
            self.logger = logger

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
            # self.logger.warning( f"Building {csvfile.name} and {haszcsvfile.name} from database." )

            # if release == "fujilupe":
            #     self.logger.info( "=== Loading Fuji Information ===" )
            #     self.generate_df( "fuji" )
            #     _fujifulldf = self._fulldf
            #     _fujihaszdf = self._haszdf
            #     _fujidf = self._df
            #     self.logger.info( "=== Loading Guadalupe Information ===" )
            #     self.generate_df( "guadalupe" )
            #     self.logger.info( "=== Merging Fuji and Guadalupe ===" )
            #     # ****
            #     self._fujifulldf = _fujifulldf
            #     self._fujihaszdf = _fujihaszdf
            #     self._fujidf = _fujidf
            #     self._guadalupefulldf = self._fulldf
            #     self._guadalupehaszdf = self._haszdf.copy()
            #     self._guadalupedf = self._df.copy()
            #     # ****
            #     self._fulldf = None
            #     intersec = _fujihaszdf.index.intersection( self._haszdf.index )
            #     if len(intersec) > 0:
            #         self.logger.error( f"Repeated indices in fuji and guadalupe: {intersec}" )
            #     self._haszdf = pandas.concat( [ _fujihaszdf, self._haszdf ] )
            #     # This one is more complicated;
            #     #  I haven't figured out if there's a pandas merge function
            #     #  that does what I want here.
            #     # I know that the _df dataframes will have exactly the same
            #     #  rows in the same order.
            #     for i in range(len(_fujidf)):
            #         if pandas.isna( self._df.iloc[i].z ):
            #             if not pandas.isna( _fujidf.iloc[i].z ):
            #                 dex = self._df.iloc[i].name
            #                 # self.logger.debug( f'Importing {dex} from fuji' )
            #                 # Pandas is loaded with lots of non-intuitive stuff
            #                 # self._df.iloc[i].z = ... didn't work, had to use "at"
            #                 self._df.at[dex, 'z'] = _fujidf.iloc[i].z
            #                 self._df.at[dex, 'zdisp'] = _fujidf.iloc[i].zdisp
            #                 self._df.at[dex, 'zerr'] = _fujidf.iloc[i].zerr
            #         else:
            #             if not pandas.isna( _fujidf.iloc[i].z ):
            #                 self.logger.warning( f'Row {self._df.iloc[i].name} has redshifts from both '
            #                                      f'fuji and guadalupe; using just the latter in df' )
            # else:
            self.generate_df( release )

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
            
    def generate_df( self, release ):
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

        # We have to muck with datatypes so that they can be nullable, otherwise
        #   integers will get converted to floats and we'll lose information.

        intfields = ( 'tileid', 'petal', 'night', 'targetid' )
        for field in intfields:
            desidf[field] = desidf[field].astype('Int64')

        # Merge the dsi information into mosthosxts to make _fulldf
                    
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
    
