import os
import sys
import math
import pathlib
import logging
import argparse
import ast
import psycopg2
import psycopg2.extras
import numpy as np
import pandas

# ======================================================================

class MostHostsDesi(object):
    '''Build and hold a Pandas dataframe with info about mosthosts objects with Desi observations.
    
    Build the dataframe by instantiating a MostHostsDesi object; see __init__ for more information.
    
    Access the dataframe with the df property.  Alternatively, the haszdf property just has the
    host entries with at least one DESI redshift with zwarn=0.
    
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
    z — variance-weighted average of the nowarn DESI redshifts for this host, or -9999 if none
    dz — uncertainty on z
    zdisp — Max-min of the individual z values observed for this host
    
    Fields starting with zpix_ are from the zbest_daily and (for targetid) from the fibermap_daily
    tables.  For each of these fields, there are also zpix_nowarn_ fields, which only include the
    observations that had zwarn=0.  All of these fields are *lists* as there may be multiple
    desi observations of the host.
    
    zpix_targetid — List of targetids for desi observations of this host (same targetid may be repeated)
    zpix_z — List of redshifts for desi observations of host
    zpix_zerr — List of redshift errors for desi observations of host
    zpix_zwarn — List of zwarns (0=good) for desi observations of host
    zpix_spectype — List of spectypes for desi observations of host
    zpix_subtype — List of subtypes for desi observations of host
    zpix_deltachi2 — List of deltachi2 for desi observations of host
    
    zpix_nowarn_* — one field corresponding to each zpix_ field.
    '''
    
    @property
    def df( self ):
        return self._df
    
    @property
    def haszdf( self ):
        return self._df[ self._df['zpix_nowarn_targetid'].map(len) > 0 ]
    
    # ========================================
    
    zpix_fields = [ 'targetid', 'z', 'zerr', 'zwarn', 'spectype', 'subtype', 'deltachi2' ]
    
    # ========================================
    
    def __init__( self, release='daily', force_regen=False, logger=None,
                  dbuserpwfile=None, dbuser=None, dbpasswd=None ):
        '''Build and return the a Pandas dataframe with info about Desi observation of mosthosts hosts.
        
        It matches by searching the daily tables by RA/Dec; things within 1" of the mosthosts host 
        coordinate are considered a match.
        
        release — everest or daily

        force_regen — by default, just reads "mosthosts_desi_{release}.csv" from the current directory.
                      This is much faster, as the matching takes some time, but will fall
                      out of date.  Set force_regen to True to force it to rebuild that file
                      from the current contents of the database.  If the .csv file doesn't exist,
                      or doesn't have all the expected columns, it will be regenerated from the
                      database even if force_regen is False.
                      
        dbuserpwfile — A file that has a single line with two words separated by a single space.
                       The first is the username for connecting to the desi database, the second
                       is the password.  This file should be kept somewhere in your account that
                       is *not* world-readable.  Defaults to something in Rob's directory that you
                       can't read....
                       
        dbuser, dbpasswd — Instead of using a dbuserpwfile, you can just pass the database user and
                           password directly.  Be careful!  Don't leave this password sitting around
                           in files that are accessible by people outside of the DESI collaboration!
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

        if force_regen:
            mustregen = True
        else:
            # Try to read the mosthosts_desi_{release}.csv file
            if csvfile.is_file():
                converters = {}
                for field in self.zpix_fields:
                    converters[ f'zpix_{field}' ] = ast.literal_eval
                    converters[ f'zpix_nowarn_{field}' ] = ast.literal_eval
                    converters[ 'dogshit' ] = ast.literal_eval
                self.logger.info( f'Reading {csvfile.name}' )
                self._df = pandas.read_csv( csvfile, converters = converters )
                mustregen = False
                # Verfiy we have the columns we expect
                self.logger.info( f'Checking...' )
                for col in [ 'spname', 'snname', 'ra', 'dec', 'pmra', 'pmdec', 'ref_epoch', 'override', 'index',
                             'hemisphere', 'sn_ra', 'sn_dec', 'sn_z', 'program', 'priority', 'tns_name', 'iau_name',
                             'ptfiptf_name', 'zpix_targetid', 'zpix_nowarn_targetid', 'zpix_z', 'zpix_nowarn_z',
                             'zpix_zerr', 'zpix_nowarn_zerr', 'zpix_zwarn', 'zpix_nowarn_zwarn', 'zpix_spectype',
                             'zpix_nowarn_spectype', 'zpix_subtype', 'zpix_nowarn_subtype', 'zpix_deltachi2',
                             'zpix_nowarn_deltachi2', 'z', 'dz', 'zdisp' ]:
                    if col not in self._df.columns:
                        logger.warning( f'(At least) {col} is missing from {csvfile.name}; regenerating.' )
                        raise Exception( "...not doing that.  Delete the file manually." )
                        mustregen = True
                        break
                if not mustregen:
                    self._df = self._df.set_index( ['spname', 'index' ] )
                    self._df = self._df.sort_index()
                    self.logger.info( f"Read {csvfile.name}" )
            else:
                mustregen = True

        if mustregen:
            self.logger.warning( f"Building {csvfile.name} from database." )
            self.generate_df( release )

        # Subset to objects with at least one redshift measurement
        # Make this when requested
        # self._haszdf = self._df[ self._df['zpix_nowarn_targetid'].map(len) > 0 ]

    # ========================================

    def connect_to_database( self ):
        if self._dbuser is None or self._dbpasswd is None:
            with open( self._dbuserpwfile ) as ifp:
                (self._dbuser,self._dbpasswd) = ifp.readline().strip().split()
                
        dbconn = psycopg2.connect( dbname='desi', host='decatdb.lbl.gov',
                                   user=self._dbuser, password=self._dbpasswd,
                                   cursor_factory=psycopg2.extras.RealDictCursor )
        return dbconn
    
    # ========================================

    def load_mosthosts( self, dbconn ):
        cursor = dbconn.cursor()
        q = "SELECT * FROM mosthosts.mosthosts"
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
            
        # # Add a column that indexes the hosts for that one supernova, and then rearrange the table
        # mosthosts.insert( loc=0, column='snhostnum', value=mosthosts.groupby( 'snname' ).cumcount() )
        mosthosts = mosthosts.set_index( ['snname', 'index' ] )
        mosthosts = mosthosts.sort_index()

        cursor.close()
        return mosthosts
        
        
    # ========================================

    def load_daily_host( self, row, dbconn ):
        cursor = dbconn.cursor()
        q = ( "SELECT f.targetid,f.tileid,f.night,z.z,z.zerr,z.zwarn,z.spectype,z.subtype,z.deltachi2 "
              "FROM public.zbest_daily z "
              "INNER JOIN public.fibermap_daily f ON (f.targetid,f.tileid,f.night)=(z.targetid,z.tile,z.yyyymmdd) "
              "WHERE q3c_radial_query(f.fiber_ra,f.fiber_dec,%s,%s,1./3600)" )
        cursor.execute( q, ( row['ra'], row['dec'] ) )
        matches = pandas.DataFrame( cursor.fetchall() )
        cursor.close()
        return matches
    
    # ========================================

    # def load_everest_host( self, row, dbconn ):
    #     cursor = dbconn.cursor()
    #     q = ( "SELECT z.targetid,z.tileid,z.z,z.zerr,z.zwarn,z.spectype,z.subtype,z.deltachi2 "
    #           "FROM everest.ztile_cumulative_redshifts z "
    #           "WHERE q3c_radial_query(z.target_ra,z.target_dec,%s,%s,1./3600) " )
    #     self.logger.debug( f'Sending query: \"{cursor.mogrify( q, ( row["ra"], row["dec"] ) )}\"' )
    #     cursor.execute( q, ( row["ra"], row["dec"] ) )
    #     matches = pandas.DataFrame( cursor.fetchall() )
    #     if len(matches) == 0:
    #         matches['night'] = []
    #         return matches

    #     nights = []
    #     for i, match in matches.iterrows():
    #         q = ( "SELECT targetid,tileid,MAX(night) AS night "
    #               "FROM everest.ztile_cumulative_fibermap "
    #               "WHERE targetid=%s AND tileid=%s "
    #               "GROUP BY targetid,tileid " )
    #         self.logger.debug( f'Sending query: \"{cursor.mogrify( q, ( match["targetid"], match["tileid"]) )}\"' )
    #         cursor.execute( q, ( match["targetid"], match["tileid"] ) )
    #         tilenights = cursor.fetchall()
    #         if len(tilenights) == 0:
    #             self.logger.error( f'len(tilenights) = 0' )
    #             nights.append("")
    #         else:
    #             if len(tilenights) > 1:
    #                 self.logger.error( f'len(tileights) = {len(tilenights)}' )
    #             tn = tilenights[0]
    #             nights.append( tn['night'] )
    #     matches['night'] = nights

    #     return matches

                              
    def load_everest_host( self, row, dbconn ):
        cursor = dbconn.cursor()
        q = ( "SELECT z.targetid,z.tileid,z.z,z.zerr,z.zwarn,z.spectype,z.subtype,z.deltachi2,MAX(f.night) AS night "
              "FROM everest.ztile_cumulative_redshifts z "
              "INNER JOIN everest.ztile_cumulative_fibermap f "
              "  ON (f.targetid,f.tileid)=(z.targetid,z.tileid) "
              "WHERE q3c_radial_query(z.target_ra,z.target_dec,%s,%s,1./3600) "
              "GROUP BY z.targetid,z.tileid,z.z,z.zerr,z.zwarn,z.spectype,z.subtype,z.deltachi2 "
        )
        self.logger.debug( f'Sending query: \"{cursor.mogrify( q, ( row["ra"], row["dec"] ) )}\"' )
        cursor.execute( q, ( row["ra"], row["dec"] ) )
        matches = pandas.DataFrame( cursor.fetchall() )
        cursor.close()
        return matches

    # ========================================
            
    def generate_df( self, release ):
        if release == 'daily':
            hostloader = self.load_daily_host
        elif release == 'everest':
            hostloader = self.load_everest_host
        else:
            raise ValueError( f'Unknown release {release}' )


        dbconn = self.connect_to_database()
        self.logger.info( "Loading mosthosts table..." )
        mosthosts = self.load_mosthosts( dbconn )
        self.logger.info( "...mosthosts table loaded." )
        
        # Add the fields that will have the desi spectrum info
        
        newfields = {}
        for field in self.zpix_fields:
            newfields[ f'zpix_{field}' ] = []
            newfields[ f'zpix_nowarn_{field}' ] = []
        nhist = np.zeros( 11, dtype=int )
        nhistnowarn = np.zeros( 11, dtype=int )
        for i in range(len(mosthosts)):
            row = mosthosts.iloc[i]
            if (i%1000 == 0):
                self.logger.info( f'Did {i} of {len(mosthosts)}; {i-nhist[0]:d} have at least 1 match' )
            matches = hostloader( row, dbconn )
            if len(matches) == 0:
                for field in self.zpix_fields:
                    newfields[f'zpix_{field}'].append( [] )
                    newfields[f'zpix_nowarn_{field}'].append( [] )
                nhist[0] += 1
                nhistnowarn[0] += 1
                continue

            # NOTE.  Sometimes there are multiple entries in the
            # fibermap_daily table with the same targetid, tileid, and
            # night.  I AM NOT SURE I'M DOING THE RIGHT THING.  Since I
            # load cumulative data in desi_specinfo.py, I think I only
            # want to keep one of these.  (In any event, data kept here
            # will be redundant if I keep more than one.)
            matches = matches.groupby( ['targetid', 'tileid', 'night'] ).aggregate('first').reset_index()

            n = len(matches)
            for field in self.zpix_fields:
                newfields[f'zpix_{field}'].append( [ val for val in matches[field] ] )
                newfields[f'zpix_nowarn_{field}'].append( [ val for val in matches[ matches['zwarn']==0 ][field] ] )
            nnowarn = len(newfields['zpix_nowarn_targetid'][-1])
            if n >= 10:
                nhist[10] += 1
            else:
                nhist[n] += 1
            if nnowarn >= 10:
                nhistnowarn[10] += 1
            else:
                nhistnowarn[nnowarn] += 1

        for field in self.zpix_fields:
            mosthosts[ f'zpix_{field}' ] = newfields[ f'zpix_{field}' ]
            mosthosts[ f'zpix_nowarn_{field}' ] = newfields[ f'zpix_nowarn_{field}' ]

        dbconn.close()

        # Combine redshifts together

        def combinez( row ):
            if len(row['zpix_nowarn_z']) == 0:
                return { 'z': -9999, 'dz': 0, 'zdisp': 0 }
            hostzs = np.array( row['zpix_nowarn_z'] )
            hostzerrs = np.array( row['zpix_nowarn_zerr'] )
            z = ( hostzs / hostzerrs**2 ).sum() / ( 1. / hostzerrs**2 ).sum()
            dz = 1. / math.sqrt( (1. / hostzerrs**2).sum() )
            return { 'z': z, 'dz': dz, 'zdisp': hostzs.max()-hostzs.min() }

        hostzvals = mosthosts.apply( combinez, axis=1, result_type='expand' )

        self._df = mosthosts.merge( hostzvals,
                                    how='left', left_index=True, right_index=True )

        cwd = pathlib.Path( os.getcwd() )
        csvfile = cwd / f"mosthosts_desi_{release}.csv"
        self._df.to_csv( csvfile )

        self.logger.warning( f'File {csvfile.name} written (I hope).' )

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
    
