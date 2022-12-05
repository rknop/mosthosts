# Requires the desi environment

import sys
import os
import re
import pathlib
import pandas
import logging
import numpy
import scipy
import scipy.ndimage
import psycopg2
import psycopg2.extras
import sqlalchemy as sa
from astropy.io import fits
from astropy.coordinates import SkyCoord

import desispec.spectra
import desispec.io
import desispec.coaddition


_desispecinfologger = logging.getLogger("desi_specinfo")
_logerr = logging.StreamHandler( sys.stderr )
_desispecinfologger.addHandler( _logerr )
_formatter = logging.Formatter( f"[%(asctime)s - %(levelname)s] - %(message)s" )
_logerr.setFormatter( _formatter )
# _desispecinfologger.setLevel( logging.INFO )
_desispecinfologger.setLevel( logging.DEBUG )

# ======================================================================

class TargetNotFound(Exception):
    def __init__(self, message):
        self.message = message
        
    def __str__(self):
        return self.message

# ======================================================================
    
class SpectrumFinder(object):
    """Make one of these to find all desi spectra within 1" of a list of ra/dec
    
    Pass collection = daily, everest, fuji, or guadalupe  ("fujilupe" isn't currently supported)
    
    The _tiledata variable is not intended to be accessed remotely.  I want it to have the
    following columns:
       targetid
       tileid
       petal_loc
       night
       device_loc
       z
       zerr
       zwarn
       deltachi2
       filename
    """

    BASE_DIR = pathlib.Path( "/global/cfs/cdirs/desi/spectro/redux" ) 
    nameparse = re.compile('^(.*)/(zbest|redrock)(-[0-9]-[0-9]{2,6}-thru[0-9]{8}.fits)$' )
    
    def __init__( self, ras, decs, radius=1./3600.,
                  names=None, desipasswd=None, collection='daily', logger=None ):
        global _desispecinfologger

        # This is mostly to avoid SQL injection attacks...
        if collection not in { 'daily', 'everest', 'fuji', 'guadalupe' }:
            raise ValueError( f'Unknown collection "{collection}"' )
        
        self.logger = _desispecinfologger if logger is None else logger
        self.radius = radius
        self.collection = collection
        self.ras = [ras] if numpy.isscalar(ras) else ras
        self.decs = [decs] if numpy.isscalar(decs) else decs
        self.names = list(range(len(self.ras))) if names is None else names

        self.inputdf = pandas.DataFrame( { 'name': self.names, 'ra': self.ras, 'dec': self.decs } )
        
        # mustclose = False
        # if desidb is None:
        #     mustclose = True
        #     if desipasswd is None:
        #         raise ValueError( "Must either pass a desidb or a desipasswd" )
        #     desidb = psycopg2.connect( dbname='desidb', host='decatdb.lbl.gov', user='desi', password=desipasswd,
        #                                cursor_factory=psycopg2.extras.RealDictCursor )

        # self.engine = sa.create_engine( 'postgresql+psycopg2://', poolclass=sa.pool.StaticPool,
        #                                 creator=lambda: desidb )
        self.engine = sa.create_engine( f'postgresql+psycopg2://desi:{desipasswd}@decatdb.lbl.gov:5432/desidb' )
        
        self.logger.info( f'Looking for {collection} spectra at {len(self.inputdf)} positions '
                          f'w/in {self.radius}°.)' )
        self.logger.debug( f'Search table:\n{self.inputdf}' )
        self._load_dbinfo()

    def _load_dbinfo( self ):
        with self.engine.connect() as conn:
            conn.execute( sa.sql.text( "CREATE TEMPORARY TABLE spectrumfinder_searchspec "
                                       "( name text, ra double precision, dec double precision )" ) )
            conn.execute( sa.sql.text( "CREATE INDEX ON spectrumfinder_searchspec (q3c_ang2ipix(ra,dec))" ) )

            self.logger.debug( "Filling temporary table..." )
            self.inputdf.to_sql( 'spectrumfinder_searchspec', con=conn, if_exists='append', index=False )
            self.logger.debug( "...filled." )

            q = sa.sql.text( f"SELECT f.targetid,f.tileid,f.petal_loc,f.device_loc,f.fiber,"
                             f"           f.mean_fiber_ra,f.mean_fiber_dec,f.target_ra,f.target_dec,"
                             f"           f.cumultile_id,ss.name "
                             f"INTO TEMPORARY spectrumfinder_matches "
                             f"FROM {self.collection}.tiles_fibermap f, spectrumfinder_searchspec ss "
                             f"WHERE q3c_join( ss.ra, ss.dec, f.target_ra, f.target_dec, :radius)" )
            self.logger.debug( "Filling second temporary table..." )
            conn.execute( q, { "radius": self.radius } )
            self.logger.debug( "...filled" )
            
            q = sa.sql.text( f"SELECT c.filename,sq.targetid,sq.tileid,sq.petal_loc,sq.device_loc,"
                             f"    c.night,sq.fiber,sq.mean_fiber_ra,sq.mean_fiber_dec,sq.target_ra,sq.target_dec,"
                             f"    z.z,z.zerr,z.zwarn,z.chi2,z.deltachi2,z.spectype,z.subtype,sq.name "
                             f"  FROM spectrumfinder_matches sq "
                             f"  INNER JOIN {self.collection}.cumulative_tiles c ON c.id=sq.cumultile_id "
                             f"  INNER JOIN {self.collection}.tiles_redshifts z ON "
                             f"     ( z.cumultile_id=sq.cumultile_id AND "
                             f"       z.targetid=sq.targetid ) " )
            self._tiledata = pandas.read_sql( q, conn, params={ "radius": self.radius } )
            # rows = [ row for row in rows ]
            # self.logger.debug( f"Returned:\n{rows}" )
            # raise Exception("Hello.")
            # self._tiledata = pandas.DataFrame( rows )

        if len( self._tiledata ) == 0:
            raise TargetNotFound( f'Nothing found' )

        # I don't *think* that the same target/tile/petal_loc/night
        # should show up more than once, as these are from cumulative
        # tiles files.  Really, we just want to use the *latest* night
        # for any given target/tile/petal_loc, as they will have summed
        # in the earlier nights.  (This should only come up in daily;
        # for an actual release, a given target/tile/petal should only
        # appear with a single night in cumulative.)

        self._tiledata = self._tiledata.loc[ self._tiledata.groupby( [ 'targetid', 'tileid', 'petal_loc' ] )
                                             ['night'].idxmax() ]
        self._targetids = set( self._tiledata.targetid.values )
        self._tiledata.set_index( [ 'targetid', 'tileid', 'petal_loc', 'night' ], inplace=True )

    @property
    def targetids( self ):
        """A set of targetids that have spectra in everest"""
        return self._targetids
    
    def targetids_for_name( self, name ):
        tmpdf = self._tiledata.reset_index()
        tmpdf = tmpdf[ tmpdf['name'] == name ]
        return set( tmpdf['targetid'].values )

    def info_for_targetid( self, targetid ):
        """Returns a list of dicts.
        
        One element in the list for each spectrum found in everest for that targetid.
        Each dict has z, zwarn, deltachi2, filename, tileid, petal_loc, device_loc, night

        """
        subframe = self._tiledata.xs( targetid, level=0 ).reset_index()
        retval = []
        for tup in subframe.itertuples():
            row = tup._asdict()
            spectrum = {}
            for field in ( ['z', 'zerr', 'zwarn', 'deltachi2', 'filename', 
                            'tileid', 'petal_loc', 'device_loc', 'night' ] ):
                spectrum[field] = row[field]
            retval.append( spectrum )
        return retval

    def get_spectra( self, targetid, smooth=0 ):
        """Returns a list of spectra for the given targetid.
        
        Order of the list corresponds to the order you get from info_for_targetid.

        It returns a list because there might be multiple spectra for the same targetid.

        Each element of the list is a desispec.spectra.Spectra object
        with just the single target's spectrum.  The only element of
        wave, flux, ivar should be 'brz', with the three combined.

        Warning: if you use smooth other than 0, the spectrum variance
        isn't really right (because of correlated errors; see comments
        in code).  smooth=0 is always safer.

        """    

        specinfo = self.info_for_targetid( targetid )
        spectra = []
        for spec in specinfo:
            spectra.append( self.get_spectrum( targetid, spec['tileid'], spec['petal_loc'], spec['night'] ) )
        return spectra

    def filepath( self, targetid, tile, petal, night ):
        """Return path (pathlib.Path object) of the coadd file for specified targetid, tile, petal, night."""

        try:
            row = self._tiledata.loc[ targetid, tile, petal, night ]
        except KeyError as e:
            # import pdb; pdb.set_trace()
            raise TargetNotFound( f'No spectrum for target {targetid}, tile {tile}, petal {petal}, night {night}' )

        match = self.nameparse.search( row["filename"] )
        if match is None:
            raise ValueError( f'Error parsing filename {spec["filename"]}' )
        return self.BASE_DIR / match.group(1) / f"coadd{match.group(3)}"
    
    def get_spectrum( self, targetid, tile, petal, night, smooth=0 ):
        """Returns a desispec.spectra.Spectra object for the specified target/tile/petal/night.
        
        You get the coadded cumulative spectrum.

        The only element of wave, flux, ivar should be 'brz', with the
        three combined.

        Warning: if you use smooth other than 0, the spectrum variance
        isn't really right (because of correlated errors; see comments
        in code).  smooth=0 is always safer.

        """
        filepath = self.filepath( targetid, tile, petal, night )
        if not filepath.is_file():
            raise FileNotFoundError( f'File {spec["filename"]} doesn\'t exist' )
        threespectrums = desispec.io.spectra.read_spectra( filepath ).select( targets=[targetid] )
        # Combine B, R, Z into brz
        spectrum = desispec.coaddition.coadd_cameras( threespectrums )

        if smooth > 0:
            spectrum.flux['brz'][0,:] = scipy.ndimage.gaussian_filter1d( spectrum.flux['brz'][0,:], smooth )
            # This isn't the right thing to do!  Two problems
            # 1. correlated errors.  This is thorny, and requires new data structures
            # 2. Really, var[i,:] = sum( k(n)² var(i+n), n=-N..N )
            #       where k(n) is the kernel that has half-width N
            #    ...but this is only of limited meaning because of course the correlated
            #    errors are huge.
            # On the other hand, this WILL give you a feeling for the noise level
            # in the original spectrum, which is maybe what we want.
            spectrum.ivar['brz'][0,:] = scipy.ndimage.gaussian_filter1d( spectrum.ivar['brz'][0,:], smooth )
        
        return spectrum
