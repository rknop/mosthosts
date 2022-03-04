# Requires the desi environment

import sys
import os
import re
import pathlib
import pandas
import logging
import numpy as np
import scipy
import scipy.ndimage
import psycopg2
import psycopg2.extras
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
_desispecinfologger.setLevel( logging.INFO )

# ======================================================================

class TargetNotFound(Exception):
    def __init__(self, message):
        self.message = message
        
    def __str__(self):
        return self.message

# ======================================================================
    
class SpectrumInfo(object):
    """Make one of these to find all desi spectra within 1" of ra/dec.
    
    Pass collection = "everest" or "daily"
    
    Then look at targetids to get a set of targetids that have spectra in everest at this ra/dec,
    and run the info_for_targetid(targetid) method to get a list of dicts with info about the spectra.

    Run info_for_targetid(targetid) to get back redshifts, tiles, nights, etc. for that targetid.
    
    Run get_spectra(targetid) to get back a list of spectra for that targetid.
    
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
    
    def __init__( self, ra, dec, desidb=None, desipasswd=None, collection='daily', logger=None ):
        global _desispecinfologger
        
        self.ra = ra
        self.dec = dec
        self._targetids = set()
        self._redshiftdata = None
        self._tiledata = None
        self.logger = _desispecinfologger if logger is None else logger

        mustclose = False
        if desidb is None:
            mustclose = True
            if desipasswd is None:
                raise ValueError( "Must either pass a desidb or a desipasswd" )
            desidb = psycopg2.connect( dbname='desidb', host='decatdb.lbl.gov', user='desi', password=desipasswd,
                                       cursor_factory=psycopg2.extras.RealDictCursor )
            
        self.logger.info( f'Looking for {collection} spectra at ({ra:.4f}, {dec:.4f})' )
        self.load_dbinfo( collection, desidb, ra, dec )

        if mustclose:
            desidb.close()


    def load_dbinfo( self, release, desidb, ra, dec ):
        cursor = desidb.cursor()
        q = ( f"SELECT c.filename,f.targetid,f.tileid,f.petal_loc,f.device_loc,"
              f"  c.night,f.fiber,f.mean_fiber_ra,f.mean_fiber_dec,f.target_ra,f.target_dec,"
              f"  z.z,z.zerr,z.zwarn,z.chi2,z.deltachi2,z.spectype,z.subtype "
              f"FROM {release}.tiles_fibermap f "
              f"INNER JOIN {release}.cumulative_tiles c ON c.id=f.cumultile_id "
              f"INNER JOIN {release}.tiles_redshifts z ON "
              f"   ( z.cumultile_id=f.cumultile_id AND "
              f"     z.targetid=f.targetid ) "
              f"WHERE q3c_radial_query(f.target_ra,f.target_dec,%(ra)s,%(dec)s,1./3600.)" )
        self.logger.debug( f'Running query: {cursor.mogrify( q, {"ra": ra, "dec": dec} )}' )
        cursor.execute( q, {"ra": ra, "dec": dec} )
        result = cursor.fetchall()
        if result is None or len(result) == 0:
            cursor.close()
            raise TargetNotFound( f'Nothing found at ({ra}, {dec})' )
        self._tiledata = pandas.DataFrame( result )

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
        cursor.close()

    @property
    def targetids( self ):
        """A set of targetids that have spectra in everest"""
        return self._targetids
    
    def info_for_targetid( self, targetid ):
        """Returns a list of dicts.
        
        One element in the list for each spectrum found in everest for that targetid.
        Each dict has z, zwar, deltachi2, filename, tileid, petal_loc, device_loc, night

        """
        subframe = self._tiledata[ self._tiledata['targetid'] == targetid ]
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
            match = self.nameparse.search( spec['filename'] )
            if match is None:
                raise ValueError( f'Error parsing filename {spec["filename"]}' )
            filepath = self.BASE_DIR / match.group(1) / f"coadd{match.group(3)}"
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
            spectra.append( spectrum )
        return spectra
