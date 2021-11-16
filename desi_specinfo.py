# Requires the desi environment

import sys
import pandas
import logging
import numpy as np
import scipy
import scipy.ndimage
import psycopg2
import psycopg2.extras
from astropy.io import fits

import desispec.spectra
import desispec.io
import desispec.coaddition

class TargetNotFound(Exception):
    def __init__(self, message):
        self.message = message
        
    def __str__(self):
        return self.message

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
    
    def __init__( self, ra, dec, desidb=None, desipasswd=None, collection='daily', logger=None ):
        self.ra = ra
        self.dec = dec
        self._targetids = set()
        self._redshiftdata = None
        self._tiledata = None
        
        if logger is None:
            self.logger = logging.getLogger("main")
            ch = logging.StreamHandler(sys.stderr)
            self.logger.addHandler(ch)
            formatter = logging.Formatter(
                f"[%(asctime)s - %(levelname)s] - %(message)s"
            )
            ch.setFormatter(formatter)
            self.logger.setLevel( logging.INFO )
        else:
            self.logger = logger

        mustclose = False
        if desidb is None:
            mustclose = True
            if desipasswd is None:
                raise ValueError( "Must either pass a desidb or a desipasswd" )
            desidb = psycopg2.connect( dbname='desi', host='decatdb.lbl.gov', user='desi', password=desipasswd,
                                       cursor_factory=psycopg2.extras.RealDictCursor )
            
        self.logger.info( f'Looking for {collection} spectra at ({ra:.4f}, {dec:.4f})' )

        cursor = desidb.cursor()
        
        if collection == "everest":
            self.load_everest( cursor, ra, dec )
        elif collection == "daily":
            self.load_daily( cursor, ra, dec )
        else:
            raise ValueError( f"Unknown collection {collection}" )
            
        cursor.close()
        if mustclose:
            desidb.close()
        
    def load_daily( self, cursor, ra, dec ):
        # Lots of redundant code with load_everest ; I should unify
        q = ( "SELECT targetid,tileid,petal_loc,device_loc,location,fiber,night,fiber_ra,fiber_dec "
              "FROM public.fibermap_daily "
              "WHERE q3c_radial_query(fiber_ra,fiber_dec,%(ra)s,%(dec)s,1./3600)" )
        self.logger.debug( f'Running query: {cursor.mogrify( q, {"ra": ra, "dec": dec} )}' )
        cursor.execute( q, {"ra": ra, "dec": dec} )
        result = cursor.fetchall()
        if result is None or len(result) == 0:
            cursor.close()
            raise TargetNotFound( f'Nothing found at ({ra}, {dec})' )
        self._tiledata = pandas.DataFrame( result )
        
        searchlist = []
        for targetid, tileid, night in zip( self._tiledata['targetid'],
                                            self._tiledata['tileid'], self._tiledata['night'] ):
            self._targetids.add( targetid )
            searchlist.append( ( targetid, tileid, night ) ) 
            
        self.logger.debug( f'searchlist is {searchlist}' )
        q = ( "SELECT targetid,tile,yyyymmdd,z,zerr,zwarn,deltachi2 FROM public.zbest_daily "
              "WHERE (targetid,tile,yyyymmdd) IN %(search)s" )
        self.logger.debug( f'Running query: {cursor.mogrify( q, { "search": tuple(searchlist) } )}' )
        cursor.execute( q, { 'search': tuple(searchlist) } )
        result = cursor.fetchall()
        zbest = pandas.DataFrame( result )
        
        filenames = []
        zs = []
        zerrs = []
        zwarns = []
        deltachi2s = []
        # Extremely irritating: pandas is converting all my int64s to float64s
        #   when I do iterrows().  Makes me think this isn't the right 
        #   datastructure.  Work arond it with a big zip.
        for tileid, night, petal_loc, targetid in zip( self._tiledata['tileid'], self._tiledata['night'],
                                                       self._tiledata['petal_loc'], self._tiledata['targetid'] ):
            # See A. Kim's notes; I think this may not be always right.
            filenames.append( f'/global/cfs/cdirs/desi/spectro/redux/daily/tiles/cumulative/'
                              f'{tileid}/{night}/'
                              f'coadd-{petal_loc}-{tileid}-thru{night}.fits' )
            subzbest = zbest[ ( zbest['targetid'] == targetid ) &
                              ( zbest['tile'] == tileid ) &
                              ( zbest['yyyymmdd'] == night ) ]
            if len(subzbest) == 0:
                self.logger.warning( f'Coudn\'t find zbest for target={targetid}, '
                                     f'tile={tileid}, night={night}' )
                zs.append( -9999 )
                zerrs.append( 0 )
                zwarns.append( 0 )
                deltachi2s.append( 0 )
            else:
                if len(subzbest) > 1:
                    self.logger.warning( f'Multiple zbest for target={target}, '
                                         f'tile={tileid}, night={night}'
                                         f'just using the first one the database happened to return.' )
                zs.append( subzbest['z'].iloc[0] )
                zerrs.append( subzbest['zerr'].iloc[0] )
                zwarns.append( subzbest['zwarn'].iloc[0] )
                deltachi2s.append( subzbest['deltachi2'].iloc[0] )
        self._tiledata['filename'] = filenames
        self._tiledata['z'] = zs
        self._tiledata['zerr'] = zerrs
        self._tiledata['zwarn'] = zwarns
        self._tiledata['deltachi2'] = deltachi2s
        self.logger.info( f'Done looking for daily spectra at ({ra:.4f}, {dec:.4f})' )                          
        
    def load_everest( self, cursor, ra, dec ):
        q = ( "SELECT targetid,z,zerr,zwarn,deltachi2,tileid,petal_loc,device_loc "
              "FROM everest.ztile_cumulative_redshifts "
              "WHERE q3c_radial_query(target_ra,target_dec,%(ra)s,%(dec)s,1./3600.)" )
        self.logger.debug( f'Running query: {cursor.mogrify( q, {"ra": ra, "dec": dec} )}' )
        cursor.execute( q, {"ra": ra, "dec": dec} )
        result = cursor.fetchall()
        if result is None or len(result) == 0:
            cursor.close()
            raise TargetNotFound( f'Nothing found at ({ra}, {dec})' )
        self._redshiftdata = pandas.DataFrame( result )

        for targetid in self._redshiftdata['targetid']:
            self._targetids.add(targetid)

        q = ( "SELECT targetid,tileid,MAX(night) as night,petal_loc,device_loc FROM everest.ztile_cumulative_fibermap "
              "WHERE targetid IN %(targetid)s "
              "GROUP BY targetid,tileid,petal_loc,device_loc" )
        self.logger.debug( f'Running query: {cursor.mogrify( q, {"targetid": tuple(self._targetids)} )}' )
        cursor.execute( q, {"targetid": tuple(self._targetids)} )
        result = cursor.fetchall()
        if result is None or len(result) == 0:
            cursor.close()
            raise TargetNotFound( f'Redshifts found, but no tiles found, at ({ra}, {dec})' )
        self._tiledata = pandas.DataFrame( result )

        # Try to merge the redshift data with the tile data; also add filenames
        zs = []
        zerrs = []
        zwarns = []
        deltachi2s = []
        filenames = []
        for i, row in self._tiledata.iterrows():
            filenames.append( f'/global/cfs/cdirs/desi/spectro/redux/everest/tiles/cumulative/'
                              f'{row["tileid"]}/{row["night"]}/'
                              f'coadd-{row["petal_loc"]}-{row["tileid"]}-thru{row["night"]}.fits' )
            subframe = self._redshiftdata[ ( self._redshiftdata['tileid'] == row['tileid'] ) &
                                           ( self._redshiftdata['petal_loc'] == row['petal_loc'] ) &
                                           ( self._redshiftdata['device_loc'] == row['device_loc'] ) ]
            if len(subframe) == 0:
                raise ValueError( f'Can\'t find tileid={row["tileid"]}, petal_loc={row["petal_loc"]}, '
                                  f'device_loc={row["device_loc"]} in redshiftdata!' )
            if len(subframe) > 1:
                self.logger.warning( f'Found multiple things in redshift data with tileid={row["tileid"]}, '
                                     f'petal_loc={row["petal_loc"]}, device_loc={row["device_loc"]}; '
                                     f'just using the first that the database happened to return.' )
            self.logger.debug( f'subframe["z"] = {subframe["z"]}' )
            zs.append( subframe['z'].iloc[0] )
            zerrs.append( subframe['zerr'].iloc[0] )
            zwarns.append( subframe['zwarn'].iloc[0] )
            deltachi2s.append( subframe['deltachi2'].iloc[0] )
        self._tiledata['filename'] = filenames
        self._tiledata['z'] = zs
        self._tiledata['zerr'] = zerrs
        self._tiledata['zwarn'] = zwarns
        self._tiledata['deltachi2'] = deltachi2s
        self.logger.info( f'Done looking for everest spectra at ({ra:.4f}, {dec:.4f})' )
        
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
        for i, row in subframe.iterrows():
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
        with just the singe target's spectrum.  The only element of
        wave, flux, ivar should be 'brz', with the three combined.
        """    

        specinfo = self.info_for_targetid( targetid )
        spectra = []
        for spec in specinfo:
            threespectrums = desispec.io.spectra.read_spectra( spec['filename'] ).select( targets=[targetid] )
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
