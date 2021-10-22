import pandas
import logging
import numpy as np
import scipy
import scipy.ndimage
from astropy.io import fits

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
    
    def __init__( self, ra, dec, desidb, collection='everest', logger=None ):
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

        self.logger.info( f'Looking for {collection} spectra at ({ra:.4f}, {dec:.4f})' )

        cursor = desidb.cursor()
        
        if collection == "everest":
            self.load_everest( cursor, ra, dec )
        elif collection == "daily":
            self.load_daily( cursor, ra, dec )
        else:
            raise ValueError( f"Unknown collection {collection}" )
            
        cursor.close()
        
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
        for taregtid, tileid, night in zip( self._tiledata['targetid'], self._tiledata['tileid'], self._tiledata['night'] ):
            self._targetid.add( targetid )
            searchlist.append( ( targetid, tileid, night ) ) 
            
        q = ( "SELECT targetid,tile,yyyymmdd,z,zerr,zwarn,deltachi2 FROM public.zbest_daily "
              "WHERE (targetid,tile,yyyymmdd) IN %s" )
        self.logger.debug( f'Running query: {cursor.mogrify( q, searchlist )}' )
        cursor.execute( q, ( searchlist, ) )
        result = cursor.fetchall()
        zbest = pandas.DataFrame( result )
        
        zs = []
        zerrs = []
        zwarns = []
        deltachi2s = []
        for i, row in self._tiledata.iterrows():
            # See A. Kim's notes; I think this may not be always right.
            filenames.append( f'/global/cfs/cdirs/desi/spectro/redux/daily/tiles/cumulative/'
                              f'row["tileid"]/row["night"]/'
                              f'coadd-{row["petal_loc"]}-{row["tileid"]}-thru{row["night"]}.fits' )
            subzbest = zbest[ ( zbest['targetid'] == row['targetid'] ) &
                              ( zbest['tile'] == row['tileid'] ) &
                              ( zbest['yyyymmdd'] == row['night'] ) ]
            if len(subzbest) == 0:
                self.logger.warning( f'Coudn\'t find zbest for target={row["targetid"]}, '
                                     f'tile={row["tileid"]}, night={row["night"]}' )
                zs.append( -9999 )
                zerrs.append( 0 )
                zwarns.append( 0 )
                deltachi2s.append( 0 )
            else:
                if len(subzbest) > 1:
                    self.logger.warning( f'Multiple zbest for target={row["targetid"]}, '
                                         f'tile={row["tileid"]}, night={row["night"]}'
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
        """Return a list of targetids that have spectra in everest"""
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
        
        It returns a list because there might be multiple spectra for the same targetid.
        
        smooth is the sigma in pixels to gaussian smooth the spectra.  (0 = no smoothing)
        
        Each element of the list is a dictionary, with fields:
            B_wavelength
            B_flux
            B_dflux
            R_wavelength
            R_flux
            R_dflux
            Z_wavelength
            Z_flux
            Z_dflux
            info
        """    

        specinfo = self.info_for_targetid( targetid )
        spectra = []
        for spec in specinfo:
            spectrum = { 'info': spec }
            with fits.open(spec["filename"]) as hdulist:
                w = np.where( hdulist['FIBERMAP'].data['DEVICE_LOC'] == spec["device_loc"] )[0]
                if len(w) != 1:
                    raise Exception( f'Found {len(w)} things in the fibermap with DEVICE_LOC={spec["device_loc"]}' )
                index = w[0]
                for λrange in ['B','R','Z']:
                    spectrum[f'{λrange}_wavelength'] = hdulist[f'{λrange}_WAVELENGTH'].data
                    flux = hdulist[f'{λrange}_FLUX'].data[index]
                    dflux = 1./np.sqrt( hdulist[f'{λrange}_IVAR'].data[index] )
                    if smooth > 0:
                        flux = scipy.ndimage.gaussian_filter1d( flux, smooth )
                        dflux = scipy.ndimage.gaussian_filter1d( dflux, smooth )
                    spectrum[f'{λrange}_flux'] = flux
                    spectrum[f'{λrange}_dflux'] = dflux
            spectra.append( spectrum )
        return spectra
