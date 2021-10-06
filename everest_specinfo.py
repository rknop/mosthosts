import pandas
import logging

class TargetNotFound(Exception):
    def __init__(self, message):
        self.message = message
        
    def __str__(self):
        return self.message

class SpectrumInfo(object):
    """Make one of these to find all desi everest spectra within 1" of ra/dec.
    
    Then look at targetids to get a set of targetids that have spectra in everest at this ra/dec,
    and run the info_for_targetid(targetid) method to get a list of dicts with info about the spectra.
    """
    
    def __init__( self, ra, dec, desidb, logger=None ):
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

        self.logger.info( f'Looking for everest spectra at ({ra:.4f}, {dec:.4f})' )

        cursor = desidb.cursor()

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
        
        cursor.close()

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
