import sys
import logging
import re
import requests
import json
import time
# import html2text
import pandas

_logger = logging.getLogger( "mosthosts_skyportal" )
_logerr = logging.StreamHandler( sys.stderr )
_logger.addHandler( _logerr )
_logerr.setFormatter( logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s' ) )
_logger.setLevel( logging.INFO )

# ======================================================================

class SpExc(Exception):
    def __init__( self, status, info, message, data=None ):
        self.status = status
        self.info = info
        self.message = message
        self.data = data

    def __str__( self ):
        return f'Status {status}: {info} ("{message}")'

# ======================================================================
    
class MostHostsSkyPortal:
    """Encapsulate info about what MostHosts candidates are on SkyPortal.

    Instantiate one of these.  The df property is a Pandas dataframe
    with useful information.  In addition, the sp_req and sp_req_data
    methods can be used to further query skyportal (with some error
    return checking built in.

    The df dataframe is by default cached in "skyportalcache.pkl" in the
    local directory.  Of course, this means that the data you have might
    be out of date from what's on the SkyPortal.  Call the instance's
    generate_df method with regen=True to force this file to be
    regenerated.  (Takes a little while — several minutes, as of Feb
    2022 when there are ~15k candidates in MostHosts.)

    The dataframe is indexed by "id" (corresponds to object id on
    Skyportal and spname in the MostHostsDesi object from
    mosthosts_des.py).  It (probably) has columns (defined by SkyPortal):

        ra
        dec
        ra_err
        dec_err
        ra_dis
        dec_dis
        redshift
        redshift_error
        redshift_history
        gal_lon
        gal_lat
        luminosity_distance
        dm
        angular_diameter_distance
        alias
        classifications
        annotations
        transient
        varstar
        is_roid
        score
        altdata
        origin
        dist_nearest_source
        mag_nearest_source
        e_mag_nearest_source
        detect_photometry_count
        spectrum_exists
        created_at
        modified
        internal_key
        offset
        groups

    Once you have this dataframe loaded, you can call the
    spectra_for_obj(objid) method of your MostHostsSkyPortal instance to
    get all the spectra for the object with id objid (which is the index
    of pandas table found in the df property of your instance).

    """
    
    mosthosts_group_id = 36

    def __init__( self, url="https://desi-skyportal.lbl.gov", token=None, logger=None ):
        if token is None:
            raise Exception( "API token required" )
        self._spapi = f'{url}/api'
        self._token = token
        self._df = None
        self.logger = _logger if logger is None else logger
        
    @property
    def df( self ):
        if self._df is None:
            self.generate_df( regen=False )
        return self._df

    @property
    def apiurl( self ):
        return self._spapi
    
    def sp_req( self, method, url, data=None, params=None ):
        """Query SkyPortal and return the result.

        Raises an SpExc exception if there's an error return or if the
        query returns text/html (instead of json).

        Returns the data structure given by the SkyPortal API.
        """
        headers = { 'Authorization': f'token {self._token}' }
        res = requests.request( method, url, json=data, params=params, headers=headers )

        if res.status_code not in [200,400]:
            self.logger.error( f'Got back status {res.status_code} ({res.reason})' )
            raise SpExc( res.status_code, 'Unexpected status', res.reason )
        if res.headers["content-type"][0:9] == "text/html":
            self.logger.error( f'Got back text/html from skyportal' )
            match = re.search( '^text/html; charset=([^;]*);?', res.headers["content-type"] )
            if match is None:
                self.logger.warning( f'Warning: can\'t get content type from header' )
                charset = 'UTF-8'
            else:
                charset = match.group(1)
            raise SpExc( res.status_code, 'Got back text/html', '', res.content.decode(charset) )
        if res.status_code == 400:
            errmsg = res.json()["message"]
            self.logger.error( f'Got error return from skyportal: {errmsg}' )
            raise SpExc( res.status_code, 'Skyportal error', errmsg )

        return res

    def sp_req_data( self, method, url, data=None, params=None ):
        """Query SkyPortal and return the data in the result.
        
        Raises an SpExc exception if there's an error return or if the
        query returns text/html (instead of json). Also raises an error
        return if there's no "status", or if the status isn't "success".

        Returns the "data" field from the API result
        """
        res = self.sp_req( method, url, data=data, params=params )
        retval = res.json()
        if 'status' not in retval:
            raise SpExc( f'Skyportal query return had no status' )
        if retval['status'] != 'success':
            errstr = f'Skyportal query returned status {retval["status"]}'
            if 'message' in retval:
                errstr += f' ({retval["message"]})'
            raise SpExc( errstr )
        if 'data' not in retval:
            raise SpExc( 'Skyportal query returned no data!' )
        return retval['data']
    
    def generate_df( self, regen=False ):
        if not regen:
            try:
                self.logger.info( "Reading skyportalcache.pkl" )
                self._df = pandas.read_pickle( "skyportalcache.pkl" )
            except Exception as e:
                self.logger.warning( "Failed to read skyportalcache.pkl, regenerating." )
                regen = True

        if regen:
            querysleep = 0.01
            totnumsrc = 999999999
            sources = []
            data = { 'group_ids': [self.mosthosts_group_id],
                     'includeSpectrumExists': True,
                     'numPerPage': 100,
                     'pageNumber': 1,
            }
            while len(sources) < totnumsrc:
                info = self.sp_req_data( 'GET', f'{self._spapi}/sources', params=data )
                totnumsrc = info['totalMatches']
                data['pageNumber'] += 1
                sources.extend( info['sources'] )
                sys.stderr.write( f'Read {len(sources)} of {totnumsrc} sources from SkyPortal\n' )
                time.sleep(querysleep)

            self._df = pandas.DataFrame( sources )
            self._df.set_index( 'id', inplace=True )
            self._df.to_pickle( "skyportalcache.pkl" )

    def get_instrument_id( self, name ):
        params = { 'name': name }
        data = self.sp_req_data('GET', f'{self._spapi}/instrument', params=params )
        if len( data ) == 0:
            raise SpExc( f'Unknown instrument {name}' )
        return data[0]['id']

    def spectra_for_obj( self, objid ):
        """Return all spectra for a given SkyPortal object ID
        
        Data structure is defined by the SkyPortal API.  It's the
        "spectra" field of "data" from
        https://skyportal.io/docs/api.html#tag/spectra/paths/~1api~1sources~1obj_id~1spectra/get
        """
        data = self.sp_req_data( 'GET', f'{self._spapi}/sources/{objid}/spectra' )
        return data['spectra']
    
# ======================================================================

def main():
    print( f'Silence in the library!  (And best not blink.)')

# ======================================================================

if __name__ == "__main__":
    main()
