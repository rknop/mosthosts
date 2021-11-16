import sys
import logging
import re
import requests
import json
import time
# import html2text
import pandas

class SpExc(Exception):
    def __init__( self, status, info, message, data=None ):
        self.status = status
        self.info = info
        self.message = message
        self.data = data

    def __str__( self ):
        return f'Status {status}: {info} ("{message}")'

class MostHostsSkyPortal:
    mosthosts_group_id = 36
    
    def __init__( self, url="https://desi-skyportal.lbl.gov", token=None, logger=None ):
        if token is None:
            raise Exception( "API token required" )
        self._spapi = f'{url}/api'
        self._token = token
        self._df = None

        if logger is None:
            self.logger = logging.getLogger( "mhsp" )
            logerr = logging.StreamHandler( sys.stderr )
            self.logger.addHandler( logerr )
            logerr.setFormatter( logging.Formatter( f'[%(asctime)s - %(levelname)s] = %(message)s' ) )
            self.logger.setLevel( logging.INFO )
        else:
            self.logger = logger
        
    @property
    def df( self ):
        if self._df is None:
            self.generate_df( regen=False )
        return self._df

    @property
    def apiurl( self ):
        return self._spapi
    
    def sp_req( self, method, url, data=None, params=None ):
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
                res = self.sp_req( 'GET', f'{self._spapi}/sources', params=data )
                info = res.json()['data']
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
        res = self.sp_req( 'GET', f'{self._spapi}/instrument', params=params )
        if res.status_code != 200:
            raise SpExc( f'Got status {res.status_code} from instrument query' )
        data = res.json()
        if ( 'status' not in data ) or ( data['status'] != 'success' ):
            raise SpExc( 'Bad status from instrument query' )
        if len( data['data'] ) == 0:
            raise SpExc( f'Unknown instrument {name}' )
        return data['data'][0]['id']
            
# ======================================================================

def main():
    print( f'Silence in the library!  (And best not blink.)')

# ======================================================================

if __name__ == "__main__":
    main()
