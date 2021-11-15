#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import logging
import re
import requests
import json
import time
import html2text
import pandas

logger = logging.getLogger( "main" )
logerr = logging.StreamHandler( sys.stderr )
logger.addHandler( logerr )
logerr.setFormatter( logging.Formatter( f'[%(asctime)s - %(levelname)s] = %(message)s' ) )
logger.setLevel( logging.INFO )

class SpExc(Exception):
    def __init__( self, status, info, message, data=None ):
        self.status = status
        self.info = info
        self.message = message
        self.data = data

    def __str__( self ):
        return f'Status {status}: {info} ("{message}")'

def sp_req( method, url, data=None, params=None ):
    global logger

    skyportal_token = 'a8e36acb-806e-4fd9-9e00-7d01f0be46a7'
    headers = { 'Authorization': f'token {skyportal_token}' }
    res = requests.request( method, url, json=data, params=params, headers=headers )

    if res.status_code not in [200,400]:
        logger.error( f'Got back status {res.status_code} ({res.reason})' )
        raise SpExc( res.status_code, 'Unexpected status', res.reason )
    if res.headers["content-type"][0:9] == "text/html":
        logger.error( f'Got back text/html from skyportal' )
        match = re.search( '^text/html; charset=([^;]*);?', res.headers["content-type"] )
        if match is None:
            logger.warning( f'Warning: can\'t get content type from header' )
            charset = 'UTF-8'
        else:
            charset = match.group(1)
        raise SpExc( res.status_code, 'Got back text/html', '', html2text.html2text(res.content.decode(charsert)) )
    if res.status_code == 400:
        errmsg = res.json()["message"]
        logger.error( f'Got error return from skyportal: {errmsg}' )
        raise SpExc( res.status_code, 'Skyportal error', errmsg )

    return res
    
def main():
    global logger

    spapi = 'https://desi-skyportal.lbl.gov/api'

    # mosthosts = pandas.read_csv( "mosthosts_desi.csv" )
    # mosthosts.set_index( ['snname', 'index'], inplace=True )

    # # This quickly gets "too many requests" errors
    # for dex, row in mosthosts.iterrows():
    #     snname = dex[0]
    #     logger.info( f'Reading source {snname}' )
    #     try:
    #         res = sp_req( 'GET', f'{spapi}/sources/{snname}' )
    #     except SpExc as e:
    #         logger.error( f'Failed to read {snname}, moving on: str(e)' )
    #         continue
    #     import pdb; pdb.set_trace()

    querysleep = 0.01
    totnumsrc = 999999999
    sources = []
    data = { 'group_ids': [36],
             'numPerPage': 100,
             'pageNumber': 0,
            }
    while len(sources) < totnumsrc:
        res = sp_req( 'GET', f'{spapi}/sources', params=data )
        info = res.json()['data']
        totnumsrc = info['totalMatches']
        data['pageNumber'] += 1
        sources.extend( info['sources'] )
        sys.stderr.write( f'Read {len(sources)} of {totnumsrc} sources.\n' )
        time.sleep(querysleep)

    sourcetable = pandas.DataFrame( sources )
    sourcetable.to_pickle( "sourcetable.pkl" )

# ======================================================================

if __name__ == "__main__":
    main()
