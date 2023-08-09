#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import logging
import re
import requests
import json
import time
# import html2text
import pandas
from mosthosts_desi import MostHostsDesi
from mosthosts_skyportal import MostHostsSkyPortal

logger = logging.getLogger( "main" )
logerr = logging.StreamHandler( sys.stderr )
logger.addHandler( logerr )
logerr.setFormatter( logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s' ) )
logger.setLevel( logging.INFO )

def main():
    global logger

    regenerate = True
    with open( '/global/homes/r/raknop/secrets/skyportal_token' ) as ifp:
        token = ifp.readline().strip()
    mhsp = MostHostsSkyPortal( token=token )
    mhsp.generate_df( regen=regenerate )
    mhd = MostHostsDesi( dbuserpwfile='/global/homes/r/raknop/secrets/decatdb_desi_desi' )

    # Find sources that are in skyportal more than once
    grp = mhsp.df['ra'].groupby('id').aggregate('count')
    repeats = grp[ grp>1 ].index.values
    print( f'Sources that repeat in SkyPortal: {repeats}' )
    
    # Find mismatches
    print( f'Length of mosthosts table: {len(mhd.df)}' )
    print( f'Unique SNe in mosthosts table: {len(mhd.df.index.unique(level=0))}' )
    print( f'Number of sources in skyportal: {len(mhsp.df)}' )
    
    sourceids = mhsp.df.index.unique().values
    tmp = pandas.Series( mhd.df.index.unique(level=0) )
    missing = tmp[ tmp.apply( lambda x: x not in sourceids ) ].unique()
    print( f'Sources missing from skyportal: {missing}' )

    spnames = mhd.df.index.unique(level=0).values
    tmp = pandas.Series( sourceids )
    missing = tmp[ tmp.apply( lambda x: x not in spnames ) ].unique()
    print( f'Sources in skyportal but not in mosthosts table: {missing}' )
    
# ======================================================================

if __name__ == "__main__":
    main()
