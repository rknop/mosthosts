import os
import sys
import time
import pandas

from mosthosts_desi import MostHostsDesi

def main():
    mhd = MostHostsDesi()
    bts = pandas.read_csv( "bts_SNIa_qualcut_20211220.csv" )

    mhdztf = mhd.df[ mhd.df['snname'].apply( lambda x: x[0:3]=='ZTF' ) ]['snname'].unique()
    mhdhaszztf = mhd.haszdf[ mhd.haszdf['snname'].apply( lambda x: x[0:3]=='ZTF' ) ]['snname'].unique()

    # nbts = 0
    # nhaszbts = 0
    # start = time.perf_counter()
    # for snname in mhdztf:
    #     if snname in bts['ZTFID'].values:
    #         nbts += 1
    # for snname in mhdhaszztf:
    #     if snname in bts['ZTFID'].values:
    #         nhaszbts += 1
    # print( f'Time: {time.perf_counter()-start}' )
    # print( f'{nbts} BTS supernovae are in MostHosts and {nhaszbts} supernovae have at least one DESI redshift' )

    # Try to do this the Pandas way
    # Interestingly, it seems to be >2× slower than the for loop
    # Of course, there's additional information here becasue the table has been updated
    start = time.perf_counter()
    mhd.df['inbts'] = mhd.df['snname'].apply( lambda x : x in bts['ZTFID'].values )
    nbts = len( mhd.df[ mhd.df['inbts'] ]['snname'].unique() )
    nhaszbts = len( mhd.haszdf[ mhd.haszdf['inbts'] ]['snname'].unique() )
    print( f'Time: {time.perf_counter()-start}' )
                                              
    print( f'{nbts} BTS supernovae are in MostHosts and {nhaszbts} supernovae have at least one DESI redshift' )
        
    subdf = mhd.haszdf[ mhd.haszdf['inbts'] ]
    print( f'{"ZTF SN":16s} Host# {"RA":8s}  {"Dec":7s}  {"Peak time":9s}    {"SN z":6s}   Host z' )
    for i, row in subdf.iterrows():
        peakt = float( bts[ bts["ZTFID"] == row["snname"] ]["peakt"] ) + 2458000
        print( f'{row["snname"]:16s} {int(i[1]):2d}    {row["ra"]:8.4f} {row["dec"]:8.4f}  {peakt:10.2f}  '
               f'{row["sn_z"]:7.4f}  {row["z"]:7.4f}' )
    
# ======================================================================

if __name__ == "__main__":
    main()
