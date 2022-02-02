import os
import sys
import math
import time
import pandas

from mosthosts_desi import MostHostsDesi

def main():
    mhd = MostHostsDesi( release="everest" )
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
    print( f'{"ZTF SN":16s} Host# {"RA":9s}  {"Dec":8s}  {"SN_RA":9s}  {"SN_Dec":9s}  '
           f'{"Peak time":9s}    {"SN z":6s}   Host z' )
    for i, row in subdf.iterrows():
        peakt = float( bts[ bts["ZTFID"] == row["snname"] ]["peakt"] ) + 2458000
        if math.fabs( row["sn_z"] - row["z"] ) > 0.05:
            appendix = "*******"
        elif math.fabs( row["sn_z"] - row["z"] ) > 0.01:
            appendix = "**"
        else:
            appendix = ""
        print( f'{row["snname"]:16s} {int(i[1]):2d}    {row["ra"]:8.5f} {row["dec"]:8.5f}  '
               f'{row["sn_ra"]:8.5f}  {row["sn_dec"]:8.5f}  {peakt:10.2f}  '
               f'{row["sn_z"]:7.4f}  {row["z"]:7.4f}  {appendix}' )
    
# ======================================================================

if __name__ == "__main__":
    main()
