import sys
import pathlib
import logging

import numpy
import pandas

from astropy.coordinates import SkyCoord
import astropy.units as units

sys.path.insert( 0, '/curveball/bin' )
import db
from define_object import define_object

_libdir = str( pathlib.Path( __file__ ).parent.parent / "lib" )
if _libdir not in sys.path:
    sys.path.insert( 0, _libdir )

from mosthosts_desi import MostHostsDesi

_logger = logging.getLogger( __name__ )
if not _logger.hasHandlers():
    _logout = logging.StreamHandler( sys.stderr )
    _logger.addHandler( _logout )
    _logout.setFormatter( logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s' ) )
_logger.setLevel( logging.INFO )

def main():
    match_obs_with_ltcvs = True
    define_objects = False
    update_to_desi_z = True
    closez = 0.01
    obs_with_ltcvs_file = 'obj_with_ltcv.lis'
    btsfile = 'ztf_bts_Ia_20231128.csv'
    # outfile = 'ztf_with_iron_z.lis'
    outfile = 'tmp.lis'
    
    mhd_iron = MostHostsDesi( release="iron", force_regen=False,
                              dbuserpwfile="/secrets/decatdb_desi_desi" )
    withz = mhd_iron.df[ ~mhd_iron.df.z.isnull() ]
    mhsc = SkyCoord( withz.sn_ra.values, withz.sn_dec.values, unit=units.deg )

    if match_obs_with_ltcvs:
        with open( obs_with_ltcvs_file, 'r' ) as ifp:
            objswithltcvs = [ i.strip() for i in ifp.readlines() ]

    ztfbts = pandas.read_csv( btsfile )
    if match_obs_with_ltcvs:
        ztfbts = ztfbts[ ztfbts['ZTFID'].apply( lambda x: x in objswithltcvs ) ]
    ztfsc = SkyCoord( ztfbts.RA.values, ztfbts.Dec.values, unit=( units.hourangle, units.deg ) )

    idx, d2d, d3d = ztfsc.match_to_catalog_sky( mhsc )

    good = d2d.value < 1./3600.
    _logger.info( f"{good.sum()} of {len(ztfbts)} BTS Ias have at least one DESI host observation" )

    ztfbts = ztfbts.iloc[ good ].reset_index()
    ztfsc = SkyCoord( ztfbts.RA.values, ztfbts.Dec.values, unit=( units.hourangle, units.deg ) )
    ztfbts.RA = ztfsc.ra.value
    ztfbts.Dec = ztfsc.dec.value
    mhmatchz = withz.iloc[ idx[good] ].reset_index()
    mhsc = SkyCoord( mhmatchz.sn_ra.values, mhmatchz.sn_dec.values, unit=units.deg )

    # The BTS table gives peakt as JD-2458000; convert to MJD
    ztfbts.peakt = ztfbts.peakt + 2458000 - 2400000.5

    with db.DB.get() as dbo:
        with open( outfile, "w" ) as ofp:
            ofp.write( 'ZTFID,RA,Dec,peakt,type,redshift,desi_z\n' )
            for dex, row in ztfbts.iterrows():

                mhsn = ( withz[ withz.index.get_level_values('sn_name_sp') == mhmatchz.iloc[ dex ].sn_name_sp ]
                         .reset_index() )
                dz = numpy.abs( mhsn.z - row.redshift )
                mhsn_with_close_z = mhsn[ dz <= closez ].reset_index()
                if len( mhsn ) == 1:
                    if len( mhsn_with_close_z ) == 0:
                        mhz = mhsn.z.iloc[0]
                        _logger.warning( f"z mismatch: {row.ZTFID} single DESI z {mhz:.3f} "
                                         f"doesn't match BTS z {row.redshift:.3f}" )
                    else:
                        mhz = mhsn_with_close_z.z.iloc[0]
                elif len( mhsn ) > 1:
                    if len( mhsn_with_close_z ) == 1:
                        _logger.info( f"{row.ZTFID} has multiple desi hosts with z, choosing the only "
                                      f"one within {closez}" )
                        mhz = mhsn_with_close_z.z.iloc[0]
                    elif len( mhsn_with_close_z ) > 1:
                        _logger.warning( f"{row.ZTFID} has multiple desi hosts with z within {closez}, "
                                         f"picking one arbitrarily" )
                        mhz = mhsn_with_close_z.z.iloc[0]
                    else:
                        mhz = mhsn.z.iloc[0]
                        _logger.warning( f"z mismatch: {row.ZTFID} has multiple desi hosts, "
                                         f"but none with z within {closez}, "
                                         f"picking one arbitrarily ({mhz:.3f} vs. bts {row.redshift:.3f})" )
                else:
                    raise RuntimeError( f"This should never happen" )
                        
                ofp.write( f"{row.ZTFID},{row.RA},{row.Dec},{row.peakt},{row.type},{row.redshift},{mhz}\n" )

                existing = db.Object.get_by_pos( row.RA, row.Dec, curdb=dbo )
                if len(existing) == 0:
                    if define_objects:
                        _logger.info( f'{row.ZTFID} not yet defined in curveball database, defining object' )
                        define_object( row.ZTFID, row.RA, row.Dec, row.peakt, z=row.redshift, curdb=dbo )
                    else:
                        _logger.warning( f'{row.ZTFID} not defined in curveball database' )
                elif not define_objects:
                    if len(existing) > 1:
                        _logger.error( f'{row.ZTFID} has {len(existing)} matches in curveball database!' )
                    _logger.debug( f'{row.ZTFID} exists in curveball database' )

                if ( len(existing) > 0 ) and ( update_to_desi_z ):
                    _logger.info( f"Updating desi Z of {existing[0].name} from {existing[0].z} to {mhz}" )
                    existing[0].z = mhz

        if update_to_desi_z:
            dbo.db.commit()

# **********************************************************************

if __name__ == "__main__":
    main()
