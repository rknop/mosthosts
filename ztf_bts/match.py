import sys
import pathlib
import logging

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
    mhd_iron = MostHostsDesi( release="iron", force_regen=False,
                              dbuserpwfile="/secrets/decatdb_desi_desi" )
    withz = mhd_iron.df[ ~mhd_iron.df.z.isnull() ]
    mhsc = SkyCoord( withz.sn_ra.values, withz.sn_dec.values, unit=units.deg )

    ztfbts = pandas.read_csv( 'ztf_bts_Ia_20231128.csv' )
    ztfsc = SkyCoord( ztfbts.RA.values, ztfbts.Dec.values, unit=( units.hourangle, units.deg ) )

    idx, d2d, d3d = ztfsc.match_to_catalog_sky( mhsc )

    good = d2d.value < 1./3600.
    _logger.info( f"{good.sum()} of {len(ztfbts)} BTS Ias have at least one DESI host observation" )

    ztfbts = ztfbts.iloc[ good ]
    ztfsc = SkyCoord( ztfbts.RA.values, ztfbts.Dec.values, unit=( units.hourangle, units.deg ) )
    ztfbts.RA = ztfsc.ra.value
    ztfbts.Dec = ztfsc.dec.value
    withz = withz.iloc[ idx[good] ]    
    mhsc = SkyCoord( withz.sn_ra.values, withz.sn_dec.values, unit=units.deg )

    # The BTS table gives peakt as JD-2458000; convert to MJD
    ztfbts.peakt = ztfbts.peakt + 2458000 - 2400000.5

    with db.DB.get() as dbo:
        with open( "ztf_with_iron_z.lis", "w" ) as ofp:
            ofp.write( 'ZTFID,RA,Dec,peakt,type,redshift\n' )
            for dex, row in ztfbts.iterrows():
                ofp.write( f"{row.ZTFID},{row.RA},{row.Dec},{row.peakt},{row.type},{row.redshift}\n" )

                existing = db.Object.get_by_pos( row.RA, row.Dec )
                if len(existing) == 0:
                    _logger.info( f'{row.ZTFID} not yet defined, defining object' )
                    define_object( row.ZTFID, row.RA, row.Dec, row.peakt, z=row.redshift, curdb=dbo )


# **********************************************************************

if __name__ == "__main__":
    main()
