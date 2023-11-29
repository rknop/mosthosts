import sys
import pathlib
import logging
import multiprocessing
import multiprocessing.pool

sys.path.insert( 0, '/curveball/bin' )
import db
from get_images_for_sn import get_images_for_sn
from make_reference import make_reference
from exposuresource import ExposureSource

_logger = logging.getLogger( __name__ )
if not _logger.hasHandlers():
    _logout = logging.StreamHandler( sys.stderr )
    _logger.addHandler( _logout )
    _logout.setFormatter( logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s' ) )

_logger.setLevel( logging.INFO )

def buildrefs( obj ):
    logger = logging.getLogger( f'logger {obj}' )
    logger.propagate = False
    loghandler = logging.FileHandler( f'{obj}.log' )
    loghandler.setFormatter( logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s' ) )
    logger.addHandler( loghandler )
    logger.setLevel( logging.INFO )

    try:
        ztf = ExposureSource.get( 'ZTF' )

        # Figure out which filters we need

        imgs = get_images_for_sn( ztf, obj=obj, just_list=True, logger=logger )
        filts = set( [ ztf.blob_image_filter( imgs, i ) for i in range(len(imgs)) ] )

        # Build the reference

        with db.DB.get() as dbo:
            for filt in filts:
                bands = dbo.db.query( db.Band ).filter( db.Band.filtercode==filt ).all()
                if len(bands) > 1:
                    raise ValueError( f"filtercode {filt} applies to more than one band!" )
                elif len(bands) == 0:
                    raise ValueError( f"Can't find band for filtercode {filt}" )
                logger.info( f"Building reference for {filt}" )
                make_reference( ztf, bands[0], obj=obj, curdb=dbo, logger=logger )

        return True
    
    except Exception as ex:
        logger.exception( str(ex) )
        return False
    
def main():
    with open( 'ztf_with_iron_z_justnames.list' ) as ifp:
        sne = ifp.readlines()
    sne = [ i.strip() for i in sne ]

    pool = multiprocessing.pool.Pool( 12 )
    poolres = []
    for sn in sne:
        poolres.append( pool.apply_async( buildrefs, args=(sn,) ) )

    pool.close()
    pool.join()

    fails = []
    for sn, res in zip( sne, poolres ):
        if not ( res.successful() and res.get() ):
            fails.append( sn )

    if len(fails) == 0:
        _logger.info( "All sne succeeded" )
    else:
        nl = '\n'
        _logger.error( f"The following SNe failed:\n{nl.join(fails)}" )

# **********************************************************************

if __name__ == "__main__":
    main()

