import sys
import logging
import psycopg2
import psycopg2.extras
import numpy
import pandas

_logger = logging.getLogger(__name__)
if not _logger.hasHandlers():
    _logout = logging.StreamHandler( sys.stderr )
    _logger.addHandler( _logout )
    _formatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S' )
    _logout.setFormatter( _formatter )
    _logger.setLevel( logging.INFO )

def find_desi_targets( df, radius=1./3600., include=['main','backup','sv1','sv2','sv3'],
                       desidb=None, desipasswd=None, infoevery=100 ):
    """Adds information about desi targets to a Pandas dataframe.  Does not include secondary targets.

    df --  A Pandas dataframe that must have columns "ra" and "Dec"
    radius -- Radius of search in degrees (default 1/3600, i.e. 1")
    include -- which target lists to search (default: ['main', 'backup', 'sv1', 'sv2', 'sv3'] )

    Will add the following columns to the dataframe:
       targetlist, targetid, survey, brickid, brickname, brick_objid
    """

    mustclose = False
    if desidb is None:
        mustclose = True
        if desipasswd is None:
            raise ValueError( "Must either pass a desidb (psycopg2 connection) or a desipasswd (string)" )
        desidb = psycopg2.connect( dbname='desidb', host='decatdb.lbl.gov', user='desi', password=desipasswd,
                                   cursor_factory=psycopg2.extras.RealDictCursor )

    indices = []
    targetlists = []
    targetids = []
    surveys = []
    brickids = []
    bricknames = []
    brick_objids = []
    for i in range(len(df)):
        if ( i % infoevery ) == 0:
            _logger.info( f'Have searched {i} of {len(df)} positions for DESI targets...' )
        cursor = desidb.cursor()
        for which in include:
            if which not in ['main','backup','secondary','sv1','sv2','sv3']:
                if mustclose: desidb.close()
                raise ValueError( f"Unknown target table {which}" )
            q = ( f"SELECT targetid,survey,brickid,brickname,brick_objid "
                  f"FROM general.{which}targets "
                  f"WHERE q3c_radial_query(ra,\"dec\",%(ra)s,%(dec)s,%(radius)s)" )
            mogq = cursor.mogrify( q, { "ra": df['ra'][i], "dec": df['dec'][i], "radius": radius } )
            _logger.debug( "Sending query {mogq}" )
            cursor.execute( q, { "ra": df['ra'][i], "dec": df['dec'][i], "radius": radius } )
            result = cursor.fetchall()

            for row in result:
                indices.append( df.index.values[i] )
                targetlists.append( which )
                targetids.append( row['targetid'] )
                surveys.append( row['survey'] )
                brickids.append( row['brickid'] )
                bricknames.append( row['brickname'] )
                brick_objids.append( row['brick_objid'] )

    if mustclose:
        desidb.close()

    if isinstance( indices[0], tuple ):
        index = pandas.MultiIndex.from_tuples( indices, names=df.index.names )
    else:
        index = indices
    newdf = pandas.DataFrame( { "targetlist": targetlists,
                                "targetid": targetids,
                                "survey": surveys,
                                "brickid": brickids,
                                "brickname": bricknames,
                                "brick_objid": brick_objids },
                              index = index )
    newdf = df.merge( newdf, how='outer', left_index=True, right_index=True )

    return newdf
        

def find_desi_spec( df, radius=1./3600., release='daily', desidb=None, desipasswd=None,
                    infoevery=100 ):
    """Adds information about existing desi spectra to a Pandas dataframe.

    df --  A Pandas data frame that must have colums "ra" and "dec".
    radius -- Radius of search in degrees (default 1/3600, i.e. 1")

    Will add the following columns to the dataframe:
       targetid, tile, petal, night, z, zerr, zwarn, filename

    If more than one spectrum is found, will add more rows.  So, for
    instance, if you pass a dataframe with columns:

         snname   ra       dec
          sn1     245.2    -3.5
          sn2     18.7     27.6

    and DESI has observed an object at (245.2,-3.5) twice, the return
    dataframe will look something like:

         snname   ra       dec    targetid    tile    petal   night     z    zerr   zwarn  filename
          sn1     245.2    -3.5     666       2001      7    20220401  0.05  0.002   0     cooldata.fits
          sn1     245.2    -3.5     666       2001      7    20220315  0.05  0.003   0     awesomedata.fits
          sn2     18.7     27.6
    
    The DESI spectrum columns for objects where nothing was found are blank.
    """

    mustclose = False
    if desidb is None:
        mustclose = True
        if desipasswd is None:
            raise ValueError( "Must either pass a desidb (psycopg2 connection) or a desipasswd (string)" )
        desidb = psycopg2.connect( dbname='desidb', host='decatdb.lbl.gov', user='desi', password=desipasswd,
                                   cursor_factory=psycopg2.extras.RealDictCursor )

    indices = []
    targetid = []
    tile = []
    petal = []
    night = []
    z = []
    zerr = []
    zwarn = []
    filename = []
    spectype = []
    subtype = []
    for i in range(len(df)):
        if ( i % infoevery ) == 0:
            _logger.info( f"Have searched {i} of {len(df)} positions for DESI spectra..." )
        cursor = desidb.cursor()
        q = ( f"SELECT c.filename,f.targetid,f.tileid,f.petal_loc,f.device_loc,"
              f"  c.night,f.fiber,f.mean_fiber_ra,f.mean_fiber_dec,f.target_ra,f.target_dec,"
              f"  z.z,z.zerr,z.zwarn,z.chi2,z.deltachi2,z.spectype,z.subtype "
              f"FROM {release}.tiles_fibermap f "
              f"INNER JOIN {release}.cumulative_tiles c ON c.id=f.cumultile_id "
              f"INNER JOIN {release}.tiles_redshifts z ON "
              f"   ( z.cumultile_id=f.cumultile_id AND "
              f"     z.targetid=f.targetid ) "
              f"WHERE q3c_radial_query(f.target_ra,f.target_dec,%(ra)s,%(dec)s,%(radius)s)" )
        mogq = cursor.mogrify( q, {"ra": df['ra'][i], "dec": df['dec'][i], "radius": radius } )
        _logger.debug( "Sending query: {mogq}" )
        cursor.execute( q, {"ra": df['ra'][i], "dec": df['dec'][i], "radius": radius } )
        result = cursor.fetchall()
        if result is None or len(result) == 0:
            _logger.debug( f'Nothing found at ({df["ra"][i]}, {df["dec"][i]})' )

        for row in result:
            indices.append( df.index.values[i] )
            targetid.append( row['targetid'] )
            tile.append( row['tileid'] )
            petal.append( row['petal_loc'] )
            night.append( row['night'] )
            z.append( row['z'] )
            zerr.append( row['zerr'] )
            zwarn.append( row['zwarn'] )
            filename.append( row['filename'] )
            spectype.append( row['spectype'] )
            subtype.append( row['subtype'] )

    if mustclose:
        desidb.close()
            
    if isinstance( indices[0], tuple ):
        index = pandas.MultiIndex.from_tuples( indices, names=df.index.names )
    else:
        index = indices
    newdf = pandas.DataFrame( { "targetid": targetid,
                                "tile": tile,
                                "petal": petal,
                                "night": night,
                                "z": z,
                                "zerr": zerr,
                                "zwarn": zwarn,
                                "filename": filename,
                                "spectype": spectype,
                                "subtype": subtype },
                              index = index )
    newdf = df.merge( newdf, how='outer', left_index=True, right_index=True )

    return newdf
