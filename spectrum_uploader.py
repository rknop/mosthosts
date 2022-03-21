# This one requires the DESI environment to be set up, as it uses
#  the desi libraries

#### ROB TODO : Add the ability to replace spectra on skyportal
####            Add the ability to do releases other than daily

import sys
import os
import pickle
import re
import time
import math
import argparse
import datetime
import logging
import traceback

import numpy as np

from mosthosts_desi import MostHostsDesi
from mosthosts_skyportal import MostHostsSkyPortal, SpExc
from desi_specinfo import SpectrumInfo

import desispec

def upload_desi_spectrum( sn_id, index, night, spectrum, mhsp, instrument_id=None, logger=logging.getLogger("main") ):
    """Upload a desispec.spectrum.Spectra to SkyPortal.

    sn_id — the id on skyportal of the sn
    index — the "index" field in Most Hosts for the host this is a spectrum of
    night — either an integer or a string in the format yyyymmdd or yyyy-mm-dd
    spectrum — a desispec.spectrum.Spectra object.  It should have just a 
               single spectrum, and a single band 'brz'
    mhsp — a MostHostsSkyPortal object
    instrument_id — The instrument_id for DESI on SkyPortal.  (Looks it up on SkyPortal if not supplied.)
    """

    try:
        match = re.search( '^(\d\d\d\d)(\d\d)(\d\d)$', str(night) )
        if match is None:
            match = re.search( '^(\d\d\d\d)-(\d\d)-(\d\d)$', str(night) )
            if match is None:
                raise ValueError( f'Error parsing {night} for yyyymmdd or yyyy-mm-dd' )
        nightlabel = f'{match.group(1)}-{match.group(2)}-{match.group(3)}'
        label = f'Host {int(index)} {nightlabel}'
        obsnight = datetime.datetime( int(match.group(1)), int(match.group(2)), int(match.group(3)) )
        if instrument_id is None:
            instrument_id = mhsp.get_instrument_id('DESI')

        if ( 'brz' not in spectrum.wave.keys() ) or ( len(spectrum.wave.keys()) != 1 ):
            raise ValueError( 'Spectrum must have a single band "brz".' )
        if spectrum.flux['brz'].shape[0] != 1:
            raise ValueError( 'Must have just a single spectrum.' )

        # Deal with infinite errors
        if ( spectrum.ivar['brz'] >  0 ).sum() == 0:
            logger.error( f'Error for {sn_id} host {index} night {night}: spectrum is all 0 or negative!' )
            return None
        tinyivar = min( spectrum.ivar['brz'][ spectrum.ivar['brz'] > 0 ] ) / 1000.
        error = np.sqrt( 1./spectrum.ivar['brz'][0,:] )
        error[ np.isinf(error) ] = math.sqrt( 1./tinyivar )
    except Exception as e:
        traceback.print_exc()
        logger.error( f'Error processing spectrum for {sn_id} host {index} night {night}!' )
        return None

    data = {
        'obj_id': sn_id,
        'label': label,
        'wavelengths': spectrum.wave['brz'].tolist(),
        'fluxes': spectrum.flux['brz'][0,:].tolist(),
        'errors': error.tolist(),
        'instrument_id': instrument_id,
        'observed_at': obsnight.isoformat(),
        'group_ids': [MostHostsSkyPortal.mosthosts_group_id],
        'type': 'host_center'
        }
    mhsp.sp_req( 'POST', f'{mhsp.apiurl}/spectra', data=data )
    return label

# ======================================================================

def main():
    # print( "This is a work in progress, do not run." )

    logger = logging.getLogger("main")
    logerr = logging.StreamHandler(sys.stderr)
    logger.addHandler(logerr)
    logerr.setFormatter( logging.Formatter('[%(asctime)s - %(levelname)s] - %(message)s') )
    logger.setLevel(logging.INFO)
    
    parser = argparse.ArgumentParser( "Try to update the Desi Skyportal Mosthosts group with DESI spectra.",
                                      description = ( "Be doing things." ) )
    parser.add_argument( "-f", "--dbuserpwfile", default=None,
                         help=( "File with one line (username password) for DESI database.  (Make sure this file "
                                "is not world-readable!!)  Must specify either this, or -u and -p" ) )
    parser.add_argument( "-u", "--dbuser", default=None, help="User for desi database" )
    parser.add_argument( "-p", "--dbpasswd", default=None, help="Password for desi database" )
    parser.add_argument( "-r", "--force-regen", default=False, action="store_true",
                         help=( "Force regeneration of mosthosts_desi_daily_desiobs.pkl from the desi database "
                                " (by default, just read mosthosts_desi_daily_desiobs.pkl and do nothing)" ) )
    parser.add_argument( "-i", "--ignore-specinfo-cache", default=False, action="store_true",
                         help=( "Ignore (rebuild) cached file of information about what spectra are on SkyPortal" ) )
    parser.add_argument( "-t", "--skyportal-token", required=True, help="API token for skyportal" )
    parser.add_argument( "-s", "--skyportal-url", default="https://desi-skyportal.lbl.gov",
                         help="URL of skyportal (default: https://desi-skyportal.lbl.gov)" )
    parser.add_argument( "--sleep-time", type=float, default=0.1,
                         help="Sleep time (seconds) between skyportal requests (default: 0.1)" )
    parser.add_argument( "--really-upload", default=False, action='store_true',
                         help="Include this to do things." )
    parser.add_argument( "-c", "--candidates", default=[], nargs='*',
                         help="Only do these candidates (\"skyportal\" name) (default: do all)" )
    args = parser.parse_args()

    if ( args.dbuserpwfile is None ) and ( args.dbuser is None or args.dbpasswd is None ):
        sys.stderr.write( "Must specify either -f, or both -u and -p.\n" )
        sys.exit(20)
    if args.dbpasswd is None:
        with open( args.dbuserpwfile ) as ifp:
            (args.dbuser, args.dbpasswd) = ifp.readline().strip().split()
        
    spapi = f'{args.skyportal_url}/api'
        
    # Load in all the information about mosthosts spectra.  (Use the default release of "daily".)

    mosthosts = MostHostsDesi( dbuser=args.dbuser, dbpasswd=args.dbpasswd, dbuserpwfile=args.dbuserpwfile,
                               force_regen=args.force_regen, logger=logger )

    if len(args.candidates) > 0:
        # It's always hazardous using pandas, because there is lots of
        # short formalism, but it's not obvious exactly what it does,
        # especially in the case of multiple indexes.  What I'm trying
        # to do here is select all hosts for the list of candidates.
        # The fact that df.loc behaves differently if you pass it a
        # tuple than if you pass it a list is a recipe for confusion!
        # (From the pandas documentation: "Importantly, a list of tuples
        # indexes several complete MultiIndex keys, whereas a tuple of
        # lists refer to several values within a level."  So easy to
        # imagine people getting that mixed up! )
        haszdf = mosthosts.haszdf.loc[ args.candidates ]
        if len(haszdf) == 0:
            raise RuntimeError( f'No lines left in list of MostHosts candidates with DESI redshifts after '
                                f'limiting to {args.candidates}' )
    else:
        haszdf = mosthosts.haszdf
    
    mhsp = MostHostsSkyPortal( token=args.skyportal_token, logger=logger )
    instrument_id = mhsp.get_instrument_id('DESI')
    
    # Try to figure out which desi spectra are already uploaded

    if ( not args.ignore_specinfo_cache ) and os.path.isfile( "spspecinfo_cache.pkl" ):
        spspecinfo = pickle.load( open("specinfo_cache.pkl", "rb") )
    else:
        spspecinfo = {}

    labelparse = re.compile('^Host (\d+) (\d\d\d\d-\d\d-\d\d) ?(\d+)?$')

    # How I keep track:
    # The data structure "spspecinfo" has info on what's on sky portal.
    # It's a dictionary indexed by candidate id (the "spname" index from MostHostsDesi).
    # Each element of that dictionary is a dictionary indexed by host index (the "index" from MostHostsDesi).
    # Each element of that dictionary is a set of nights ("yyyy-mm-dd") for spectra that are on SkyPortal
    #
    # NOTE : it's possible, at least in principle, the same object will
    # be observed on the same night with a different targetid.  Right now,
    # this code ignores that and assumes it never happens.
    
    numdid = 0
    nmissing = 0
    curid = None
    skipcurid = False
    for mhdidx, mhdrow in haszdf.iterrows():
        if numdid % 100 == 0:
            logger.info( f'***** Doing row {numdid} of {len(haszdf)} DESI host spectra.' )
        numdid += 1
        # mhdid is the current candidate id, mhddex is the host index of the candidate
        mhdid, mhddex, targetid, tileid, petal, night = mhdidx
        spname = mhdrow["spname"]
        nightstr = str(night)
        nightstr = f"{nightstr[0:4]}-{nightstr[4:6]}-{nightstr[6:8]}"

        if ( ( spname in spspecinfo ) and ( mhddex in spspecinfo[spname] )
             and ( nightstr in spspecinfo[spname][mhddex] ) ):
            logger.info( f'{mhdid} host {mhddex} night {nightstr} already uploaded, moving on' )
            continue

        # Get the spectra for the current object (if we haven't already in a previous iteration)
        # Both from skyportal, and from desi
        
        if mhdid != curid:
            curid = mhdid
            skipcurid = False
            curhost = None
            
            if spname not in mhsp.df.index.values:
                logger.error( f'{spname} ({mhdid}) is in MostHostsDesi but not MostHostsSkyPortal' )
                nmissing += 1
                skipcurid = True
                continue
            if spname not in spspecinfo:
                spspecinfo[spname] = {}
            time.sleep( args.sleep_time )
            specs = mhsp.spectra_for_obj( spname )
            for spec in specs:
                match = labelparse.search( spec['label'] )
                if match is None:
                    logger.error( f'Failed to parse spectrum label {spec["label"]} for {spname}' )
                else:
                    sphost = int(match.group(1))
                    spnight = match.group(2)
                    spextra = match.group(3)    # Ignored for now
                    if sphost not in spspecinfo[spname]:
                        spspecinfo[spname][sphost] = set()
                    spspecinfo[spname][sphost].add( spnight )

        if mhddex != curhost:
            curhost = mhddex
                
            try:
                si = SpectrumInfo( mhdrow['ra'], mhdrow['dec'], desipasswd=args.dbpasswd, logger=logger )
            except Exception as e:
                logger.exception( "Failed to create SpectrumInfo for {spname}, moving on." )
                continue

        if skipcurid:
            continue

        if mhddex not in spspecinfo[spname]:
            spspecinfo[spname][mhddex] = set()

        # See if we need to upload the current spectrum

        if nightstr in spspecinfo[spname][mhddex]:
            logger.info( f'{spname} host {mhddex} night {nightstr} already uploaded, moving on' )
            continue

        # Need to upload current spectrum

        try:
            spec = si.get_spectrum( targetid, tileid, petal, night )
        except Exception as e:
            logger.exception( f'Failed to get spectrum for {spname} '
                              f'target {targetid} tile{tileid} petal {petal} night {night}' )
            continue
        if args.really_upload:
            logger.info( f'Uploading {spname} host {mhddex} night {nightstr} (target {targetid})...' )
            try:
                upload_desi_spectrum( spname, mhddex, nightstr, spec, mhsp, instrument_id )
            except Exception as e:
                logger.exception( f'Error uploading target {targetid} tile {tileid} petal {petal} night {night}|' )
            logger.info( f'...uploaded.' )
            time.sleep( args.sleep_time )
            spspecinfo[spname][mhddex].add( nightstr )
        else:
            logger.info( f'Would upload {spname} host {mhddex} night {nightstr} (target {targetid})' )
            
    pickle.dump( spspecinfo, open("specinfo_cache.pkl", "wb") )

# ======================================================================

if __name__ == "__main__":
    main()
