# This one requires the DESI environment to be set up, as it uses
#  the desi libraries

import sys
import re
import math
import datetime

import numpy as np

from mosthosts_desi import MostHostsDesi
from mosthosts_skyportal import MostHostsSkyPortal
from desi_specinfo import SpectrumInfo

import desispec

def upload_desi_spectrum( sn_id, index, night, spectrum, mhsp, instrument_id=None ):
    """Upload a desispec.spectrum.Spectra to SkyPortal.

    sn_id — the id on skyportal of the sn
    index — the "index" field in Most Hosts for the host this is a spectrum of
    night — either an integer or a string in the format yyyymmdd
    spectrum — a desispec.spectrum.Spectra object.  It should have just a 
               single spectrum, and a single band 'brz'
    mhsp — a MostHostsSkyPortal object
    instrument_id — The instrument_id for DESI on SkyPortal.  (Looks it up on SkyPortal if not supplied.)
    """

    yyyymmddparser = re.compile( '^(\d\d\d\d)(\d\d)(\d\d)$' )
    match = yyyymmddparser.search( str(night) )
    if match is None:
        raise ValueError( f'Error parsing {night} for yyyymmdd' )
    obsnight = datetime.datetime( int(match.group(1)), int(match.group(2)), int(match.group(3)) )
    if instrument_id is None:
        instrument_id = mhsp.get_instrument_id('DESI')

    if ( 'brz' not in spectrum.wave.keys() ) or ( len(spectrum.wave.keys()) != 1 ):
        raise ValueError( 'Spectrum must have a single band "brz".' )
    if spectrum.flux['brz'].shape[0] != 1:
        raise ValueError( 'Must have just a single spectrum.' )

    # Deal with infinite errors
    tinyivar = min( spectrum.ivar['brz'][ spectrum.ivar['brz'] > 0 ] ) / 1000.
    error = np.sqrt( 1./spectrum.ivar['brz'][0,:] )
    error[ np.isinf(error) ] = math.sqrt( 1./tinyivar )

    data = {
        'obj_id': sn_id,
        'label': f'Host_{int(index)}_{night}',
        'wavelengths': spectrum.wave['brz'].tolist(),
        'fluxes': spectrum.flux['brz'][0,:].tolist(),
        'errors': error.tolist(),
        'instrument_id': instrument_id,
        'observed_at': obsnight.isoformat(),
        'group_ids': [36],
        'type': 'host_center'
        }
    mhsp.sp_req( 'POST', f'{mhsp.apiurl}/spectra', data=data )
        

# ======================================================================

def main():
    print( "This is a work in progress, do not run." )
    
    parser = argparse.ArgumentParser( "Do things.",
                                      description = ( "Be doing things." ) )
    parser.add_argument( "-f", "--dbuserpwfile", default=None,
                         help=( "File with oneline username password for DESI database.  (Make sure this file "
                                "is not world-readable!!)  Must specify either htis, or -u and -p" ) )
    parser.add_argument( "-u", "--dbuser", default=None, help="User for desi database" )
    parser.add_argument( "-p", "--dbpasswd", default=None, help="Password for desi database" )
    parser.add_argument( "-r", "--force-regen", default=False, action="store_true",
                         help=( "Force regeneration of mosthosts_desi.csv from the desi database "
                                " (by default, just read mosthosts_dei.csv, and do nothing)" ) )
    parser.add_argument( "-t", "--skyportal-token", required=True, help="API token for skyportal" )
    parser.add_argument( "-s", "--skyportal-url", default="https://desi-skyportal.lbl.gov",
                         help="URL of skyportal (default: https://desi-skyportal.lbl.gov)" )
    parser.add_argument( "--sleep-time", type=float, default=0.1,
                         help="Sleep time (seconds) between skyportal requests (default: 0.1)" )
    args = parser.parse_args()

    if ( args.dbuserpwfile is None ) and ( args.dbuser is None or args.dbpasswd is None ):
        sys.stderr.write( "Must specify either -f, or both -u and -p.\n" )
        sys.exit(20)

    spapi = f'{args.skyportal_url}/api'
        
    # Load in all the information about mosthosts spectra.

    mosthosts = MostHostsDesi( dbuser=args.dbuser, dbpasswd=args.dbpasswd, dbuserpwfile=args.dbuserpwfile,
                               force_regen=args.force_regen )
    mhsp = MostHostsSkyPortal( token=args.skyportal_token )

    # Try to figure out which desi spectra are already uploaded

    
    
    


# ======================================================================

if __name__ == "__main__":
    main()
