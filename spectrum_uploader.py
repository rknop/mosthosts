import sys

from mosthosts_desi import MostHostsDesi
from mosthosts_skyportal import MostHostsSkyPortal
from desi_specinfo import SpectrumInfo

def upload_desi_spectrum( sn_id, index, ordinal spectrumdata, mhsp ):
    for band in ['B', 'R', 'Z']:
        data = {
            'obj_id': sn_id,
            'label': 'Host_{band}_{index}_{ordinal}_{rob something about the date}'
            'wavelengths': spectrumdata[f'{band}_wavelength'],
            'fluxes': spectrumdata[f'{band}_flux'],
            'errors': spectrumdata[f'{band}_dflux'],
            'instrument_id': ROB FIGURE THIS OUT,
            'observed_at': ROB FIGURE THIS OUT,
            'group_ids': [36],
            'type': 'host_center'
            }
        mhsp.sp_req( f'{mhsp.apiurl}/spectra', data=data )
        

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
