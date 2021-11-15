import sys

from mosthosts_desi.py import MostHostsDesi
from desi_specinfo.py import SpectrumInfo

class SpExc(Exception):
    def __init__( self, status, info, message, data=None ):
        self.status = status
        self.info = info
        self.message = message
        self.data = data

    def __str__( self ):
        return f'Status {status}: {info} ("{message}")'

def sp_req( method, url, data=None, params=None ):
    global logger

    skyportal_token = 'a8e36acb-806e-4fd9-9e00-7d01f0be46a7'
    headers = { 'Authorization': f'token {skyportal_token}' }
    res = requests.request( method, url, json=data, params=params, headers=headers )

    if res.status_code not in [200,400]:
        logger.error( f'Got back status {res.status_code} ({res.reason})' )
        raise SpExc( res.status_code, 'Unexpected status', res.reason )
    if res.headers["content-type"][0:9] == "text/html":
        logger.error( f'Got back text/html from skyportal' )
        match = re.search( '^text/html; charset=([^;]*);?', res.headers["content-type"] )
        if match is None:
            logger.warning( f'Warning: can\'t get content type from header' )
            charset = 'UTF-8'
        else:
            charset = match.group(1)
        raise SpExc( res.status_code, 'Got back text/html', '', html2text.html2text(res.content.decode(charsert)) )
    if res.status_code == 400:
        errmsg = res.json()["message"]
        logger.error( f'Got error return from skyportal: {errmsg}' )
        raise SpExc( res.status_code, 'Skyportal error', errmsg )

    return res

# ======================================================================

def main():
    print( "This is broken, do not run." )
    
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
    
    
    
    


# ======================================================================

if __name__ == "__main__":
    main()
