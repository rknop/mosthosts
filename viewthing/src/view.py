import sys
import os
import io
import re
import pathlib
import math
import web
import json
import traceback

import psycopg2
import psycopg2.extras

rundir = pathlib.Path(__file__).parent
if not str(rundir) in sys.path:
    sys.path.insert( 0, str(rundir) )
import db
import auth

# ======================================================================

def logerr( cls, e ):
    out = io.StringIO()
    traceback.print_exc( file=out )
    sys.stderr.write( out.getvalue() )
    return json.dumps( { "error": f"Exception in {cls}: {e}" } )

# ======================================================================

class HandlerBase:
    def __init__(self):
        self.response = ""
        self.db = None

    def __del__( self ):
        self.closedb()

    def opendb( self ):
        self.closedb()
        with open( "/secrets/desiuserpassword" ) as ifp:
            dbuser, dbpasswd = ifp.readline().strip().split()
        db.DB.DBinit( 'decatdb.lbl.gov', 5432, dbuser, dbpasswd, 'desidb' )
        self.db = db.DB.get()

    def closedb( self ):
        if self.db is not None:
            self.db.close()
        self.db = None

    def GET( self, *args, **kwargs ):
        self.opendb()
        retval = self.do_the_things( *args, **kwargs )
        self.closedb()
        return retval

    def POST( self, *args, **kwargs ):
        self.opendb()
        retval = self.do_the_things( *args, **kwargs )
        self.closedb()
        return retval

    def verifyauth( self ):
        if ( not hasattr( web.ctx.session, "authenticated" ) ) or ( not web.ctx.session.authenticated ):
            raise RuntimeError( "User not authenticated" )

    def jsontop( self ):
        web.header('Content-Type', 'application/json')

    def jsontop_verifyauth( self ):
        self.jsontop()
        self.verifyauth()

    def htmltop( self, title=None, h1=None, statusdiv=False, includejsstart=False,
                 jsstart="mosthosts_start.js", addjs=[] ):
        web.header( 'Content-Type', 'text/html; charset="UTF-8"' )
        webapdirurl = str( pathlib.Path( web.ctx.homepath ).parent )
        if webapdirurl[-1] != "/":
            webapdirurl += "/"
        if title is None:
            title = "Mosthosts Viewer Thingy"
        if h1 is None:
            h1 = "Mosthosts Viewer Thingy"
        self.response += "<!DOCTYPE html>\n"
        self.response += "<html>\n<head>\n<meta charset=\"UTF-8\">\n"
        self.response += f"<title>{title}</title>\n"
        self.response += f"<link href=\"{webapdirurl}mosthosts.css\" rel=\"stylesheet\" type=\"text/css\">\n"
        self.response += f"<script src=\"{webapdirurl}aes.js\"></script>\n"
        self.response += f"<script src=\"{webapdirurl}jsencrypt.min.js\"></script>\n"
        self.response += f"<script src=\"{webapdirurl}mosthosts.js\" type=\"module\"></script>\n"
        for js in addjs:
            self.response += f"<script src=\"{webapdirurl}{js}\"></script>\n"
        if includejsstart:
            self.response += f"<script src=\"{webapdirurl}{jsstart}\" type=\"module\"></script>\n"
        self.response += "</head>\n<body>\n"
        if statusdiv:
            self.htmlstatusdiv()
        if h1 != "":
            self.response += f"<h1>{h1}</h1>\n";
        self.response += "<div id=\"maindiv\">\n"

    def htmlbottom( self ):
        self.response += "</div>\n</body>\n</html>\n"

    def htmlstatusdiv( self ):
        self.response += "<div id=\"status-div\" name=\"status-div\"></div>\n"

# ======================================================================

class FrontPage(HandlerBase):
    def do_the_things( self ):
        self.htmltop()
        self.response += "<p>Hello, world!</p>\n"
        self.htmlbottom()
        return self.response

# =====================================================================
    
class GetHosts(HandlerBase):
    def do_the_things( self ):
        self.jsontop()
        try:
            data = { 'min_hosts': None,
                     'limit': 100,
                     'offset': 0 }
            # data.update( json.loads( web.data() ) )
            conn = db.DB.engine.raw_connection()
            with conn.cursor( cursor_factory=psycopg2.extras.DictCursor ) as cursor:
                q = ( "SELECT snname "
                      "INTO TEMP TABLE temp_snname "
                      "FROM ( SELECT snname,COUNT(hostnum) AS nhosts "
                      "       FROM static.mosthosts_pre "
                      "       GROUP BY snname ORDER BY snname ) subq " )
                if data['min_hosts'] is not None:
                    q += "WHERE nhosts>%(minhosts)s "
                if data['limit'] is not None:
                    q += "LIMIT %(limit)s "
                if data['offset'] is not None:
                    q += "OFFSET %(offset)s "

                cursor.execute( q, { 'minhosts': data['min_hosts'],
                                     'limit': data['limit'],
                                     'offset': data['offset'] } )
                q = ( "SELECT snname,hostnum,sn_z,sn_ra,sn_dec,ra_dr9,dec_dr9,ra_sga,dec_sga, "
                      "  fracflux_g_dr9,fracflux_r_dr9,fracflux_z_dr9,"
                      "  fracflux_w1_dr9,fracflux_w2_dr9,fracflux_w3_dr9,fracflux_w4_dr9 "
                      "FROM static.mosthosts_pre "
                      "WHERE snname IN ( SELECT snname FROM temp_snname ) "
                      "ORDER BY snname" )
                cursor.execute( q )
                rows = cursor.fetchall()
                sne = []
                cursndata = {}
                cursn = None
                for row in rows:
                    if row['snname'] != cursn:
                        if cursn is not None:
                            sne.append( cursndata )
                        cursn = row['snname']
                        cursndata = { i: row[i] for i in [ 'snname', 'sn_z', 'sn_ra', 'sn_dec' ] }
                        cursndata['hosts'] = []
                    cursndata['hosts'].append( { i: row[i] for i in [ 'ra_dr9', 'dec_dr9', 'ra_sga',
                                                                      'dec_sga', 'fracflux_g_dr9',
                                                                      'fracflux_r_dr9', 'fracflux_w1_dr9',
                                                                      'fracflux_w2_dr9', 'fracflux_w3_dr9',
                                                                      'fracflux_w4_dr9' ] } )
                if cursn is not None:
                    sne.append( cursndata )

                return json.dumps( { 'status': 'ok', 'sne': sne } )
        except Exception as ex:
            sys.stderr.write( f"Exception in {self.__class__}: {ex}\n" )
            return logerr( self.__class__, ex )

# ======================================================================

urls = (
    '/', "FrontPage",
    '/gethosts', "GetHosts",
    # '/auth', auth.app,
    # '(.*)', "DumpURL"
    )

app = web.application( urls, locals() )
web.config.session_parameters['samesite'] = 'lax'

# ROB MAKE THIS CONFIGURABLE
web.config.smtp_server = 'smtp.lbl.gov'
web.config.smtp_port = 25

initializer = {}
initializer.update( auth.initializer )
session = web.session.Session( app, web.session.DiskStore( "/sessions" ), initializer=initializer )
def session_hook(): web.ctx.session = session
app.add_processor( web.loadhook( session_hook ) )

application = app.wsgifunc()

# ======================================================================
# This won't be run from within apache, but it's here for a smoke test

def main():
    global app
    sys.stderr.write( "Running webapp.\n" )
    sys.stderr.flush()
    app.run()

if __name__ == "__main__":
    main()

