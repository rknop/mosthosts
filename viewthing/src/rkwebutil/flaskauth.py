# This file is part of rkwebutil
#
# rkwebutil is Copyright 2023 by Robert Knop
#
# rkwebutil is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the
# Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.
#
# rkwebutil is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
# for more details.
#
# You should have received a copy of the GNU General Public License
# along with rkwebutil. If not, see <https://www.gnu.org/licenses/>.

import sys
import pathlib
import binascii
import uuid
import datetime
import pytz
import traceback
import smtplib
import ssl
from email.message import EmailMessage
from email.policy import EmailPolicy
import flask
import Cryptodome.PublicKey.RSA
import Cryptodome.Cipher.PKCS1_v1_5

_dir = pathlib.Path(__file__).parent
if str(_dir) not in sys.path:
    sys.path.append( str(_dir) )

import db

bp = flask.Blueprint( 'auth', __name__, url_prefix='/auth' )

class RKAuthConfig:
    """Global rkauth config.

    After importing auth, reset these two variables to what you want to
    see in the header of the "reset password" emails.

    """
    email_from = 'RKAuth <nobody@nowhere.org>'
    email_subject = 'RKAuth password reset'
    email_system_name = 'a webserver using the RKAuth system'
    smtp_server = 'some_smtp_server'
    smtp_port = 465
    smtp_use_ssl = True
    smtp_username = None
    smtp_password = None
    webap_url = None

    def setdbparams( *args ):
        db.DB.setdbparams( *args )
    
# ROB WRITE INSTRUCTIONS

@bp.route( '/getchallenge', methods=['POST'] )
def getchallenge():
    try:
        flask.session['authenticated'] = False
        if not flask.request.is_json:
            return flask.jsonify( { 'error': '/auth/getchallenge was expecting application/json' } )
        if not 'username' in flask.request.json:
            return flask.jsonify( { 'error': 'No username sent to server' } )
        with db.DB.get() as dbsess:
            users = db.AuthUser.getbyusername( flask.request.json['username'] )
            if len(users) > 1:
                return flask.jsonify( { 'error':
                                        f'User {flask.request.json["username"]} is multiply defined; this is bad!' } )
            if len(users) == 0:
                return flask.jsonify( { 'error': f'No such user {flask.request.json["username"]}' } )
            user = users[0]
        tmpuuid = str( uuid.uuid4() )
        if user.pubkey is None:
            return flask.jsonify( { 'error': ( f'User {flask.request.json["username"]} '
                                               f'does not have a password set yet.' ) } )
        pubkey = Cryptodome.PublicKey.RSA.importKey( user.pubkey )
        cipher = Cryptodome.Cipher.PKCS1_v1_5.new( pubkey )
        challenge = binascii.b2a_base64( cipher.encrypt( tmpuuid.encode("UTF-8") ) ).decode( "UTF-8" )
        flask.session['username'] = user.username
        flask.session['useruuid'] = user.id
        flask.session['userdisplayname'] = user.displayname
        flask.session['useremail'] = user.email
        flask.session['authuuid']= tmpuuid
        retdata = { 'username': user.username,
                    'privkey': user.privkey,
                    'challenge': challenge }
        return flask.jsonify( retdata )
    except Exception as e:
        sys.stderr.write( f'{traceback.format_exc()}\n' )
        return flask.jsonify( { 'error': f'Exception in GetAuthChallenge: {str(e)}' } )


@bp.route( '/respondchallenge', methods=['POST'] )
def respondchallenge():
    try:
        if not flask.request.is_json:
            return flask.jsonify( { 'error': '/auth/getchallenge was expecting application/json' } )
        if ( ( not 'username' in flask.request.json ) or
             ( not 'response' in flask.request.json ) ):
            return flask.jsonify( { 'error': ( 'Login error: username or challenge response missing; '
                                               ' (you probably can\'t fix this, contact code maintainer) '
                                              ) } )
        if flask.request.json['username'] != flask.session['username']:
            return flask.jsonify( { 'error': ( f'username {username} didn\'t match session username '
                                               '{flask.session["username"]}; try logging out and logging back in '
                                              ) } )
        if flask.session["authuuid"] != flask.request.json['response']:
            return flask.jsonify( { 'error': 'Authentication failure.' } )
        flask.session['authenticated'] = True
        return flask.jsonify(
            { 'status': 'ok',
              'message': f'User {flask.session["username"]} logged in.',
              'username': flask.session["username"],
              'useruuid': str( flask.session["useruuid"] ),
              'useremail': flask.session["useremail"],
              'userdisplayname': flask.session["userdisplayname"],
             } )
    except Exception as e:
        sys.stderr.write( f'{traceback.format_exc()}\n' )
        return flask.jsonify( { 'error': f'Exception in RespondAuthChallenge: {str(e)}' } )


@bp.route( '/getpasswordresetlink', methods=['POST'] )
def getpasswordresetlink():
    try:
        if not flask.request.is_json:
            return flask.jsonify( { 'error': '/auth/getchallenge was expecting application/json' } )
        if 'username' in flask.request.json:
            username = flask.request.json['username']
            them = db.AuthUser.getbyusername( username );
            if len( them ) == 0:
                return flask.jsonify( { "error": f"username {username} not known" } )
            if len( them ) > 1:
                return flask.jsonify( { "error": ( f"username {username} is multiply defined! "
                                                   "This is bad.  This is very bad." ) } )
        elif 'email' in flask.request.json:
            email = flask.request.json['email']
            them = db.AuthUser.getbyemail( email )
            if len( them ) == 0:
                return flask.request.jsonify( { "error": f"email {email} not known" } )
        sentto = ""
        for user in them:
            link = db.PasswordLink.new( user.id )
            if RKAuthConfig.webap_url is None:
                webap_url = flask.request.base_url
            else:
                webap_url = RKAuthConfig.webap_url

            if RKAuthConfig.smtp_use_ssl:
                ssl_context = ssl.create_default_context()
                smtp = smtplib.SMTP_SSL( RKAuthConfig.smtp_server, RKAuthConfig.smtp_port, context=ssl_context )
            else:
                smtp = smtplib.SMTP( RKAuthConfig.smtp_server, RKAuthConfig.smtp_port )
            if RKAuthConfig.smtp_username is not None:
                smtp.login( RKAuthConfig.smtp_username, RKAuthConfig.smtp_password )

            policy = EmailPolicy( max_line_length=999, linesep='\n' )
            msg = EmailMessage( policy )
            msg['Subject'] = RKAuthConfig.email_subject
            msg['From'] = RKAuthConfig.email_from
            msg['To'] = user.email
            sys.stderr.write( f"webap_url is {webap_url}; RKAuthConfig.webap_url is {RKAuthConfig.webap_url}; "
                              f"flask.request.base_url is {flask.request.base_url}\n" )
            msg.set_content(f"Somebody requested a password reset for {user.username}\n"
                            f"for {RKAuthConfig.email_system_name}.  This link will expire in 1 hour.\n"
                            f"\n"
                            f"If you did not request this, you may ignore this message.\n"
                            f"Here is the link; cut and paste it into your browser:\n"
                            f"\n"
                            f"{webap_url}/resetpassword?uuid={str(link.id)}" )
            smtp.send_message( msg, RKAuthConfig.email_from, to_addrs=user.email )
            smtp.quit()
            if len(sentto) > 0:
                sentto += " "
            sentto += user.username
        return flask.jsonify( { 'status': f'Password reset link(s) sent for {sentto}.' } )
    except Exception as e:
        sys.stderr.write( f'{traceback.format_exc()}\n' )
        return flask.jsonify( { 'error': f'Exception in GetPasswordResetLink: {str(e)}' } )

@bp.route( '/resetpassword', methods=['GET'] )
def resetpassword():
    response = "<!DOCTYPE html>\n"
    response += "<html>\n<head>\n<meta charset=\"UTF-8\">\n"
    response += f"<title>Password Reset</title>\n"

    # webapdirurl = str( pathlib.Path( web.ctx.env['SCRIPT_NAME'] ).parent )
    webapdirurl = str( pathlib.Path( flask.request.path ).parent.parent )
    if webapdirurl[-1] != '/':
        webapdirurl += "/"
    # sys.stderr.write( f"In ResetPassword, webapdirurl is {webapdirurl}\n" )
    # response += "<link href=\"" + webapdirurl
    # response += "photodb.css\" rel=\"stylesheet\" type=\"text/css\">\n"
    response += "<script src=\"" + webapdirurl + "static/aes.js\"></script>\n"
    response += "<script src=\"" + webapdirurl + "static/jsencrypt.min.js\"></script>\n"
    response += "<script src=\"" + webapdirurl + "static/resetpasswd_start.js\" type=\"module\"></script>\n"
    response += "</head>\n<body>\n"
    response += f"<h1>Reset Password</h1>\n<p><b>ROB Todo: make this header better</b></p>\n";

    try:
        if not 'uuid' in flask.request.args:
            response += "<p>Malformed password reset URL.</p>\n</body></html>"
            return flask.make_response( response )
        pwlink = db.PasswordLink.get( flask.request.args['uuid'] )
        if pwlink is None:
            response += "<p>Invalid password reset URL.</p>\n</body></html>"
            return flask.make_response( response )
        if pwlink.expires < datetime.datetime.now(pytz.utc):
            response += "<p>Password reset link has expired.</p>\n</body></html>"
            return flask.make_response( response )
        user = db.AuthUser.get( pwlink.userid )

        response += f"<h2>Reset password for {user.username}</h2>\n"
        response += "<div id=\"authdiv\">"
        response += "<table>\n"
        response += "<tr><td>New Password:</td><td>"
        response += ( "<input type=\"password\" name=\"newpassword\" id=\"reset_password\" "
                      "type=\"password\" size=20>" )
        response += "</td></tr>\n"
        response += "<tr><td>Confirm:</td><td>"
        response += ( "<input type=\"password\" name=\"confirmpassword\" id=\"reset_confirm_password\" "
                      "type=\"password\" size=20>" )
        response += "</td></tr>\n"
        response += "<tr><td colspan=\"2\">"
        response += "<button name=\"getnewpassword\" id=\"setnewpassword_button\">Set New Password</button>\n"
        response += "</td></tr>\n</table>\n"
        response += "</div>\n"
        response += ( f"<input type=\"hidden\" name=\"linkuuid\" id=\"resetpasswd_linkid\" "
                      f"value=\"{str(pwlink.id)}\">" )
        response += "</body>\n</html>\n"
        return flask.make_response( response )
    except Exception as e:
        sys.stderr.write( f'{traceback.format_exc()}\n' )
        response += f'Exception in ResetPassword: {str(e)}'
        return flask.make_response( response )


@bp.route( '/getkeys', methods=['POST'] )
def getkeys():
    try:
        if not flask.request.is_json:
            return flask.jsonify( { 'error': '/auth/getkeys was expecting application/json' } )
        if 'passwordlinkid' not in flask.request.json:
            return flask.jsonify( { "error": "No password link id specified" } )
        sys.stderr.write( f"flask.request.json['passwordlinkid'] is {flask.request.json['passwordlinkid']}\n" )
        link = db.PasswordLink.get( flask.request.json['passwordlinkid'] )
        if link is None:
            return flask.jsonify( { "error": "Invalid password link id" } )
        if link.expires < datetime.datetime.now(pytz.utc):
            return flask.jsonify( { "error": "Password reset link has expired" } )
        keys = Cryptodome.PublicKey.RSA.generate( 2048 )
        return flask.jsonify( { "privatekey": keys.exportKey().decode("UTF-8"),
                                "publickey": keys.publickey().exportKey().decode("UTF-8") } )
    except Exception as e:
        sys.stderr.write( f'{traceback.format_exc()}\n' )
        return flask.jsonify( { "error": f"Exception in GetKeys: {str(e)}" } )

@bp.route( '/changepassword', methods=['POST'] )
def changepassword():
    try:
        if not flask.request.is_json:
            return flask.jsonify( { 'error': '/auth/getkeys was expecting application/json' } )
        if not "passwordlinkid" in flask.request.json:
            return flask.jsonify( { "error": "Call to changepassword without passwordlinkid" } )
        if not "publickey" in flask.request.json:
            return flask.jsonify( { "error": "Call to changepassword without publickey" } )
        if not "privatekey" in flask.request.json:
            return flask.jsonify( { "error": "Call to changepassword without privatekey" } )

        with db.DB.get() as dbsess:
            pwlink = db.PasswordLink.get( flask.request.json['passwordlinkid'], curdb=dbsess )
            user = db.AuthUser.get( pwlink.userid, curdb=dbsess )
            user.pubkey = flask.request.json['publickey']
            user.privkey = flask.request.json['privatekey']
            dbsess.db.delete( pwlink )
            dbsess.db.commit()
            return flask.jsonify( { "status": "Password changed" } )
    except Exception as e:
        sys.stderr.write( f'{traceback.format_exc()}\n' )
        return flask.jsonify( { 'error': f'Exception in ChangePassword: {str(e)}' } )

@bp.route( '/isauth', methods=['POST'] )
def isauth():
    if ( 'authenticated' in flask.session ) and ( flask.session['authenticated'] ):
        return flask.jsonify( { 'status': True,
                                'username': flask.session["username"],
                                'useruuid': str( flask.session["useruuid"] ),
                                'useremail': flask.session["useremail"],
                                'userdisplayname': flask.session["userdisplayname"],
                               } )
    else:
        return flask.jsonify( { 'status': False } );


@bp.route( '/logout', methods=['POST'] )
def logout():
    flask.session['authenticated'] = False
    del flask.session['username']
    del flask.session['useruuid']
    del flask.session['useremail']
    del flask.session['userdisplayname']
    return flask.jsonify( { 'status': 'Logged out' } )
    
    
