import pathlib
import flask
import flask_session
import logging

from . import db

def create_app():
    app = flask.Flask(  __name__ )
    app.logger.setLevel( logging.INFO )
    app.config.from_mapping(
        SECRET_KEY='blah',
        SESSION_COOKIE_PATH='/',
        SESSION_TYPE='filesystem',
        SESSION_PERMANENT=True,
        SESSION_USE_SIGNER=True,
        SESSION_FILE_DIR='/sessions',
        SESSION_FILE_THRESHOLD=1000,
    )

    server_session = flask_session.Session( app )

    db.DB.setdbparams( 'postgres', 'fragile', 'postgres', 5432, 'test_rkwebutil' )
    
    from . import flaskauth
    flaskauth.RKAuthConfig.email_from = 'rkwebutil test <nobody@nowhere.org>'
    flaskauth.RKAuthConfig.email_subject = 'rkwebutil test password reset'
    flaskauth.RKAuthConfig.email_system_name = 'the rkwebutil test webserver'
    flaskauth.RKAuthConfig.smtp_server = 'mailhog'
    flaskauth.RKAuthConfig.smtp_port = 1025
    flaskauth.RKAuthConfig.smtp_use_ssl = False
    flaskauth.RKAuthConfig.smtp_username = None
    flaskauth.RKAuthConfig.smtp_password = None
    flaskauth.RKAuthConfig.webap_url = 'http://flaskserver:8081/auth'
    # A truly bizarre thing -- the db.DB class inside flaskauth is *not*
    #   the same as the db.DB class here.  I don't really get that, but
    #   flask must do something weird with its blueprints.  Effectively,
    #   that means I have to initialize the DB in flaskauth separately
    flaskauth.RKAuthConfig.setdbparams( 'postgres', 'fragile', 'postgres', 5432, 'test_rkwebutil' )
    app.register_blueprint( flaskauth.bp )

    @app.route('/')
    def hello_world():
        username = flask.session['username'] if 'username' in flask.session else '(None)'
        userdisplayname = flask.session['userdisplayname'] if 'userdisplayname' in flask.session else '(None)'
        authenticated = ( 'authenticated' in flask.session ) and flask.session['authenticated']
        return flask.render_template( 'ap.html', username=username, userdisplayname=userdisplayname,
                                      authenticated=authenticated )

    return app
