from datetime import datetime, timedelta
import dateutil.parser
import pytz
import uuid
import sqlalchemy
import sqlalchemy.ext.declarative
import sqlalchemy.pool
from sqlalchemy.dialects.postgresql import UUID as sqlUUID

Base = sqlalchemy.ext.declarative.declarative_base()

# ======================================================================

class DB(object):
    """An object that encapsulates a SQLAlchemy / Postgres DB connection.
    
    Get one by calling dbo=DB.get(current_dbo), where current_dbo is
    either None or a DB object.  You can call tll DB.get as part of a
    with statement ( "with DB.get(current_dbo) as dbo:" )

    Call dbo.close() when done; this happens automatically if the object
    was created at the start of a with block.

    Once you have the object, the db field is the SQLAlchemy session.

    Before the first call to DB.get(), must call DB.setdbparams() with all arguments.

    """
    _engine = None
    _sessionfac = None
    _dbparamsset = False
    
    @classmethod
    def setdbparams( cls, user, password, host, port, database ):
        cls._user = user
        cls._password = password
        cls._host = host
        cls._port = port
        cls._database = database
        cls._dbparamsset = True
    
    @classmethod
    def DBinit( cls ):
        if cls._engine is None:
            if not cls._dbparamsset:
                # Uncomment this next line, and comment out the
                #  exception, if you've replaced setdbparams() with
                #  something that has defaults
                # cls.setdbparams()
                raise RuntimeError( "DB Parameters not set." )
            cls._engine = sqlalchemy.create_engine(f'postgresql://{cls._user}:{cls._password}@{cls._host}:{cls._port}'
                                                   f'/{cls._database}', poolclass=sqlalchemy.pool.NullPool )
            # ROB -- remove the following line?
            # DeferredReflection.prepare( DB._engine )
            cls._sessionfac = sqlalchemy.orm.sessionmaker( bind=cls._engine, expire_on_commit=False )

    @staticmethod
    def get( dbo=None ):
        """Get a DB object.
        
        dbo - either a DB object, or None.  If None, creates a new
        SQLAlchmey session, which is then available in the db field of
        the returned DB object.  If not None, returns a DB object that
        has the same SQLAlchemy session as the passed DB object.

        """
        if dbo is None:
            return DB()
        else:
            return DB( dbo.db )

    def __init__( self, db=None ):
        """Never call this directly; call DB.get()"""

        self.mustclose = False
        if db is None:
            if DB._engine is None:
                DB.DBinit()
            self.db = DB._sessionfac()
            self.mustclose = True
        else:
            self.db = db

    def __enter__( self ):
        return self

    def __exit__( self, exc_type, exc_val, exc_tb ):
        self.close()
            
    def __del__( self ):
        self.close()

    def close( self ):
        """Call this when done with the DB object.  This gets called automatically if DB.get() was in a with statement."""
        if self.mustclose and self.db is not None:
            self.db.close()
            self.db = None

# ======================================================================-
# Utility functions

NULLUUID = uuid.UUID( '00000000-0000-0000-0000-000000000000' )

def asUUID( val, canbenone=True ):
    """Pass either None, as tring, or a uuid.UUID.  Return either None or uuid.UUID.
    
    If canbenone is True, passing None returns None.  Otherwise, passing None returns
    uuid.UUID('00000000-0000-0000-0000-000000000000').

    Will throw a ValueError if val isn't properly formatted.
    
    """

    if val is None:
        if canbenone:
            return None
        else:
            return NULLUUID
    if isinstance( val, uuid.UUID ):
        return val
    else:
        return uuid.UUID( val )

def asDateTime( string ):
    """Pass either a datetime.datetime or a string.  Returns a datetime.datetime.
    
    If a string, must be something that dateutil.parser.parse can handle.

    Doesn't do anything to take care of timezone aware vs. timezone
    unaware dates.  It probably should.  Dealing with that is always a
    nightmare.

    """

    if string is None:
        return None
    if isinstance( string, datetime ):
        return string
    if not isinstance( string, str ):
        raise RuntimeError( f'Error, must pass either a datetime or a string to asDateTime, not a {type(string)}' )
    string = string.strip()
    if len(string) == 0:
        return None
    try:
        dateval = dateutil.parser.parse( string )
        return dateval
    except Exception as e:
        if hasattr( e, 'message' ):
            sys.stderr.write( f'Exception in asDateTime: {e.message}\n' )
        else:
            sys.stderr.write( f'Exception in asDateTime: {e}\n' )
        raise RuntimeError( f'Error, {string} is not a valid date and time.' )

    
# ======================================================================

class AuthUser(Base):
    __tablename__ = "authuser"

    id = sqlalchemy.Column( sqlUUID(as_uuid=True), primary_key=True, default=uuid.uuid4 )
    username = sqlalchemy.Column( sqlalchemy.Text, nullable=False, unique=True, index=True )
    displayname = sqlalchemy.Column( sqlalchemy.Text, nullable=False )
    email = sqlalchemy.Column( sqlalchemy.Text, nullable=False, index=True )
    pubkey = sqlalchemy.Column( sqlalchemy.Text )
    privkey = sqlalchemy.Column( sqlalchemy.Text )
    lastlogin = sqlalchemy.Column( sqlalchemy.DateTime(timezone=True), default=None )
    
    @classmethod
    def get( cls, id, curdb=None, cfg=None ):
        id = id if isinstance( id, uuid.UUID) else uuid.UUID( id )
        with DB.get( curdb ) as db:
            q = db.db.query(cls).filter( cls.id==id )
            if q.count() > 1:
                raise ErrorMsg( f'Error, {cls.__name__} {id} multiply defined!  This shouldn\'t happen.' )
            if q.count() == 0:
                return None
            return q[0]

    @classmethod
    def getbyusername( cls, name, curdb=None ):
        with DB.get( curdb ) as db:
            q = db.db.query(cls).filter( cls.username==name )
            return q.all()

    @classmethod
    def getbyemail( cls, email, curdb=None ):
        with DB.get( curdb ) as db:
            q = db.db.query(cls).filter( cls.email==email )
            return q.all()

# ======================================================================

class PasswordLink(Base):
    __tablename__ = "passwordlink"

    id = sqlalchemy.Column( sqlUUID(as_uuid=True), primary_key=True, default=uuid.uuid4 )
    userid = sqlalchemy.Column( sqlUUID(as_uuid=True), sqlalchemy.ForeignKey("authuser.id", ondelete="CASCADE"), index=True )
    expires = sqlalchemy.Column( sqlalchemy.DateTime(timezone=True) )
    
    @classmethod
    def new( cls, userid, expires=None, curdb=None ):
        if expires is None:
            expires = datetime.now(pytz.utc) + timedelta(hours=1)
        else:
            expires = asDateTime( expires )
        with DB.get(curdb) as db:
            link = PasswordLink( userid = asUUID(userid),
                                 expires = expires )
            db.db.add( link )
            db.db.commit()
            return link

    @classmethod
    def get( cls, uuid, curdb=None ):
        with DB.get(curdb) as dbo:
            q = dbo.db.query( PasswordLink ).filter( PasswordLink.id==uuid )
            if q.count() == 0:
                return None
            else:
                return q.first()
