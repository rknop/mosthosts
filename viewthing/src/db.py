import sys
import uuid
import sqlalchemy as sa
import sqlalchemy.orm
from sqlalchemy.dialects.postgresql import UUID as sqlUUID

Base = sqlalchemy.orm.declarative_base()

class DB(object):
    engine = None
    sessionfac = None

    @classmethod
    def DBinit( cls, dbhost, dbport, dbuser, dbpasswd, dbname ):
        if cls.engine is None:
            cls.engine = sa.create_engine( f"postgresql+psycopg2://{dbuser}:{dbpasswd}@{dbhost}:{dbport}/{dbname}" )
            cls.sessionfac = sa.orm.sessionmaker( bind=cls.engine, expire_on_commit=False )

    @staticmethod
    def get( db=None ):
        if db is None:
            return DB()
        else:
            return DB( db.db )

    def __init__( self, db=None ):
        if db is None:
            if DB.engine is None:
                raise RuntimeError( "DB not initialized" )
            self.db = DB.sessionfac()
            self.mustclose = True
        else:
            self.db = db
            self.mustclose = False

    def __enter__( self ):
        return self

    def __exit__( self, exc_type, exc_val, exc_tb ):
        self.close()

    def __del__( self ):
        self.close()

    def close( self ):
        if self.mustclose and self.db is not None:
            self.db.close()
            self.db = None

# ======================================================================
# Never instantiate this directly, it's used for subclassing only

class HasPrimaryID(object):
    id = sa.Column( sqlUUID(as_uuid=True), primary_key=True, default=uuid.uuid4 )
    
    @classmethod
    def get( cls, id, curdb=None ):
        sys.stderr.write( f"Trying to make a UUID out of {str(id)}, type {type(id)}\n" )
        id = id if isinstance( id, uuid.UUID) else uuid.UUID( id )
        with DB.get(curdb) as db:
            q = db.db.query(cls).filter( cls.id==id )
            if q.count() > 1:
                raise ErrorMsg( f'Error, {cls.__name__} {id} multiply defined!  This shouldn\'t happen.' )
            if q.count() == 0:
                return None
            return q[0]

# ======================================================================

class AuthUser(Base, HasPrimaryID):
    __tablename__ = "authuser"

    username = sa.Column( sa.Text, nullable=False, unique=True, index=True )
    displayname = sa.Column( sa.Text, nullable=False )
    email = sa.Column( sa.Text, nullable=False, index=True )
    pubkey = sa.Column( sa.Text )
    privkey = sa.Column( sa.Text )
    lastlogin = sa.Column( sa.DateTime(timezone=True), default=None )
    
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

class PasswordLink(Base, HasPrimaryID):
    __tablename__ = "passwordlink"

    userid = sa.Column( sqlUUID(as_uuid=True), sa.ForeignKey("authuser.id", ondelete="CASCADE"), index=True )
    expires = sa.Column( sa.DateTime(timezone=True) )
    
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

