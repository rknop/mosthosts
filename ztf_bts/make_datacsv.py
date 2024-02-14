import sys
import psycopg2.extras

import db

def main():
    # salt2tag = "bts_z"
    salt2tag = "desi_z"
    snlist = 'ztf_with_iron_z_justmatch.lis'

    sne = set()
    with open( snlist ) as ifp:
        # Strip header
        junk = ifp.readline()
        for line in ifp.readlines():
            commadex = line.index(",")
            sne.add( line[:commadex] )

    with db.DB.get() as dbo:
        con = dbo.db.connection().connection
        cursor = con.cursor( cursor_factory=psycopg2.extras.RealDictCursor )
        q = ( "SELECT DISTINCT ON (o.name) "
              "  o.name,s.z,s.dz,s.mbstar,s.dmbstar,s.x1,s.dx1,s.c,s.dc,s.chisq,s.dof "
              "FROM object o "
              "INNER JOIN salt2fit s ON o.id=s.object_id "
              "INNER JOIN salt2fit_versiontag svt ON s.id=svt.salt2fit_id "
              "INNER JOIN versiontag v ON svt.versiontag_id=v.id "
              "WHERE v.name=%(version)s "
              "ORDER BY o.name" )
        cursor.execute( q, { "version": salt2tag } )

        print( "sn,z,dz,mbstar,dmbstar,x1,dx1,c,dc,chisq,dof" )
        fields = [ 'name', 'z', 'dz', 'mbstar', 'dmbstar', 'x1', 'dx1', 'c', 'dc', 'chisq', 'dof' ]
        for row in cursor.fetchall():
            if row['name'] in sne:
                print( ",".join( [ str(row[f]) for f in fields ] ) )
                sne.remove( row['name'] )
                chisq_nu = row['chisq'] / row['dof']
                if ( chisq_nu < 0.98 ) or ( chisq_nu > 1.02 ):
                    sys.stderr.write( f"WARNING': {row['name']} has χ²/ν = {chisq_nu}\n" )

    if len(sne) != 0:
        sys.stderr.write( f"{len(sne)} SNe didn't have a salt fit with version {salt2tag}\n" )
        sys.stderr.write( f"{str(sne)}\n" )
        
             

# ======================================================================

if __name__ == "__main__":
    main()
