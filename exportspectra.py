import os
import io
import sys
import pathlib
import logging
import traceback
import multiprocessing
import queue

import numpy
import pandas

from lib.mosthosts_desi import MostHostsDesi
from lib.desi_specfinder import TargetNotFound, SpectrumFinder

outdir = pathlib.Path( 'exported_spectra' )

numprocs = 10

_logger = logging.getLogger( __name__ )
if not _logger.hasHandlers():
    _logout = logging.StreamHandler( sys.stderr )
    _logger.addHandler( _logout )
    _logout.setFormatter( logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s' ) )
_logger.setLevel( logging.INFO )
# _logger.setLevel( logging.DEBUG )

# ======================================================================

def pearson_hash( string ):
    # https://en.wikipedia.org/wiki/Pearson_hashing

    # I did this with random.shuffle(), proving that 77 is the most random 8-bit number
    byteperm = numpy.array( [77, 130, 147, 35, 240, 54, 126, 114, 153, 101, 146, 155, 237, 148, 231, 80, 86,
                             216, 74, 90, 133, 107, 223, 154, 42, 12, 245, 193, 28, 16, 180, 174, 58, 69, 33,
                             105, 125, 182, 73, 238, 191, 100, 150, 254, 1, 135, 242, 10, 251, 30, 5, 88, 81,
                             221, 36, 31, 131, 185, 8, 98, 97, 190, 247, 112, 123, 234, 219, 120, 60, 222, 66,
                             96, 83, 13, 4, 144, 253, 19, 24, 172, 45, 179, 93, 11, 46, 207, 59, 99, 39, 44,
                             53, 220, 118, 34, 189, 41, 55, 109, 25, 40, 57, 229, 250, 102, 27, 183, 164, 178,
                             82, 212, 129, 197, 79, 186, 252, 116, 75, 134, 187, 94, 23, 0, 89, 2, 132, 206,
                             217, 151, 249, 175, 210, 194, 181, 127, 61, 171, 49, 142, 233, 68, 255, 37, 62,
                             244, 92, 152, 196, 21, 202, 248, 124, 7, 84, 227, 218, 176, 17, 199, 110, 103,
                             63, 149, 115, 170, 47, 50, 243, 85, 140, 67, 228, 137, 162, 95, 205, 169, 156,
                             166, 230, 157, 104, 15, 198, 111, 64, 241, 113, 209, 208, 215, 14, 18, 201, 239,
                             177, 70, 136, 72, 143, 87, 78, 203, 224, 3, 192, 225, 141, 119, 232, 168, 204,
                             188, 43, 165, 22, 76, 184, 145, 48, 211, 52, 106, 108, 51, 9, 91, 235, 167, 246,
                             32, 26, 200, 71, 226, 128, 56, 121, 163, 38, 195, 29, 161, 138, 213, 117, 139,
                             20, 122, 65, 236, 173, 6, 159, 158, 160, 214],
                            dtype=numpy.ubyte )
    blob = string.encode( 'utf-8' )

    hash = numpy.array( [0], dtype=numpy.ubyte )

    for b in blob:
        hash[0] = byteperm[ hash[0] ^ b ]

    return f"{hash[0]:02x}"

# ======================================================================

def host_subprocessor( pipe, dbpasswd, logger ):
    me = multiprocessing.current_process()
    logger.info( f"host_subprocessor starting: {me.name} PID {me.pid}" )
    try:
        done = False
        while not done:
            command = pipe.recv()

            if command['command'] == 'die':
                logger.info( f"{me.name} : got die command" )
                done = True

            elif command['command'] == 'snhost':
                snname = command['snname']
                cleansnname = snname.replace( "/", "_" )
                host = command['host']
                ra = command['ra']
                dec = command['dec']
                phash = pearson_hash( snname )

                logger.info( f"{me.name} doing {snname} host {host}" )

                # **** HACK ALERT ; only keep 57
                if phash != '57':
                    pipe.send( [] )
                    continue
                # ****
                
                dex = 0
                retspec = []

                finder = SpectrumFinder( ra, dec, names='{snname}_{host}',
                                         desipasswd=dbpasswd, collection='daily', logger=logger )
                for targid in finder.targetids:
                    specinfos = finder.info_for_targetid( targid )
                    logger.debug( f"...{len(specinfos)} spectra for targetid {targid} of {snname} {host}" )
                    for i, specinfo in enumerate( specinfos ):
                        try:
                            spec = finder.get_spectrum( targid, specinfo['tileid'],
                                                        specinfo['petal_loc'], specinfo['night'] )
                        except FileNotFoundError as ex:
                            logger.error( f"Failed to find file for {targid} of {snname}_{host}: {ex}; skipping" )
                            continue
                        night = str( specinfo['night'] )
                        strnight = f'{night[0:4]}-{night[4:6]}-{night[6:8]}'
                        try:
                            outfile = outdir / phash[0] / phash[1] / f'{cleansnname}_host{host}_{dex}.csv'
                        except Exception as ex:
                            strio = io.StringIO()
                            traceback.print_exc( file=strio )
                            strio.write( f"\nphash={phash}" )
                            logger.error( strio.getvalue() )
                        dfluxen = numpy.sqrt( 1 / spec.ivar['brz'] )
                        #### dflux[ spec.ivar['brz'] <= 0 ] = sys.float_info.max
                        logger.debug( f"{me.name} writing spectrum {outfile.name} for "
                                      f"{cleansnname} host {host} dex {dex}" )
                        with open(outfile, "w") as ofp:
                            ofp.write( "lambda flux dflux\n" )
                            # TODO : figure out what the array of flux arrays means!
                            for wave, flux, dflux in zip( spec.wave['brz'], spec.flux['brz'][0], dfluxen[0] ):
                                ofp.write( f"{wave:.2f} {flux:.5e} {dflux:.5e}\n" )
                        retspec.append( ( snname, host, targid, dex, strnight, phash[0], phash[1], outfile ) )
                        dex += 1
                pipe.send( retspec )
            else:
                logger.error( f"{me.name} unknown command {command['command']}, ignoring" )

    except EOFError as ex:
        logger.info( f"Pipe received EOF, closing down process {me.name} PID {me.pid}" )
        return
    except Exception as ex:
        strio = io.StringIO()
        strio.write( f"Process {me.name} PID {me.pid} returning after exception: {ex}\n" )
        traceback.print_tb( file=strio )
        logger.error( strio.getvalue() )
        return

    logger.info( f"{me.name} PID {me.pid} exiting" )
    
# ======================================================================

def main():

    # Make output directories
    # To avoid having too big of directories, we're going to make two-level subdirectories

    _logger.info( "Making output directories" )
    csvs = {}
    for i in '0123456789abcdef':
        csvs[i] = {}
        for j in '0123456789abcdef':
            direc = outdir / i / j
            direc.mkdir( exist_ok=True, parents=True )
            csvs[j] = []
                            
    with open( pathlib.Path(os.getenv("HOME")) / "secrets/decatdb_desi_desi" ) as ifp:
        (dbuser, dbpasswd) = ifp.readline().strip().split()

    _logger.info( "Loading mosthosts" )
    mosthosts = MostHostsDesi( dbuser=dbuser, dbpasswd=dbpasswd, logger=_logger, release='daily', force_regen=False )
    haszdf = mosthosts.haszdf.sort_index( level=['sn_name_sp', 'hostnum', 'targetid', 'tileid', 'petal', 'night'] )

    _logger.info( "Launching processes" )
    procs = []
    pipes = []
    for i in range( numprocs ):
        mypipe, theirpipe = multiprocessing.Pipe( True )
        pipes.append( mypipe )
        proc = multiprocessing.Process( target=host_subprocessor, args=( theirpipe, dbpasswd, _logger ),
                                        name=f'proc {i}' )
        proc.start()
    idleprocs = list( range( numprocs ) )
    busyprocs = []


    _logger.info( "Iterating" )
    snnames = []
    hosts = []
    targids = []
    dexen = []
    nights = []
    phash0s = []
    phash1s = []
    outfiles = []
    lastsn = None
    lasthost = None

    def appendvals( rval ):
        snnames.append( rval[0] )
        hosts.append( rval[1] )
        targids.append( rval[2] )
        dexen.append( rval[3] )
        nights.append( rval[4] )
        phash0s.append( rval[5] )
        phash1s.append( rval[6] )
        outfiles.append( rval[7] )

    done = False
    # ****
    # tmp = mosthosts.haszdf.iloc[0:30].reset_index()
    # nl = '\n'
    # _logger.info( f"Going to do:{nl}{nl.join([ f'{r.sn_name_sp} {r.hostnum} {r.targetid} {r.tileid} {r.petal} {r.night}' for r in tmp.itertuples()])} " )
    # sys.exit(20)
    # ****

    irate = mosthosts.haszdf.itertuples()
    # *** HACK ALERT: just do some
    # irate = mosthosts.haszdf.iloc[15290:15790].itertuples()
    # ****
    while not done:

        kaglorky = False
        while len( idleprocs ) > 0:
            if not kaglorky:
                kaglorky = True
                _logger.debug( "Starting idleprocs loop" )
            try:
                tup = next( irate )
            except StopIteration as ex:
                done = True
                break

            if not done:
                if tup.zwarn != 0:
                    continue

                snname = tup.Index[0]
                snhost = tup.Index[1]
                if ( snname == lastsn ) and ( snhost == lasthost ):
                    continue
                lastsn = snname
                lasthost = snhost

                which = idleprocs.pop()
                busyprocs.append( which )
                _logger.info( f"Sending {snname} {snhost} to proc {which}" )
                pipes[ which ].send( { 'command': 'snhost',
                                       'snname': snname,
                                       'host': snhost,
                                       'ra': tup.ra,
                                       'dec': tup.dec } )
        if kaglorky:
            _logger.debug( "Finished idleprocs loop" )

        busydex = 0
        while busydex < len( busyprocs ):
            if pipes[busyprocs[busydex]].poll():
                rvals = pipes[busyprocs[busydex]].recv()
                if len(rvals) > 0:
                    _logger.info( f"Got {rvals[0][0]} {rvals[0][1]} ({len(rvals)} dexen) "
                                  f"from proc {busyprocs[busydex]}" )
                else:
                    _logger.warning( f"Got empty return from proc {busyprocs[busydex]}" )
                idleprocs.append( busyprocs[busydex] )
                del busyprocs[busydex]
                for rval in rvals:
                    appendvals( rval )
            else:
                busydex += 1

    _logger.info( "Done submitting jobs, waiting for running processes to finish." )
    while len(busyprocs) > 0:
        busydex = 0
        while busydex < len( busyprocs ):
            if pipes[busyprocs[busydex]].poll():
                rvals = pipes[busyprocs[busydex]].recv()
                del busyprocs[busydex]
                idleprocs.append( busydex )
                for rval in rvals:
                    appendvals( rval )
            else:
                busydex += 1

    _logger.info( "Telling processes to die" )
    for pipe in pipes:
        pipe.send( { 'command': 'die' } )
                
    _logger.info( "Building CSV files..." )
    outmess = pandas.DataFrame( { 'phash0': phash0s, 'phash1': phash1s, 'snname': snnames, 'host': hosts,
                                  'targid': targids, 'dex': dexen, 'night': nights, 'outfile': outfiles } )
    outmess.sort_values( [ 'phash0', 'phash1', 'snname', 'host', 'targid', 'dex' ], inplace=True )

    _logger.info( "...calculating variance-weighted redshifts for each targetid" )
    snzs = []
    snras = []
    sndecs = []
    zbars = []
    dzbars = []
    hostcomments = []
    for tup in outmess.itertuples():
        thismh = haszdf.xs( ( tup.snname, tup.host, tup.targid ),
                            level=( 'sn_name_sp', 'hostnum', 'targetid' ) )

        ok = True
        if len( thismh ) == 0:
            _logger.error( f"Didn't find {tup.snname} {tup.host} {tup.targid} in haszdf!" )
            ok = False
            snzs.append( -999. )
            snras.append( -999. )
            sndecs.append( -999. )
            hostcomments.append( "Something is broken" )
        else:
            if not numpy.all( thismh.sn_z.values == thismh.sn_z.values[0] ):
                _logger.warning( f"SN {sn_name_sp} host {hostnum} has divergent sn_z!" )
            snzs.append( thismh.sn_z.values[0] )
            snras.append( thismh.sn_ra.values[0] )
            sndecs.append( thismh.sn_dec.values[0] )
            hostcomments.append( f'host ra={thismh.iloc[0].ra:.5f} dec={thismh.iloc[0].dec:.5f}' )
            thismh = thismh[ thismh.zwarn == 0 ]
            if len( thismh ) == 0:
                _logger.error( f"No zwarn=0 for {tup.sname} {tup.host} {tup.targid}" )
                ok = False
        
        if ok and ( thismh.z.max() - thismh.z.min() > 0.001 ):
            _logger.warning( f"{tup.snname} {tup.host} {tup.targid}; it has divergent zs: "
                             f"{[ row.z for row in thismh.itertuples() ]}" )
            # ok = False

        if ok:
            zbars.append( ( thismh.z / thismh.zerr**2 ).sum() / ( 1 / thismh.zerr**2 ).sum() )
            dzbars.append( 1. / ( 1 / thismh.zerr**2 ).sum() )
        else:
            zbars.append( -999. )
            dzbars.append( -999.)

    outmess['z'] = zbars
    outmess['dz'] = dzbars
    outmess['snra'] = snras
    outmess['sndec'] = sndecs
    outmess['snz'] = snzs
    outmess['hostcomment'] = hostcomments

    fields = [ 'Obj. IAU-name*','Obj. internal-name*','Source Group-Id*',
               'RA','DEC','Obj. Type-Id','Redshift','Host-name','Host-redshift',
               'Obj. Prop-period value','Prop-period units','Assoc. Groups',
               'Ascii-filename*','FITS-filename*','Obs-date* [YYYY-MM-DD HH:MM:SS] / JD',
               'Instrument-Id*','Exp-time (sec)','WL Units-id','WL Medium-Id',
               'Flux Unit Coeff','Flux Units-Id','Flux Calib. By-Id','Extinction-Corrected-Id',
               'Observer/s','Reducer/s','Reduction-date [YYYY-MM-DD HH:MM:SS] / JD',
               'Aperture (Slit)','Dichroic','Grism','Grating','Blaze','Airmass','Hour Angle',
               'Spec Type-Id','Spec Quality-Id','Spec. Prop-period value','Prop-period units',
               'Assoc. Groups','Spec-Remarks','Publish (bibcode)','Contrib','Related-file1',
               'RF1 Comments','Related-file2','RF2 Comments' ]
    constants = { 'Source Group-Id*': 78,
                  'Obj. Type-Id': 1,
                  'Obj. Prop-period value': 'NULL',
                  'Prop-period units': 'NULL',
                  'Assoc. Groups': 'NULL',
                  'FITS-filename*': 'NULL',
                  'Instrument-Id*': 258,
                  'Exp-time (sec)': 'NULL',
                  'WL Units-id': 11,
                  'WL Medium-Id': 2,
                  'Flux Unit Coeff': 1,
                  'Flux Units-Id': 6,
                  'Flux Calib. By-Id': 'NULL',
                  'Extinction-Corrected-Id': 'NULL',
                  'Observer/s': 'DESI',
                  'Reducer/s': 'DESI',
                  'Reduction-date [YYYY-MM-DD HH:MM:SS] / JD': 'NULL',
                  'Aperture (Slit)': '1.47""',
                  'Dichroic': 'NULL',
                  'Grism': 'NULL',
                  'Grating': 'NULL',
                  'Blaze': 'NULL',
                  'Airmass': 'NULL',
                  'Hour Angle': 'NULL',
                  'Spec Type-Id': 20,
                  'Spec Quality-Id': 3,
                  'Spec. Prop-period value': 'NULL',
                  'Prop-period units': 'NULL',
                  'Assoc. Groups': 'NULL',
                  'Publish (bibcode)': 'NULL',
                  'Contrib': 'Sougmagnac et. al. 2024',
                  'Related-file1': 'NULL',
                  'RF1 Comments': 'NULL',
                  'Related-file2': 'NULL',
                  'RF2 Comments': 'NULL',
                 }
    messkws = { 'Obj. internal-name*': 'snname',
                'RA': 'snra',
                'DEC': 'sndec',
                'Redshift': 'snz',
                'Host-redshift': 'z',
                'Obs-date* [YYYY-MM-DD HH:MM:SS] / JD': 'night',
                'Spec-Remarks': 'hostcomment',
               }
    manuals = [ 'Host-name', 'Ascii-filename*', 'Obj. IAU-name*' ]

    _logger.info( f"About to write csv files, outmess has len(outmess) rows" )
    for phash0 in '0123456789abcdef':
        for phash1 in '0123456789abcdef':
            thismess = outmess[ ( outmess.phash0==phash0 ) & ( outmess.phash1==phash1 ) ]
            if len(thismess) == 0:
                continue
            _logger.info( f"Writing {phash0}{phash1}.csv ; {len(thismess)} rows" )
            with open( f'exported_spectra/{phash0}{phash1}.csv', 'w' ) as ofp:
                ofp.write( '"Obj. IAU-name*"\t"Obj. internal-name*"\t"Source Group-Id*"\t'
                           '"RA"\t"DEC"\t"Obj. Type-Id"\t"Redshift"\t"Host-name"\t"Host-redshift"\t'
                           '"Obj. Prop-period value"\t"Prop-period units"\t"Assoc. Groups"\t'
                           '"Ascii-filename*"\t"FITS-filename*"\t"Obs-date* [YYYY-MM-DD HH:MM:SS] / JD"\t'
                           '"Instrument-Id*"\t"Exp-time (sec)"\t"WL Units-id"\t"WL Medium-Id"\t'
                           '"Flux Unit Coeff"\t"Flux Units-Id"\t"Flux Calib. By-Id"\t"Extinction-Corrected-Id"\t'
                           '"Observer/s"\t"Reducer/s"\t"Reduction-date [YYYY-MM-DD HH:MM:SS] / JD"\t'
                           '"Aperture (Slit)"\t"Dichroic"\t"Grism"\t"Grating"\t"Blaze"\t"Airmass"\t"Hour Angle"\t'
                           '"Spec Type-Id"\t"Spec Quality-Id"\t"Spec. Prop-period value"\t"Prop-period units"\t'
                           '"Assoc. Groups"\t"Spec-Remarks"\t"Publish (bibcode)"\t"Contrib"\t"Related-file1"\t'
                           '"RF1 Comments"\t"Related-file2"\t"RF2 Comments"\n' )

                for row in thismess.itertuples():
                    first = True
                    _logger.debug( f"Adding to CSV for {pathlib.Path(getattr(row,'outfile')).name}" )
                    for kw in fields:
                        if first:
                            first = False
                        else:
                            ofp.write( "\t" )
                        if kw in constants.keys():
                            if isinstance( constants[kw], str ):
                                ofp.write( f'"{constants[kw]}"' )
                            else:
                                ofp.write( str(constants[kw]) )

                        elif kw in messkws:
                            val = getattr( row, messkws[kw] )
                            if kw == 'Redshift':
                                if val < 0:
                                    ofp.write( f'"NULL"' )
                                else:
                                    ofp.write( f"{val:.6f}" )
                            elif kw == 'Host-redshift':
                                ofp.write( f"{val:.6f}" )
                            else:
                                if isinstance( val, str ):
                                    ofp.write( f'"{val}"' )
                                else:
                                    ofp.write( str(val) )

                        elif kw in manuals:
                            mhentry = mosthosts.mosthosts.xs( row.snname, level='sn_name_sp' )
                            if kw == 'Host-name':
                                ofp.write( f'"host {getattr( row, "host" )}' )
                                if len(mhentry) > 1:
                                    ofp.write( f' ({len(mhentry)} host candidates)' )
                                ofp.write( '"' )
                            elif kw == 'Ascii-filename*':
                                outfile = getattr( row, "outfile" )
                                ofp.write( f'"{pathlib.Path(outfile).name}"' )
                            elif kw == 'Obj. IAU-name*':
                                mh0 = mhentry.iloc[0]
                                if ( mh0.sn_name_iau is not None ):
                                    ofp.write( f'"{mh0.sn_name_iau}"' )
                                elif ( mh0.sn_name_tns is not None ):
                                    ofp.write( f'"{mh0.sn_name_tns}"' )
                                else:
                                    ofp.write( '"NULL"' )
                            else:
                                _logger.error( "Error 27B/6" )

                        else:
                            _logger.error( f"Keyword not found: {kw}\n" )
                    ofp.write( "\n" )
            
    

# ======================================================================

if __name__ == "__main__":
    main()
