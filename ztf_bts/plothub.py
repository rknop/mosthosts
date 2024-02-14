import numpy
import pandas

import matplotlib
from matplotlib import pyplot


def main():
    # These are the values I get with bts_z ; needed intrinsic dm = 0.206 to get χ²/ν = 1
    # datafile = "bts_mosthosts_btsz.csv"
    # scriptm = 24.120    # Most people use -19.081 (ref?)
    # alpha = 0.127
    # dalpha = 0.010
    # beta = 2.794
    # dbeta = 0.067

    # These are the values I get with desi_z, rejecting the rejects ; needed intrinsic dm = 0.15 to get χ²/ν = 1
    datafile = "bts_mosthosts_desiz.csv"
    scriptm = 24.120
    alpha = 0.132
    dalpha = 0.009
    beta = 2.328
    dbeta = 0.113
    dint = 0.15
    
    tickfontsize = 24
    labelfontsize = 32
    insetlabelfontsize = 14
    insettickfontsize = 12
    
    df = pandas.read_csv( datafile )

    matplotlib.rc('font', **{'family': 'serif', 'serif': ['Computer Modern']})
    matplotlib.rc('text', usetex=True)
    
    fig = pyplot.figure( figsize=(10,6), layout='tight' )
    ax = fig.add_subplot( 1, 1, 1 )
    mbcor = df.mbstar + alpha * df.x1 - beta * df.c
    dmbcor = numpy.sqrt( df.dmbstar ** 2 + ( dalpha * df.x1 ) ** 2 + ( alpha * df.dx1 ) ** 2
                         + ( dbeta * df.c ) ** 2 + ( beta * df.dc ) **2  + dint **2 )
    df['mbcor'] = mbcor
    df['dmbcor'] = dmbcor
    
    # Cuts from
    # @ARTICLE{pbb+2021,
    #    author = {{Popovic}, Brodie and {Brout}, Dillon and {Kessler}, Richard and {Scolnic}, Dan and {Lu}, Lisa},
    #     title = "{Improved Treatment of Host-galaxy Correlations in Cosmological Analyses with Type Ia Supernovae}",
    #   journal = {\apj},
    #  keywords = {Cosmology, Astrophysical dust processes, Cosmological models, Cosmological parameters, Origin of the universe, Dark energy, 343, 99, 337, 339, 1186, 351, Astrophysics - Cosmology and Nongalactic Astrophysics},
    #      year = 2021,
    #     month = may,
    #    volume = {913},
    #    number = {1},
    #       eid = {49},
    #     pages = {49},
    #       doi = {10.3847/1538-4357/abf14f},
    # archivePrefix = {arXiv},
    #    eprint = {2102.01776},
    # primaryClass = {astro-ph.CO},
    #    adsurl = {https://ui.adsabs.harvard.edu/abs/2021ApJ...913...49P},
    #  adsnote = {Provided by the SAO/NASA Astrophysics Data System}
    # }
    wbad = ( ( df['c'] > 0.3 ) | ( df['c'] < -0.3 ) |
             ( df['x1'] > 3 ) | ( df['x1'] < -3 ) |
             ( df['dc'] > 0.2 ) | ( df['dx1'] > 1 ) )
    wgood = ~wbad
    
    ax.errorbar( df.z[wgood], mbcor[wgood], dmbcor[wgood], linestyle='none', marker='o', color='blue', zorder=1 )
    ax.errorbar( df.z[wbad], mbcor[wbad], dmbcor[wbad], linestyle='none', marker='o', color='red',
                 fillstyle='none', zorder=2 )
    zs = numpy.arange( 0.0075, max( df.z )+0.002, 0.001 )
    fitmbstar = scriptm + 5 * numpy.log10( zs )
    
    ax.plot( zs, fitmbstar, color='green' )

    ax.set_xlabel( r'$z$', fontsize=labelfontsize )
    ax.set_ylabel( r'$m_{bcor}$', fontsize=labelfontsize )
    ax.tick_params( "both", labelsize=tickfontsize )

    ax = fig.add_axes( [ 0.65, 0.25, 0.3, 0.4 ] )
    ax.set_xlabel( r'$m_{bcor}-\mathcal{M}-5\,\log{z}$', fontsize=insetlabelfontsize )
    ax.set_xticks( [ -0.6, -0.4, -0.2, 0., 0.2, 0.4, 0.6 ] )
    ax.tick_params( "both", labelsize=insettickfontsize )
    delta = df.mbcor[wgood] - scriptm - 5 * numpy.log10( df.z[wgood] )
    print( f"delta.mean()={delta.mean()}, delta.std()={delta.std()}" )
    ax.hist( delta, bins=numpy.arange( -0.65, 0.65, 0.05 ), color='blue' )
    
    fig.savefig( "it.svg" )
    pyplot.close( fig )

    # Talk about the outliers

    for i, row in df.iterrows():
        fitmbstar = scriptm + 5 * numpy.log10( row['z'] )
        if ( numpy.abs( mbcor[i] - fitmbstar ) > 3.*dmbcor[i] ):
            bad = " (bad)" if not wgood[i] else "(good)"
            print( f"{bad}  {row['sn']} at z={row['z']} has mbcor={mbcor[i]} but fitmbstar={fitmbstar}" )

    # Write a datafile
    df['passcut'] = 'yes'
    df.loc[wbad, 'passcut'] = 'no'

    df.to_csv( 'hubbleplotpoints.csv', index=False, columns=['z', 'mbcor', 'dmbcor', 'passcut'] )
            
            
# ======================================================================

if __name__ == "__main__":
    main()
