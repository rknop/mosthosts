import argparse
import astropy.units as u
import math

parser = argparse.ArgumentParser()
parser.add_argument( "scriptm", type=float )
parser.add_argument( "-m", "--mbabs", type=float, default=-19.4 )
args = parser.parse_args()

c = 299792458 * u.m / u.s
d0 = 10 * u.pc

H0 = 10 ** ( - ( args.scriptm - args.mbabs ) / 5. ) * c / d0

print( H0.to( u.km / u.s / u.Mpc ) )

