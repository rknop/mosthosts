#!/bin/bash

for sn in `cat obj_with_ltcv.lis`; do
    echo "Doing " $sn
    python /curveball/bin/saltfitltcv.py $sn \
           --photometry-version default \
           --versiontag desi_z \
           --t0-bound \
           --save \
           --errorpuff \
           --force
done

