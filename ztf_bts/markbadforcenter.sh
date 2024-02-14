#!/bin/bash

for sn in `cat obj_with_ltcv.lis`; do
    echo "Doing " $sn
    python /curveball/bin/saltfitltcv.py $sn -p centering -t -i -m -e -f --save -n --versiontags centering
done

          
