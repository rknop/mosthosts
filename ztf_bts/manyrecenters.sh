#!/bin/bash

for sn in `cat obj_with_ltcv.lis`; do
    echo "Doing " $sn
    python /curveball/bin/recenter.py $sn -p rc/${sn}.svg --update-position
done
