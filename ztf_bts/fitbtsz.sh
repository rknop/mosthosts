# #!/bin/bash
# 
# for sn in `cat obj_with_ltcv.lis`; do
#     echo "Doing " $sn
#     python /curveball/bin/saltfitltcv.py $sn \
#            --photometry-version default \
#            --versiontag bts_z \
#            --t0-bound \
#            --iterate-sigreject \
#            --mark-rejected-bad \
#            --save \
#            --no-tag-default \
#            --errorpuff \
#            --force
# done
# 
