#!/usr/bin/env bash
#SBATCH -J buildrefs
#SBATCH -o buildrefs.out
#SBATCH --ntasks=1
#SBATCH -A m2218
#SBATCH --constraint=cpu
#SBATCH -t 12:00:00
#SBATCH --qos=shared
#SBATCH --cpus-per-task=13
#SBATCH --mem=12000

podman-hpc run \
           --mount type=bind,source=/global/homes/r/raknop/curveball,target=/curveball \
           --mount type=bind,source=/global/homes/r/raknop/secrets,target=/secrets \
           --mount type=bind,source=/pscratch/sd/r/raknop,target=/data \
           --mount type=bind,source=/global/homes/r/raknop/desi/mosthosts,target=/mosthosts \
           --workdir /mosthosts/ztf_bts \
           --env CURVEBALL_CONFIG=curveballconfig \
           -it rknop/curveball:daedalus \
           python buildrefs.py
