#!/bin/bash

#SBATCH --partition=high-mem
#SBATCH --job-name=combine_tiles
#SBATCH -o combine_tiles.out
#SBATCH -e combine_tiles.err
#SBATCH -t 48:00:00
#SBATCH --mem=512G

conda activate rioxarray_env

python ~/tools/xesmf/src/xesmf_utils/regrid_latlon.py dem_tiles dem_grid.nc --output-aggregation-path=dem.nc --output-aggregation-function=max --output-aggregation-chunksize=1000