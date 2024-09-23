#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate xarray_regridder_env

xesmf_regridder $INPUT_PATH $GRID_PATH $OUTPUT_FOLDER $CACHE_PATH --max-distance $MAX_DISTANCE