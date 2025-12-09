#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate eo_pipelines_env

python -m eo_pipelines.stages.tools.create_grid $OUTPUT_PATH \
        --lat-min $LAT_MIN --lat-max $LAT_MAX \
        --lon-min $LON_MIN --lon-max $LON_MAX \
        --resolution $RESOLUTION