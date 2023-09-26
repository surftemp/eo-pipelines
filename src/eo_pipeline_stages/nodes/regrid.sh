#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate landsat2nc_env

python -m landsat2nc.landsat2nc $SCENE_PATH $OUTPUT_PATH \
      --lat-min $LAT_MIN --lat-max $LAT_MAX \
      --lon-min $LON_MIN --lon-max $LON_MAX \
      --resolution $RESOLUTION \
      --bands $BANDS --method nearest --interpolate $INCLUDE_ANGLES \
      --export_oli_as $EXPORT_OLI_AS

