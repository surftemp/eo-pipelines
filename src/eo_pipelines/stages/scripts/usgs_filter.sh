#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate cartopy_env

python -m eo_pipelines.stages.tools.usgs_filter $SCENES_CSV_PATH \
      --lat-min $LAT_MIN --lat-max $LAT_MAX \
      --lon-min $LON_MIN --lon-max $LON_MAX \
      --min-overlap-fraction $MIN_OVERLAP_FRACTION $CHECK_VERSION



