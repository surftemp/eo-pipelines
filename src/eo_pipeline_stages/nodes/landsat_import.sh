#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate rioxarray_env

echo run_landsat_importer $SCENE_PATH $OUTPUT_PATH \
      --min-lat $LAT_MIN --max-lat $LAT_MAX \
      --min-lon $LON_MIN --max-lon $LON_MAX \
      --bands $BANDS

run_landsat_importer $SCENE_PATH $OUTPUT_PATH \
      --min-lat $LAT_MIN --max-lat $LAT_MAX \
      --min-lon $LON_MIN --max-lon $LON_MAX \
      --bands $BANDS $INJECT_METADATA

