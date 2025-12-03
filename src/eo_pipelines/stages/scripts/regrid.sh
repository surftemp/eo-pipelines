#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate xarray_regridder_env

xarray_regridder $INPUT_PATH $GRID_PATH $OUTPUT_PATH \
  --variables $VARIABLES \
  --source-x $SOURCE_X --source-y $SOURCE_Y --source-crs $SOURCE_CRS \
  --target-x $TARGET_X --target-y $TARGET_Y --target-crs $TARGET_CRS $CHECK_VERSION