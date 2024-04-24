#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate pyproj_env

python -m pyproj_utils.regrid $INPUT_PATH $GRID_PATH $OUTPUT_PATH \
  --variables $VARIABLES --mode $MODE \
  --source-x $SOURCE_X --source-y $SOURCE_Y --source-crs $SOURCE_CRS \
  --target-x $TARGET_X --target-y $TARGET_Y --target-crs $TARGET_CRS