#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate pyproj_env

python -m pyproj_utils.missing_filter --input-folders=$INPUT_FOLDERS --output-path=$OUTPUT_PATH --bands $BANDS --missing-fraction-threshold $MISSING_FRACTION_THRESHOLD