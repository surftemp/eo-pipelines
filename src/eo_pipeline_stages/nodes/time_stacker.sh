#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate pyproj_env

python -m pyproj_utils.time_stacker --input-folders=$INPUT_FOLDERS --output-path=$OUTPUT_PATH