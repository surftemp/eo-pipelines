#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate eo_pipelines_env

python -m pyproj_utils.time_stacker --input-folders $INPUT_FOLDER --output-path=$OUTPUT_PATH