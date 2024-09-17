#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate eo_pipelines_env

python -m pyproj_utils.metadata_tweaker --input-path $INPUT_PATH --output-path $OUTPUT_PATH --config-path $CONFIG_PATH