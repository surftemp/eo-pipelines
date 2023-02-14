#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate landsat2nc_env

python -m eo_pipeline_stages.adjust_tool $INPUT_PATH $OUTPUT_PATH $ADJUST_OPTIONS