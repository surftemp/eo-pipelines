#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate landsat2nc_env

python `dirname $0`/adjust_tool.py $INPUT_PATH $OUTPUT_PATH $ADJUST_OPTIONS
