#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate pyproj_env

python -m pyproj_utils.grouper $GROUPING_SPEC_PATH $OUTPUT_PATH
