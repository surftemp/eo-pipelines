#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate landsat2nc_env

python -m landsat2nc.grouping.grouper $GROUPING_SPEC_PATH $OUTPUT_PATH
