#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate landsat2nc_env

python -m landsat2nc.stacking.time_stacker $INPUT_FOLDER $OUTPUT_PATH

exit 0