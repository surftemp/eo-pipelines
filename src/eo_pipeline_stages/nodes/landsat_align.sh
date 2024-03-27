#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate xesmf_env

python -m xesmf_utils.landsat_align $INPUT_FOLDER $OUTPUT_MIN_PATH $OUTPUT_MAX_PATH $OUTPUT_FOLDER --check