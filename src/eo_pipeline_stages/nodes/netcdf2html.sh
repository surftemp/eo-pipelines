#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate cartopy_env

python -m cartopy_utils.netcdf2html.cli.convert --input-path $INPUT_PATH --output-path $OUTPUT_PATH --layers $LAYERS