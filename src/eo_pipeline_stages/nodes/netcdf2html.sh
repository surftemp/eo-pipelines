#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate cartopy_env

python -m cartopy_utils.netcdf2html.cli.convert --input-path $INPUT_PATH --output-path $OUTPUT_PATH --layer-config-path $LAYER_CONFIG_PATH --title "$TITLE"