#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate netcdfexplorer_env

generate_html --input-path $INPUT_PATH --output-folder $OUTPUT_PATH --config-path $LAYER_CONFIG_PATH --title "$TITLE" $OPTIONS