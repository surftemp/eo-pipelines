#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate eo_pipelines_env

python -m  eo_pipelines.stages.tools.add_masks --input-path $INPUT_PATH --output-path $OUTPUT_PATH --add-layers $LAYER_SPECIFICATIONS