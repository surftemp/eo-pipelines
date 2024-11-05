#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate eo_pipelines_env

python -m eo_pipelines.stages.tools.time_stacker --input-folders $INPUT_FOLDER --output-path=$OUTPUT_PATH $STACK_ATTRIBUTES $KEEP_ATTRIBUTES