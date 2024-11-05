#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate eo_pipelines_env

python -m eo_pipelines.stages.tools.group $GROUPING_SPEC_PATH $OUTPUT_PATH
