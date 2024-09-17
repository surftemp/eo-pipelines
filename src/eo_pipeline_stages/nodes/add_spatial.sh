#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate eo_pipelines_env

python -m cartopy_utils.add_spatial --input-path $INPUT_PATH --output-path $OUTPUT_PATH --dataset-path $DATASET_PATH --dataset-variables $DATASET_VARIABLES