#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate eo_pipelines_env

python -m eo_pipelines.stages.tools.missing_filter --input-folder $INPUT_FOLDER --output-folder $OUTPUT_FOLDER --band $BAND --max-missing-fraction $MAX_MISSING_FRACTION