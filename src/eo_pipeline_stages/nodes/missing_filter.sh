#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate eo_pipelines_env

python -m pyproj_utils.missing_filter --input-folder $INPUT_FOLDER --output-folder $OUTPUT_FOLDER --band $BAND --max-missing-fraction $MAX_MISSING_FRACTION