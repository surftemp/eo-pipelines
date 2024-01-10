#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate xesmf_env

regrid $INPUT_PATH $GRID_PATH $OUTPUT_FOLDER $CACHE_FOLDER --max-distance $MAX_DISTANCE