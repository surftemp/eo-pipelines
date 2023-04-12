#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate usgs_env

usgs download --scene $CATALOG $DATASET $SCENE --prune-suffixes "$PRUNE_SUFFIXES"
