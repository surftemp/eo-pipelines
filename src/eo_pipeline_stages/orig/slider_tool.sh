#!/bin/bash

eval "$($CONDA_PATH 'shell.bash' 'hook' 2> /dev/null)"

conda activate slider_tool_env

python -c "import slider_tool;print('slider_tool version: ' + slider_tool.VERSION)"

slider_tool $YAML_PATH --default_input_path $INPUT_PATH --output_path $OUTPUT_PATH
