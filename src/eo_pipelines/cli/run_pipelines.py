# MIT License
#
# Copyright (c) 2022 National Center for Earth Observation (NCEO)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import argparse
import logging
import os

from .run_pipeline import EOPipelineRunner

import glob

def read_progress(path):
    completed_pipelines = []
    if os.path.exists(path):
        with open(path) as f:
            lines = f.readlines()
            for line in lines:
                line = line.strip()
                if line:
                    completed_pipelines.append(line)
    return completed_pipelines

def write_progress(path, completed_pipelines):
    with open(path,"w") as f:
        for path in completed_pipelines:
            f.write(path+"\n")

def main():
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("yaml_pattern")
    parser.add_argument("--progress-csv-path",default="pipelines_progress.csv")
    parser.add_argument("--limit", type=int, default=None)

    args = parser.parse_args()
    pipelines_progress = read_progress(args.progress_csv_path)

    yaml_paths = glob.glob(args.yaml_pattern,recursive=True)

    main_folder = os.getcwd()

    ran = 0
    for yaml_path in yaml_paths:
        if yaml_path not in pipelines_progress:
            runner = EOPipelineRunner()
            folder,yaml_filename = os.path.split(yaml_path)
            os.chdir(folder)
            print(f"running pipeline: {yaml_path} in folder {folder}")
            runner.run(yaml_filename) # fixme should check status somehow
            ran += 1
            print(f"completed pipeline: {yaml_path} in folder {folder}")
            os.chdir(main_folder)
            pipelines_progress.append(yaml_path)
            write_progress(args.progress_csv_path,pipelines_progress)
            if args.limit and ran >= args.limit:
                break

if __name__ == '__main__':
    main()
