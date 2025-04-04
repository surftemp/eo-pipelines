# MIT License
#
# Copyright (c) 2022-2025 National Center for Earth Observation (NCEO)
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

import os
import datetime
import glob
import json

from .run_pipeline import EOPipelineRunner


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("input_paths", nargs="+", help="paths to pipeline files, may contain wildcards")
    parser.add_argument("--launch-command", type=str, help="command to launch a pipeline", default="sbatch run.sh")
    parser.add_argument("--launch-count", type=int, help="number of jobs to launch", default=0)
    parser.add_argument("--reset-queued", action="store_true", help="reset jobs that are marked as queued")
    parser.add_argument("--reset-running", action="store_true", help="reset jobs that are marked as running")
    parser.add_argument("--reset-succeeded", action="store_true", help="reset jobs that are marked as succeeded")
    parser.add_argument("--reset-failed", action="store_true", help="reset jobs that are marked as failed")

    args = parser.parse_args()
    to_launch = args.launch_count

    not_launched = launched = launch_failed = queued = running = succeeded = failed = 0

    # collect the input paths
    paths = []
    for input_path in args.input_paths:
        paths += glob.glob(input_path, recursive=True)

    # convert to absolute paths
    abs_paths = list(map(lambda path: os.path.abspath(path), paths))

    # for files matched, get the folder
    folders = list(map(lambda path: path if os.path.isdir(path) else os.path.split(path)[0], abs_paths))

    print(f"Found {len(folders)}, collecting status...")

    for folder_path in folders:
        folder = os.path.split(folder_path)[-1]
        status_path = os.path.join(folder_path, EOPipelineRunner.STATUS_FILENAME)

        status = {}
        state = None

        if os.path.exists(status_path):
            with open(status_path) as f:
                status = json.loads(f.read())

        completed_stages = None
        if "stage_ids" in status:
            completed_stages = []
            for stage_id in status["stage_ids"]:
                if os.path.exists(os.path.join(folder_path, stage_id, "results.json")):
                    completed_stages.append(stage_id)

        if "succeeded" in status:
            if args.reset_succeeded:
                status = {}
                os.remove(status_path)
            else:
                succeeded += 1
                state = "succeeded"
        elif "failed" in status:
            if args.reset_failed:
                status = {}
                os.remove(status_path)
            else:
                failed += 1
                state = "failed"
        elif "running" in status:
            if args.reset_running:
                status = {}
                os.remove(status_path)
            else:
                running += 1
                state = "running"
        elif "launched" in status:
            if args.reset_queued:
                status = {}
                os.remove(status_path)
            else:
                queued += 1
                state = "queued"

        if state is None:
            if to_launch > 0:
                to_launch -= 1
                os.chdir(folder_path)
                ret = os.system(args.launch_command)
                if ret == 0:
                    state = "launched"
                    status["launched"] = datetime.datetime.now().isoformat()
                    with open(status_path, "w") as f:
                        f.write(json.dumps(status))
                    print(f"Launched {folder}")
                    launched += 1
                else:
                    state = "launch failed"
                    launch_failed += 1
            else:
                state = "not launched"
                not_launched += 1

        if completed_stages is not None:
            state += " (completed: " + ",".join(completed_stages) + ")"

        print(f"\t{folder} {state}")

    print("Summary")
    print(f"Not launched:    {not_launched}")
    print(f"Launched:        {launched}")
    print(f"Launch failed:   {launch_failed}")
    print(f"Queued:          {queued}")
    print(f"Running:         {running}")
    print(f"Succeeded:       {succeeded}")
    print(f"Failed:          {failed}")


if __name__ == '__main__':
    main()
