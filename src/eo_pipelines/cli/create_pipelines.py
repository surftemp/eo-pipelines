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

import csv
import os
import argparse
from mako.template import Template


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("template_path",help="path to file containing pipeline template")
    parser.add_argument("parameters_path", help="path to csv file containing parameters")
    parser.add_argument("output_folder", help="path to folder in which output pipelines are to be constructed")
    parser.add_argument("--groups", type=int,
                        help="split generated pipelines into this many groups, stored in subfolders", default=None)
    parser.add_argument("--job-script-template",
                        help="specify the path to a template script for running the generated pipelines", default=None)

    args = parser.parse_args()

    with open(args.template_path) as f:
        template = Template(f.read())

    job_script_template = None
    job_script_filename = None
    if args.job_script_template:
        with open(args.job_script_template) as f:
            job_script_template = f.read()
        job_script_filename = os.path.split(args.job_script_template)[-1]

    os.makedirs(args.output_folder, exist_ok=True)

    with open(args.parameters_path) as f:
        rdr = csv.reader(f)
        cols = {}
        pid = 0

        for line in rdr:
            if len(cols) == 0:
                for idx in range(len(line)):
                    cols[line[idx]] = idx
            else:
                name = "pipeline"+str(pid)
                group = None
                if args.groups is not None:
                    group = pid % args.groups
                    parent_folder = os.path.join(args.output_folder,f"group{group}")
                else:
                    parent_folder = args.output_folder
                folder = os.path.join(parent_folder, name)
                os.makedirs(folder, exist_ok=True)

                if job_script_template:
                    job_script_path = os.path.join(parent_folder,job_script_filename)
                    if not os.path.exists(job_script_path):
                        s = job_script_template.replace("{working_directory}",parent_folder)
                        s = s.replace("{pid}", str(pid))
                        if group is not None:
                            s = s.replace("{group}",str(group))
                        with open(job_script_path,"w") as f:
                            f.write(s)

                filename = os.path.join(folder,"pipeline.yaml")
                with open(filename,"w") as of:
                    d = {"pid":str(pid), "working_directory":os.path.abspath(folder)}
                    for key in cols:
                        v = line[cols[key]]
                        d[key] = v
                    s = template.render(**d)
                    of.write(s)

                pid += 1

        print(f"Created {pid} pipelines under {args.output_folder}")

if __name__ == '__main__':
    main()