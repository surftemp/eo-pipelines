import csv
import os
import argparse

if __name__ == '__main__':
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
        template = f.read()

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
                pid += 1

                if job_script_template:
                    job_script_path = os.path.join(parent_folder,job_script_filename)
                    if not os.path.exists(job_script_path):
                        s = job_script_template.replace("{working_directory}",parent_folder)
                        if group is not None:
                            s = s.replace("{group}",str(group))
                        with open(job_script_path,"w") as f:
                            f.write(s)

                filename = os.path.join(folder,"pipeline.yaml")
                with open(filename,"w") as of:
                    s = template.replace("{working_directory}",os.path.abspath(folder))
                    for key in cols:
                        v = line[cols[key]]
                        s = s.replace("{"+key+"}",v)
                    of.write(s)
