import csv
import os
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("template_path",help="path to file containing pipeline template")
    parser.add_argument("parameters_path", help="path to csv file containing parameters")
    parser.add_argument("output_folder", help="path to folder in which output pipelines are to be constructed")

    args = parser.parse_args()

    with open(args.template_path) as f:
        template = f.read()

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
                pid += 1
                folder = os.path.join(args.output_folder,name)
                os.makedirs(folder,exist_ok=True)
                filename = os.path.join(folder,"pipeline.yaml")
                with open(filename,"w") as of:
                    s = template.replace("{working_directory}",os.path.abspath(folder))
                    for key in cols:
                        v = line[cols[key]]
                        s = s.replace("{"+key+"}",v)
                    of.write(s)
