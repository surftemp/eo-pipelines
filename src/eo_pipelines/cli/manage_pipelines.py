import os
import datetime
import shutil


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("parent_path", help="path parent folder")
    parser.add_argument("--launch-command", type=str, help="command to launch a pipeline", default="sbatch run.sh")
    parser.add_argument("--launch-count", type=int, help="number of jobs to launch", default=0)
    parser.add_argument("--output-path", help="path to expected output files", default="")
    parser.add_argument("--harvest-to-folder", help="harvest output to this folder", default="")
    parser.add_argument("--remove-completed", nargs="+", help="remove the following files/folders from completed jobs")
    parser.add_argument("--remove-incomplete", nargs="+", help="remove the following files/folders from incomplete jobs")

    args = parser.parse_args()
    to_launch = args.launch_count

    parent_path = os.path.abspath(args.parent_path)
    launched = running = completed = not_launched = failed = 0
    folders = os.listdir(parent_path)

    def removefn(path):
        if os.path.isfile(path):
            os.remove(path)
            print("\tremoved file: "+path)
        elif os.path.isdir(path):
            shutil.rmtree(path)
            print("\tremoved folder: " + path)

    for folder in folders:
        folder_path = os.path.join(parent_path, folder)
        running_path = os.path.join(folder_path,"running.txt")
        if not os.path.exists(running_path):
            if to_launch > 0:
                to_launch -= 1
                os.chdir(folder_path)
                os.system(args.launch_command)
                with open("running.txt","w") as f:
                    f.write(datetime.datetime.now().strftime("%Y/%M/%D %H:%m:%s"))
                print(f"Launched {folder}")
                launched += 1
            else:
                not_launched += 1
        else:
            expected_output_folder = os.path.join(folder_path,"n5","stacked_output","LANDSAT_OT_C2_L2")
            expected_results_file = os.path.join(folder_path, "n5", "results.json")
            if os.path.exists(expected_results_file):
                if os.path.isdir(expected_output_folder) and len(os.listdir(expected_output_folder)) == 1:
                    print(f"Completed {folder}")
                    if args.harvest_to_folder:
                        filename = os.listdir(expected_output_folder)[0]
                        src_path = os.path.join(expected_output_folder,filename)
                        dst_path = os.path.join(args.harvest_to_folder,filename)
                        if not os.path.exists(dst_path):
                            print(f"Harvesting {filename}")
                            shutil.copy(src_path,dst_path)
                    if args.remove_completed:
                        for name in args.remove_completed:
                            path = os.path.join(folder_path, name)
                            removefn(path)

                    completed += 1
                else:
                    print(f"Failed {folder}")
                    failed += 1
            else:
                processed = []
                for subfolder in os.listdir(folder_path):
                    if os.path.exists(os.path.join(folder_path,subfolder,"results.json")):
                        processed.append(subfolder)
                processed = ",".join(processed)
                print(f"Running {folder} (completed {processed})")
                running += 1
                if args.remove_incomplete:
                    for name in args.remove_incomplete:
                        path = os.path.join(folder_path, name)
                        removefn(path)

    print("Summary")
    print(f"Not launched: {not_launched}")
    print(f"Launched:     {launched}")
    print(f"Running:      {running}")
    print(f"Completed:    {completed}")
    print(f"Failed:       {failed}")


if __name__ == '__main__':
    main()

