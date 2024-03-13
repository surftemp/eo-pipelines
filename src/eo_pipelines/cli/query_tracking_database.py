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

from eo_pipelines.executors.tracking_database import TrackingDatabase

def dump(rowlist):
    for row in rowlist:
        print(row)

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("database_path",help="Path to the tracking database")
    parser.add_argument("--list-runs",action="store_true",help="List the runs in the database")
    parser.add_argument("--list-tasks", action="store_true", help="List the tasks in the database")
    parser.add_argument("--run-id", help="Filter tasks/stages by run", default=None)
    parser.add_argument("--stage-id", help="Filter tasks/stages by stage", default=None)

    args = parser.parse_args()
    db = TrackingDatabase(args.database_path)
    if args.list_runs:
        dump(db.list_runs())
    if args.list_tasks:
        dump(db.list_tasks(args.run_id, args.stage_id))

if __name__ == '__main__':
    main()