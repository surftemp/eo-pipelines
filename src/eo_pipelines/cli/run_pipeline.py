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

import argparse
import logging
import sys

import eo_pipelines
from eo_pipelines.api.eo_pipeline_runner import EOPipelineRunner


def main():
    logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    parser = argparse.ArgumentParser(prog='run_pipeline', usage='%(prog)s [options]')
    parser.add_argument('-V', '--version', action='version', version="%(prog)s " + eo_pipelines.VERSION)
    parser.add_argument("yaml_path")
    parser.add_argument("--only-stages", nargs="+", metavar=["STAGE_ID"])
    parser.add_argument("--ignore-errors", action="store_true")
    parser.add_argument("--start-date", help="Override the start date, format YYYY-MM-DD", default="")
    parser.add_argument("--end-date", help="Override the end date, format YYYY-MM-DD", default="")
    args = parser.parse_args()
    runner = EOPipelineRunner()
    if not runner.run(args.yaml_path, only_stages=args.only_stages, start_date=args.start_date, end_date=args.end_date,
                      ignore_errors=args.ignore_errors):
        sys.exit(-1)


if __name__ == '__main__':
    main()
