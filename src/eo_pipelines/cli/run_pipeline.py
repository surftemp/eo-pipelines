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

from eo_pipelines.api.eo_pipeline_runner import EOPipelineRunner


def main():
    logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    parser = argparse.ArgumentParser()
    parser.add_argument("yaml_path")
    parser.add_argument("--with-configuration", nargs=2, action="append",
                        metavar=["config-property-name", "config-property-value"])
    parser.add_argument("--only-stages", nargs="+", metavar=["STAGE_ID"])
    args = parser.parse_args()
    runner = EOPipelineRunner()
    if args.with_configuration:
        for override in args.with_configuration:
            config_property_name = override[0]
            config_property_value = override[1]
            runner.configure(config_property_name, config_property_value)
    if not runner.run(args.yaml_path, only_stages=args.only_stages):
        sys.exit(-1)


if __name__ == '__main__':
    main()
