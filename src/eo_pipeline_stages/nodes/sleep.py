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

import os

from eo_pipelines.pipeline_stage import PipelineStage
from eo_pipelines.pipeline_stage_utils import format_int


class Sleep(PipelineStage):

    VERSION = "0.0.1"
    fetch_stage_count = 0


    def __init__(self, node_services):
        super().__init__(node_services,"sleep")
        self.output_path = self.get_configuration().get("output_path", None)
        if self.output_path is None:
            self.output_path = self.get_working_directory()
        else:
            if not os.path.isabs(self.output_path):
                self.output_path = os.path.join(self.get_working_directory(),self.output_path)

        self.download_path = self.get_configuration().get("download_path", None)
        if self.download_path is None:
            self.download_path = self.output_path
        else:
            if not os.path.isabs(self.download_path):
                self.download_path = os.path.join(self.get_working_directory(), self.download_path)

        self.get_logger().info("eo_pipeline_stages.Sleep %s" % Sleep.VERSION)

    def execute_stage(self, inputs):

        executor = self.create_executor()

        task_count = self.get_configuration().get("task_count", 1)
        sleep_seconds = self.get_configuration().get("sleep_seconds", 60)

        sleep_script = os.path.join(os.path.split(__file__)[0], "sleep.sh")

        sleep_task_ids = []
        for idx in range(task_count):
            task_id = executor.queue_task(self.get_stage_id(),sleep_script,
                                          { "SLEEP_SECONDS": format_int(sleep_seconds) },
                                          self.get_working_directory())
            sleep_task_ids.append(task_id)

        executor.wait_for_tasks()

        for task_id in sleep_task_ids:
            if not executor.get_task_result(task_id):
                self.get_logger().error("Sleep task failed: %s" % task_id)

        return {}

