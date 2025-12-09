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

import os.path
import tempfile
import shutil

from eo_pipelines.pipeline_stage import PipelineStage

class ApplyFMask(PipelineStage):

    VERSION = "0.0.1"

    def __init__(self, node_services):
        super().__init__(node_services, "apply_fmask")
        self.node_services = node_services

    async def load(self):
        await super().load()
        self.output_path = self.get_configuration().get("output_path", None)
        self.dataset = self.get_configuration().get("dataset", "")
        self.model = self.get_configuration().get("model", "UPL")
        self.fmask_home = self.get_configuration().get("fmask_home", "")

        if self.dataset == "" or self.fmask_home == "":
            raise ValueError("configuration keys dataset and fmask_home are required")

        if self.output_path is None:
            self.output_path = self.get_working_directory()
        else:
            if not os.path.isabs(self.output_path):
                self.output_path = os.path.join(self.get_working_directory(), self.output_path)

        self.get_logger().info("eo_pipeline_stages.LandsatImport %s" % ApplyFMask.VERSION)

    def get_parameters(self):
        return {}

    def execute_stage(self, inputs):

        executor = self.create_executor()

        output_scenes = {}

        total_succeeded = 0
        total_failed = 0
        input = inputs.get("input", {})

        for dataset in input:

            dataset_folder = input[dataset]

            if dataset != self.dataset:
                output_scenes[dataset] = dataset_folder
                continue

            dataset_output_folder = os.path.join(self.output_path, dataset)
            output_scenes[dataset] = dataset_output_folder
            os.makedirs(dataset_output_folder, exist_ok=True)

            with tempfile.TemporaryDirectory() as tmpdir:

                os.makedirs(tmpdir, exist_ok=True)
                folders = []

                for filename in os.listdir(dataset_folder):
                    if filename.endswith("_MTL.xml"):
                        fileroot = filename[:-len("_MTL.xml")]
                        folderpath = os.path.join(tmpdir, fileroot)
                        os.makedirs(folderpath)
                        folders.append(folderpath)
                        for src_filename in os.listdir(dataset_folder):
                            if src_filename.startswith(fileroot):
                                os.symlink(os.path.join(dataset_folder, src_filename),
                                           os.path.join(folderpath, src_filename))
                                os.symlink(os.path.join(dataset_folder, src_filename),
                                           os.path.join(dataset_output_folder, src_filename))

                succeeded = 0
                failed = 0

                task_ids = []
                task_folders = {}

                for folderpath in folders:
                    custom_env = self.get_parameters()
                    custom_env["MODEL"] = self.model
                    custom_env["SCENEPATH"] = folderpath
                    custom_env["FMASK_HOME"] = self.fmask_home
                    scene_id = os.path.split(folderpath)[1]

                    script = os.path.join(os.path.split(__file__)[0], "..", "scripts", "fmask.sh")
                    task_id = executor.queue_task(self.get_stage_id(), script, custom_env,
                                                  self.get_working_directory(),
                                                  description=scene_id)
                    task_ids.append(task_id)
                    task_folders[task_id] = folderpath

                executor.wait_for_tasks()
                for task_id in task_ids:
                    if executor.get_task_result(task_id):
                        folderpath = task_folders[task_id]
                        scene_id = os.path.split(folderpath)[1]
                        output_filename = scene_id + "_" + self.model + ".tif"
                        result_filename = scene_id + "_" + self.model + ".TIF"
                        expected_output_path = os.path.join(folderpath, output_filename)
                        if os.path.exists(expected_output_path):
                            succeeded += 1
                            shutil.copyfile(expected_output_path, os.path.join(dataset_output_folder, result_filename))
                        else:
                            failed += 1
                    else:
                        failed += 1

                total_succeeded += succeeded
                total_failed += failed

        self.get_logger().info(f"fmask scenes: succeeded:{total_succeeded}, failed:{total_failed}")

        return {"output": output_scenes}
