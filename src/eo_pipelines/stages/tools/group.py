# -*- coding: utf-8 -*-

#    landsat2nc
#    Copyright (C) 2020  National Centre for Earth Observation (NCEO)
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Utility programming for grouping together regridded scenes that are acquired at similar times and cover similar areas"""

import argparse
import logging
import os
import json
import xarray as xr
import numpy as np

from .datetime_utils import date_parse, date_format

class Group:

    def __init__(self, start_dt, end_dt, dataset, scene_path, group_attrs):
        self.start_dt = start_dt
        self.end_dt = end_dt
        self.scenes_by_dataset = {}
        self.scenes_by_dataset[dataset] = [scene_path]
        self.group_attrs = group_attrs

    def get_scenes_for_dataset(self, dataset):
        return sorted(self.scenes_by_dataset.get(dataset, []))

    def extend(self, dt, dataset, scene_path, group_attrs, time_window_seconds):
        if group_attrs != self.group_attrs:
            return False
        include = False
        if dt >= self.start_dt and dt <= self.end_dt:
            include = True
        elif dt < self.start_dt and (self.end_dt - dt).total_seconds() <= time_window_seconds:
            include = True
            self.start_dt = dt
        elif dt > self.end_dt and (dt - self.start_dt).total_seconds() <= time_window_seconds:
            include = True
            self.end_dt = dt
        if include:
            if dataset not in self.scenes_by_dataset:
                self.scenes_by_dataset[dataset] = []
            self.scenes_by_dataset[dataset].append(scene_path)
        return include

    def __repr__(self):
        s = "Group(%s, %s" % (str(self.start_dt), str(self.end_dt))
        for (dataset, paths) in self.scenes_by_dataset.items():
            for path in paths:
                s += ", "
                s += path
        s += ")"
        return s

class GroupProcessor:

    DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S%z"

    def __init__(self, input_spec, output_folder):
        self.input_spec = input_spec
        self.output_folder = output_folder
        os.makedirs(self.output_folder, exist_ok=True)

        # list of Group objects populated by find_groups
        self.groups = []

        self.logger = logging.getLogger("GroupProcessor")
        self.time_window_seconds = self.input_spec["time_window_seconds"]

    def find_groups(self):
        datasets = self.input_spec["datasets"]
        group_by_attributes = self.input_spec["group_by_attributes"]
        for (dataset, folder) in datasets.items():
            for filename in os.listdir(folder):
                path = os.path.join(folder, filename)
                self.logger.info(f"find_groups opening: {path}")
                if not path.endswith(".nc"):
                    continue
                with xr.open_dataset(path) as ds:
                    dt = date_parse(ds.attrs["acquisition_time"])
                    group_attrs = [ds.attrs[attr] for attr in group_by_attributes]

                added = False
                for group in self.groups:
                    if group.extend(dt, dataset, path, group_attrs, self.time_window_seconds):
                        added = True
                        break
                if not added:
                    self.groups.append(Group(dt, dt, dataset, path, group_attrs))
                    self.logger.info(f"Creating group: {len(self.groups)}")

        self.logger.info("Found %d groups:" % len(self.groups))

    def process_groups(self):
        group_index = 0

        rename = self.input_spec.get("rename", {})
        overlap_using = self.input_spec.get("overlap_using", {})

        for group in self.groups:

            group_index += 1
            self.logger.info("Processing group (%d/%d): %s" % (group_index, len(self.groups), str(group)))
            combined_ds = None
            combined_filename = ""
            datasets = self.input_spec["datasets"].keys()

            valid_group = True

            # make sure each dataset is represented in this group
            for dataset in datasets:
                if len(group.get_scenes_for_dataset(dataset)) == 0:
                    self.logger.warning("Skipping group containing no data for dataset %s" % dataset)
                    valid_group = False

            # if not, skip it
            if not valid_group:
                continue

            # track the output variables for each dataset
            output_variables_by_dataset = {}
            for dataset in datasets:
                output_variables_by_dataset[dataset] = set()

            try:
                processing_levels = set()
                for dataset in datasets:
                    # merge in all the scenes for this particular dataset
                    # if multiple scenes exist this involves "stitching" scenes together "along" the orbit

                    paths = group.get_scenes_for_dataset(dataset)

                    # paths will be sorted by filename, earlier paths take priority during stitching
                    if len(paths) > 1:
                        if dataset not in overlap_using:
                            self.logger.warning(
                                "Stitching multiple scenes for dataset %s in  group (%d/%d) without overlap_using, each band will be stitched independently" % (
                                    dataset, group_index, len(self.groups)))
                        else:
                            self.logger.info(
                                "Stitching multiple scenes for dataset %s in group (%d/%d) with overlap_using band %s" % (
                                    dataset, group_index, len(self.groups), overlap_using[dataset]))

                    for path in paths:
                        ds = xr.open_dataset(path)

                        if dataset in rename:
                            rename_vars = {k: v for (k, v) in rename[dataset].items() if k in ds.variables}
                            if rename_vars:
                                ds = ds.rename_vars(rename_vars)

                        # track the output variables
                        for v in ds.data_vars:
                            output_variables_by_dataset[dataset].add(v)

                        # extend the first file we come across to create the output
                        if combined_ds is None:
                            combined_ds = ds
                            combined_filename = os.path.split(path)[1]
                            combined_ds.attrs["time_coverage_start"] = date_format(group.start_dt)
                            combined_ds.attrs["time_coverage_end"] = date_format(group.end_dt)
                        else:
                            processing_levels.add(ds.attrs["processing_level"])

                            # merge in global attributes
                            for attr_name in ds.attrs:
                                if attr_name not in combined_ds.attrs:
                                    combined_ds.attrs[attr_name] = ds.attrs[attr_name]
                                else:
                                    if ds.attrs[attr_name] != combined_ds.attrs[attr_name]:
                                        combined_ds.attrs[attr_name] = str(combined_ds.attrs[attr_name]) + ", " + str(
                                            ds.attrs[attr_name])

                            # merge in variables

                            overlap_mask = None
                            if len(paths) > 1:
                                # when stitching multiple scenes...
                                # if an overlap band has been specified, create a mask based on this
                                # the mask will be true for each pixel where the overlap band currently has a NaN
                                # value and the new dataset has a non-NaN values for the overlap band
                                if dataset in overlap_using:
                                    overlap_band = overlap_using[dataset]
                                    if overlap_band in combined_ds:
                                        overlap_mask = np.logical_and(np.isnan(combined_ds[overlap_band].values),
                                                                      ~np.isnan(ds[overlap_band].values))

                            for v in ds.data_vars:
                                if v not in combined_ds:
                                    combined_ds[v] = ds[v]
                                else:
                                    a = combined_ds[v].values
                                    if overlap_mask is None or overlap_mask.shape != a.shape:
                                        # no valid overlap mask, simply overwrite any NaN valued pixels in this band
                                        a = np.where(np.isnan(a), ds[v].values, a)
                                    else:
                                        # using the overlap mask, only overwrite pixels where the overlap band is NaN
                                        a = np.where(overlap_mask, ds[v].values, a)
                                    combined_ds[v].data = a
                            ds.close()

                combined_output_path = os.path.join(self.output_folder, combined_filename)
                combined_ds.attrs["processing_level"] = ",".join(processing_levels)
                combined_ds.attrs["acquisition_time"] = date_format(
                    group.start_dt + (group.end_dt - group.start_dt) / 2)

                combined_ds.to_netcdf(combined_output_path)
                combined_ds.close()

                self.logger.info("Processed group (%d/%d): Written combined data to %s" % (
                    group_index, len(self.groups), combined_output_path))
            except Exception:
                self.logger.exception("failed to process group")


# this program performs actions controlled by a grouping spec
#
# the grouping spec is a JSON formatted file which specifies the following top level keys
#
# datasets:
#   a mapping from dataset name to the path to the folder containing regridded files obtained from the dataset
# bands:
#   a mapping from dataset name to the list of bands to copy from this dataset to the output datasets
#
# {"datasets": {"LANDSAT_8_C1": "/home/dev/github/eo-pipelines/test/outputs/LANDSAT_8_C1", "LANDSAT_OT_C2_L2": "/home/dev/github/eo-pipelines/test/outputs/LANDSAT_OT_C2_L2"}, "bands": {"LANDSAT_8_C1": ["3", "4", "5", "6", "10", "11", "QA"], "LANDSAT_OT_C2_L2": ["ST", "QA_PIXEL"]}}

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "grouping_spec", help="path to a JSON file defining the dataset locations and bands to include")
    parser.add_argument(
        "output_folder", help="output folder to store the grouped files")

    args = parser.parse_args()

    with open(args.grouping_spec) as f:
        grouping_spec = json.loads(f.read())

    processor = GroupProcessor(grouping_spec, args.output_folder)
    processor.find_groups()
    processor.process_groups()
