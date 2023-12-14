#!/usr/bin/env python
# -*- coding: utf-8 -*-

#     EOCIS high resolution data processing for the British Isles
#
#     Copyright (C) 2023  EOCIS and National Centre for Earth Observation (NCEO)
#
#     This program is free software: you can redistribute it and/or modify
#     it under the terms of the GNU General Public License as published by
#     the Free Software Foundation, either version 3 of the License, or
#     (at your option) any later version.
#
#     This program is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU General Public License for more details.
#
#     You should have received a copy of the GNU General Public License
#     along with this program.  If not, see <https://www.gnu.org/licenses/>.

import os
import xarray as xr
import numpy as np
import logging

import argparse

class LandsatAlign:

    def __init__(self, input_folder, output_folder, min_path=None, max_shift=200):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.min_path = min_path
        self.max_shift = max_shift
        self.logger = logging.getLogger("LandsatAlign")

    def run(self):

        min_shift = None
        max_shift = None
        max_shift_filename = None
        first_ds = None
        ref_ds = None
        ref_filename = None

        if self.output_folder is not None:
            os.makedirs(self.output_folder,exist_ok=True)

        scenes = []
        datasets = {}

        for f in os.listdir(input_folder):
            if f.endswith(".nc"):
                input_path = os.path.join(self.input_folder, f)
                try:
                    ds = xr.open_dataset(input_path)
                    # sanity check for HDF errors
                    lat0 = ds.lat[0,0]
                    lon0 = ds.lon[0,0]
                    scenes.append(f)
                    datasets[f] = ds
                except Exception as ex:
                    self.logger.warning(f"Error {str(ex)} reading input file {input_path}, ignoring")

        if len(scenes) == 0:
            raise ValueError(f"No input files found to process in {input_folder}")

        self.logger.info(f"Found {len(scenes)} input files")
        # find the "most westerly shifted" file
        for f in scenes:
            ds = datasets[f]
            if first_ds is None:
                first_ds = ds
            else:
                shift = self.find_shift(first_ds, ds)
                if shift is None:
                    raise Exception(f"Unable to calculate shift for file {f}")
                if min_shift == None or shift < min_shift:
                    min_shift = shift
                    ref_ds = ds
                    ref_filename = f
                if max_shift is None or shift > max_shift:
                    max_shift = shift
                    max_shift_filename = f

            datasets[f] = ds

        self.logger.info(f"Western most file is {ref_filename}, Eastern most file is {max_shift_filename}, pixel shift is {max_shift-min_shift}")

        # now work out the output width and final shifts
        shifts = {}
        output_width = None
        output_height = None
        for f in scenes:
            ds = datasets[f]
            (height,width) = ds.lon.shape
            shift = self.find_shift(ref_ds,ds) if f != ref_filename else 0
            shifts[f] = shift
            assert(shift >= 0) # just checking
            if output_width is None or shift+width > output_width:
                output_width = shift+width
            if output_height is None or height < output_height:
                output_height = height


        self.logger.info(f"Output dimensions height: {output_height}, width: {output_width}")

        # get the output lon/lat arrays
        output_lons = np.zeros(shape=(output_height,output_width),dtype=ref_ds.lon.dtype)
        output_lats = np.zeros(shape=(output_height,output_width),dtype=ref_ds.lat.dtype)
        output_lats[:,:] = np.nan
        output_lons[:,:] = np.nan

        for f in [ref_filename,max_shift_filename]:
            ds = datasets[f]
            width = ds.lon.shape[1]
            shift = shifts[f]
            output_lons[0:output_height,shift:shift+width] = ds.lon.data[0:output_height,:]
            output_lats[0:output_height,shift:shift+width] = ds.lat.data[0:output_height,:]

        # create the output datasets and aggregate
        if self.min_path:
            min_arrays = { "lat": output_lats, "lon": output_lons }
        else:
            min_arrays = None

        for f in scenes:
            self.logger.info(f"Processing input file: {f}")
            ds = datasets[f]
            (height,width) = ds.lon.shape
            shift = shifts[f]
            min_height = min(height,output_height)

            output_arrays = { "lat": output_lats, "lon": output_lons}

            for v in ds.variables:
                if v != "time" and v != "lat" and v != "lon":

                    arr = np.zeros(shape=(1,output_height,output_width),dtype=ds[v].dtype)
                    arr[0,:,:] = np.nan
                    arr[0,0:min_height,shift:shift+width] = ds[v].data[0,0:min_height,:]
                    output_arrays[v] = arr

                    if min_arrays is not None:
                        if v not in min_arrays:
                            min_arrays[v] = arr
                        else:
                            min_arrays[v] = np.nanmin(np.stack([arr,min_arrays[v]]),axis=0)

            if self.output_folder:
                output_path = os.path.join(self.output_folder, f)
                self.logger.info(f"Creating output file: {output_path}")
                self.create_output_dataset(output_path,ds,output_arrays)

            ds.close()

        if self.min_path:
            self.logger.info(f"Creating output file: {self.min_path}")
            self.create_output_dataset(self.min_path, ds, min_arrays)

    def create_output_dataset(self,to_path,from_ds,variable_arrays):
        encodings = {}
        output_ds = xr.Dataset(attrs=from_ds.attrs)
        for v in variable_arrays:
            output_da = xr.DataArray(data=variable_arrays[v], dims=from_ds[v].dims, attrs=from_ds[v].attrs)
            output_ds[v] = output_da
            encodings[v] = {"zlib": True, "complevel": 5}
        output_ds["time"] = from_ds["time"]
        output_ds.to_netcdf(to_path,encoding=encodings)

    def find_shift(self,ref_ds,other_ds):
        # return many pixels other_ds should be shifted "to the east" to align with ref_ds
        # or None if no shift was found within the max_shift
        for shift in range(-self.max_shift,self.max_shift+1,1):
            ref_idx_x = 0
            other_idx_x = 0
            if shift < 0:
                ref_idx_x = -shift
            else:
                other_idx_x = shift
            if float(ref_ds.lat[0,ref_idx_x]) == float(other_ds.lat[0,other_idx_x]) \
               and float(ref_ds.lon[0,ref_idx_x]) == float(other_ds.lon[0,other_idx_x]):
               return shift

        # no shift found...
        return None


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("input_folder")
    parser.add_argument("--output-folder",help="Output aligned versions of input files to this folder", default=None)
    parser.add_argument("--output-min-path",help="Aggregate minimum values and output to this path",default=None)

    logging.basicConfig(level=logging.INFO)

    args = parser.parse_args()

    input_folder = args.input_folder
    output_folder = args.output_folder

    aligner = LandsatAlign(input_folder,output_folder,min_path=args.output_min_path)
    aligner.run()

