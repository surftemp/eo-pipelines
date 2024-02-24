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
import gc

import argparse

class LandsatAlign:

    def __init__(self, input_folder, output_folder, min_path=None):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.min_path = min_path
        self.logger = logging.getLogger("LandsatAlign")

    def run(self, slice_size=None, check_alignment=False):


        if self.output_folder is not None:
            os.makedirs(self.output_folder,exist_ok=True)

        scenes = []
        datasets = {}

        for f in os.listdir(input_folder):
            if f.endswith(".nc"):
                input_path = os.path.join(self.input_folder, f)
                try:
                    ds = xr.open_dataset(input_path)
                    if slice_size != None:
                        ds = ds.isel(ni=range(0,slice_size),nj=range(0,slice_size))
                    scenes.append(f)
                    datasets[f] = ds
                except Exception as ex:
                    self.logger.warning(f"Error {str(ex)} reading input file {input_path}, ignoring")

        if len(scenes) == 0:
            raise ValueError(f"No input files found to process in {input_folder}")

        self.logger.info(f"Found {len(scenes)} input files")

        ref_ds = datasets[scenes[1]]
        ref_filename = scenes[1]

        self.logger.info(f"Selecting {ref_filename} as the reference scene")
        self.logger.info(f"Aligning scenes...")

        wshifts = {}
        nshifts = {}

        heights = {}
        widths = {}

        for f in scenes:
            if f in datasets:
                try:
                    ds = datasets[f]
                    (height,width) = ds.lon.shape
                    heights[f] = height
                    widths[f] = width
                    self.logger.info(f"Calculating shift for {f}...")
                    # work out how many pixels to shift this dataset to the "west"
                    (nshift,wshift) = self.find_shift(ref_ds,ds) if f != ref_filename else (0,0)

                    if wshift is None:
                        self.logger.warning(f"Unable to align {f}, skipping")
                        continue
                    else:
                        self.logger.info(f"Shift for {f} is ({nshift},{wshift})")

                    wshifts[f] = wshift
                    nshifts[f] = nshift
                except Exception as ex:
                    print(ex)

        min_wshift = min(wshifts.values())
        min_nshift = min(nshifts.values())

        # make shifts relative to a SE "origin"
        for f in wshifts:
            wshifts[f] = wshifts[f] - min_wshift
            nshifts[f] = nshifts[f] - min_nshift

        se_intersection_wshift = max(wshifts.values())
        se_intersection_nshift = max(nshifts.values())

        # calculate the shifts to the nw corners
        nw_wshifts = {}
        nw_nshifts = {}
        for f in wshifts:
            nw_wshifts[f] = wshifts[f] + widths[f]
            nw_nshifts[f] = nshifts[f] + heights[f]

        nw_intersection_wshift = min(nw_wshifts.values())
        nw_intersection_nshift = min(nw_nshifts.values())

        output_width = nw_intersection_wshift - se_intersection_wshift
        output_height = nw_intersection_nshift - se_intersection_nshift

        self.logger.info(f"Output dimensions height: {output_height}, width: {output_width}")

        # get the output lon/lat arrays
        output_lons = np.zeros(shape=(output_height,output_width),dtype=ref_ds.lon.dtype)
        output_lats = np.zeros(shape=(output_height,output_width),dtype=ref_ds.lat.dtype)
        output_lats[:,:] = np.nan
        output_lons[:,:] = np.nan

        ds = datasets[ref_filename]

        start_n = se_intersection_nshift - nshifts[ref_filename]
        start_w = se_intersection_wshift - wshifts[ref_filename]

        output_lons[:,:] = ds.lon.data[start_n:start_n+output_height,start_w:start_w+output_width]
        output_lats[:,:] = ds.lat.data[start_n:start_n+output_height,start_w:start_w+output_width]

        # create the output datasets and aggregate
        if self.min_path:
            min_arrays = { "lat": output_lats, "lon": output_lons }
        else:
            min_arrays = None

        processed_count = 0
        for f in scenes:
            if f in datasets:
                try:
                    self.logger.info(f"Processing input file: {f}")
                    ds = datasets[f]

                    start_n = se_intersection_nshift - nshifts[f]
                    start_w = se_intersection_wshift - wshifts[f]

                    # sanity check lat/lon alignment
                    if check_alignment:
                        maxlondiff = np.max(np.abs(
                            output_lons -
                            ds.lon.data[start_n:start_n+output_height,start_w:start_w+output_width]))
                        maxlatdiff = np.max(np.abs(
                            output_lats -
                            ds.lat.data[start_n:start_n+output_height,start_w:start_w+output_width]))
                        if maxlondiff > 1e-7 or maxlatdiff > 1e-7:
                            raise Exception(f"Internal error: maxlondiff={maxlondiff},maxlatdiff={maxlatdiff}")

                    output_arrays = { "lat": output_lats, "lon": output_lons }

                    for v in ds.variables:
                        if v != "time" and v != "lat" and v != "lon":
                            arr = np.zeros(shape=(1,output_height,output_width),dtype=ds[v].dtype)
                            arr[0,:,:] = np.nan
                            arr[0,:,:] = ds[v].data[0,start_n:start_n+output_height,start_w:start_w+output_width]
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

                    processed_count += 1

                except:
                    self.logger.exception(f"Error processing input file: {f}")
                datasets[f].close()
                del datasets[f]
                gc.collect()


        if self.min_path:
            self.logger.info(f"Creating output file: {self.min_path}")
            self.create_output_dataset(self.min_path, ref_ds, min_arrays)

        processed_pct = int(100*processed_count/len(scenes))

        self.logger.info(f"Processed {processed_count} ({processed_pct} %) input files")

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
        # return how many pixels other_ds should be shifted "to the west" and "to the north" to align with ref_ds
        # returning (north_shift,west_shift)
        # or None if no shift was found within the max_shift
        for (max_shift, shift_step) in [(50,10),(600,5),(200,1)]:
            for xshift in range(-max_shift,max_shift+1,shift_step):
                # x-axis runs approx from west to east
                ref_idx_x = 0
                other_idx_x = 0
                if xshift < 0:
                    other_idx_x = -xshift # shift other "west"
                else:
                    ref_idx_x = xshift # shift other "east"
                for yshift in range(-max_shift, max_shift + 1, shift_step):
                    # y-axis runs approx from north to south
                    ref_idx_y = 0
                    other_idx_y = 0
                    if yshift < 0:
                        other_idx_y = -yshift  # shift other "north"
                    else:
                        ref_idx_y = yshift  # shift other "south"
                    if float(ref_ds.lat[ref_idx_y,ref_idx_x]) == float(other_ds.lat[other_idx_y,other_idx_x]) \
                       and float(ref_ds.lon[ref_idx_y,ref_idx_x]) == float(other_ds.lon[other_idx_y,other_idx_x]):
                       return (yshift,xshift)

        # no shift found...
        return (None,None)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("input_folder")
    parser.add_argument("--output-folder",help="Output aligned versions of input files to this folder", default=None)
    parser.add_argument("--output-min-path",help="Aggregate minimum values and output to this path",default=None)
    parser.add_argument("--slice", type=int, help="Work with a NE corner slice of the data of this size, useful for testing", default=None)
    parser.add_argument("--check", action="store_true", help="Double check alignment")

    logging.basicConfig(level=logging.INFO)

    args = parser.parse_args()

    input_folder = args.input_folder
    output_folder = args.output_folder

    aligner = LandsatAlign(input_folder,output_folder,min_path=args.output_min_path)
    aligner.run(slice_size=args.slice,check_alignment=args.check)

