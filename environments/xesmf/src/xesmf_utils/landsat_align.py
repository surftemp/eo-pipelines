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

    def __init__(self, input_folder, output_folder, min_path=None, max_shift=200):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.min_path = min_path
        self.max_shift = max_shift
        self.logger = logging.getLogger("LandsatAlign")

    def run(self, slice_size=None, check_alignment=False):

        min_eshift = None
        max_eshift = None
        max_eshift_filename = None
        first_ds = None
        ref_ds = None
        ref_filename = None
        ref_sshift = None

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
        # find the "most westerly shifted" file
        for f in scenes:
            try:
                ds = datasets[f]
                if first_ds is None:
                    first_ds = ds
                else:
                    (nshift,wshift) = self.find_shift(first_ds, ds)

                    if wshift is None:
                        raise Exception(f"Unable to calculate shift for file {f}")
                    eshift = -wshift
                    sshift = -nshift

                    if min_eshift == None or eshift < min_eshift:
                        min_eshift = eshift
                        ref_ds = ds
                        ref_filename = f
                        ref_sshift = sshift
                    if max_eshift is None or eshift > max_eshift:
                        max_eshift = eshift
                        max_eshift_filename = f
            except:
                del datasets[f]
                self.logger.exception(f"Error processing input file: {f}")

        self.logger.info(f"Western most file is {ref_filename}, Eastern most file is {max_eshift_filename}, pixel shift is {max_eshift-min_eshift}")

        # now work out the output width and final shifts
        eshifts = {}
        sshifts = {}
        output_width = None
        output_height = None
        for f in scenes:
            if f in datasets:
                try:
                    ds = datasets[f]
                    (height,width) = ds.lon.shape
                    # work out how many pixels to shift this dataset to the "east"
                    (nshift,wshift) = self.find_shift(ref_ds,ds) if f != ref_filename else (0,0)
                    eshift = -wshift
                    sshift = -nshift
                    eshifts[f] = eshift
                    sshifts[f] = sshift
                    assert(eshift >= 0) # just checking
                    # shift south wrt reference
                    rel_sshift = sshift - ref_sshift
                    # if shifted further north wrt ref, reduce usable height
                    if rel_sshift < 0:
                        height -= -rel_sshift

                    if output_width is None or -wshift+width > output_width:
                        output_width = eshift+width
                    if output_height is None or height < output_height:
                        output_height = height
                except:
                    del datasets[f]
                    self.logger.exception(f"Error processing input file: {f}")


        self.logger.info(f"Output dimensions height: {output_height}, width: {output_width}")

        # get the output lon/lat arrays
        output_lons = np.zeros(shape=(output_height,output_width),dtype=ref_ds.lon.dtype)
        output_lats = np.zeros(shape=(output_height,output_width),dtype=ref_ds.lat.dtype)
        output_lats[:,:] = np.nan
        output_lons[:,:] = np.nan

        for f in [ref_filename,max_eshift_filename]:
            if f in datasets:
                try:
                    ds = datasets[f]
                    height = ds.lon.shape[0]
                    width = ds.lon.shape[1]
                    eshift = eshifts[f]
                    sshift = sshifts[f]
                    rel_sshift = sshift - ref_sshift
                    dst_start_y = rel_sshift if rel_sshift > 0 else 0
                    src_start_y = -rel_sshift if rel_sshift < 0 else 0
                    copy_height = min(output_height-dst_start_y,height-src_start_y)

                    output_lons[dst_start_y:dst_start_y+copy_height,eshift:eshift+width] = ds.lon.data[src_start_y:src_start_y+copy_height,:]
                    output_lats[dst_start_y:dst_start_y+copy_height,eshift:eshift+width] = ds.lat.data[src_start_y:src_start_y+copy_height,:]
                except:
                    del datasets[f]
                    self.logger.exception(f"Error processing input file: {f}")

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
                    (height,width) = ds.lon.shape
                    eshift = eshifts[f]
                    sshift = sshifts[f]
                    rel_sshift = sshift - ref_sshift
                    dst_start_y = rel_sshift if rel_sshift > 0 else 0
                    src_start_y = -rel_sshift if rel_sshift < 0 else 0
                    copy_height = min(output_height - dst_start_y, height - src_start_y)

                    # sanity check lat/lon alignment
                    if check_alignment:
                        maxlondiff = np.max(np.abs(
                            output_lons[dst_start_y:dst_start_y + copy_height, eshift:eshift + width] -
                            ds.lon.data[src_start_y:src_start_y + copy_height,:]))
                        maxlatdiff = np.max(np.abs(
                            output_lats[dst_start_y:dst_start_y + copy_height, eshift:eshift + width] -
                            ds.lat.data[src_start_y:src_start_y + copy_height,:]))
                        if maxlondiff > 1e-7 or maxlatdiff > 1e-7:
                            raise Exception(f"Internal error: maxlondiff={maxlondiff},maxlatdiff={maxlatdiff}")

                    output_arrays = { "lat": output_lats, "lon": output_lons }

                    for v in ds.variables:
                        if v != "time" and v != "lat" and v != "lon":
                            arr = np.zeros(shape=(1,output_height,output_width),dtype=ds[v].dtype)
                            arr[0,:,:] = np.nan
                            arr[0,dst_start_y:dst_start_y+copy_height,eshift:eshift+width] = ds[v].data[0,src_start_y:src_start_y+copy_height,:]
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
        for xshift in range(-self.max_shift,self.max_shift+1,10):
            # x-axis runs approx from west to east
            ref_idx_x = 0
            other_idx_x = 0
            if xshift < 0:
                other_idx_x = -xshift # shift other "west"
            else:
                ref_idx_x = xshift # shift other "east"
            for yshift in range(-self.max_shift, self.max_shift + 1, 10):
                # y-axis runs approx from north to south
                ref_idx_y = 0
                other_idx_y = 0
                if yshift < 0:
                    other_idx_y = -yshift  # shift other "north"
                else:
                    ref_idx_y = yshift  # shift other "south"
                if float(ref_ds.lat[ref_idx_y,ref_idx_x]) == float(other_ds.lat[other_idx_y,other_idx_x]) \
                   and float(ref_ds.lon[ref_idx_y,ref_idx_x]) == float(other_ds.lon[other_idx_y,other_idx_x]):
                   return (-yshift,-xshift)

        # no shift found...
        return None


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("input_folder")
    parser.add_argument("--output-folder",help="Output aligned versions of input files to this folder", default=None)
    parser.add_argument("--output-min-path",help="Aggregate minimum values and output to this path",default=None)
    parser.add_argument("--slice", type=int, help="Work with a NE corner slice of the data of this size, useful for testing", default=None)
    parser.add_argument("--check-alignment", action="store_true", help="Double check alignment")

    logging.basicConfig(level=logging.INFO)

    args = parser.parse_args()

    input_folder = args.input_folder
    output_folder = args.output_folder

    aligner = LandsatAlign(input_folder,output_folder,min_path=args.output_min_path)
    aligner.run(slice_size=args.slice,check_alignment=args.check_alignment)

