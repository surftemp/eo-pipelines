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

import xarray as xr

def copy_metadata_ds(ds, ds_ref):
    # maybe there is a quicker way to do this

    # copy over global attributes
    for (name, value) in ds_ref.attrs.items():
        if name not in ds.attrs:
            ds.attrs[name] = value

    # copy over attributes for each variable
    for (vname, v) in ds_ref.variables.items():
        if vname in ds.variables:
            for (name, value) in v.attrs.items():
                if name not in ds[vname].attrs:
                    ds[vname].attrs[name] = value

def copy_metadata(input_path, reference_path, output_path):

    ds = xr.open_dataset(input_path)
    ds_ref = xr.open_dataset(reference_path)

    copy_metadata(ds, ds_ref)

    ds.to_netcdf(output_path)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("input_data_path", help="input data file to be updated")
    parser.add_argument("reference_data_path", help="reference file from which metadata is to be copied in")
    parser.add_argument("output_data_path", help="path to write modified data")

    args = parser.parse_args()
    copy_metadata(args.input_data_path, args.reference_data_path, args.output_data_path)