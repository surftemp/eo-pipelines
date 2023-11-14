#!/usr/bin/env python

# -*- coding: utf-8 -*-

#     create prototype EOCIS high resolution lat-lon grids for the British Isles
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
import numpy as np

def main():

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("input_path",
                        help="path to an input file in netcdf4 format containing lat and lon as 1D arrays")
    parser.add_argument("output_path",
                        help="path to output a file with lat/lon converted from 1D to 2D")

    args = parser.parse_args()

    ds = xr.open_dataset(args.input_path)

    for v in ds.variables:
        if v == "lat" or v == "lon":
            continue
        da = ds[v]
        if "lat" in da.dims:
            ds = ds.drop_vars([v])
            ds[v] = xr.DataArray(da.data,dims=("y","x"),attrs=da.attrs)

    n_lat = ds.lat.shape[0]
    n_lon = ds.lon.shape[0]
    shape_2d = (n_lat, n_lon)

    lat_data = np.broadcast_to(ds.lat.data[None].T, shape_2d)
    lat_attrs = ds.lat.attrs
    lon_data = np.broadcast_to(ds.lon.data, shape_2d)

    lon_attrs = ds.lon.attrs

    ds = ds.drop_vars(["lat", "lon"])

    ds["lat"] = xr.DataArray(lat_data, dims=("y","x"), attrs=lat_attrs)
    ds["lon"] = xr.DataArray(lon_data, dims=("y","x"), attrs=lon_attrs)

    encoding = {
        "lat": {"dtype": "float32", "zlib": True, "complevel": 5},
        "lon": {"dtype": "float32", "zlib": True, "complevel": 5}
    }

    ds.to_netcdf(args.output_path, encoding=encoding)

if __name__ == '__main__':
    main()