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

"""Utility program for extracting a section of a large regular latlon grid that covers a particular ROI"""

import xarray as xr
import argparse

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("input_grid_path", help="path to input grid with 1D lat and lon coords")
    parser.add_argument("roi_grid_path", help="path to region of interest grid with 1d or 2d lat and lon")
    parser.add_argument("output_grid_path",help="path to write output subset grid with 1D lat and lon")

    args = parser.parse_args()

    # extract a lat/lon bounding box from the ROI
    roi_ds = xr.open_dataset(args.roi_grid_path)
    lat_min = float(roi_ds.lat.min())
    lat_max = float(roi_ds.lat.max())
    lon_min = float(roi_ds.lon.min())
    lon_max = float(roi_ds.lon.max())

    # read the input grid
    ds = xr.open_dataset(args.input_grid_path)

    (nlat,) = ds.lat.shape
    (nlon,) = ds.lat.shape
    lat_slice = slice(lat_min,lat_max) if ds.lat[0] < ds.lat[nlat-1] else slice(lat_max,lat_min)
    lon_slice = slice(lon_min,lon_max)

    # extract a part of the input grid and write it out
    target_ds = ds.sel(lat=lat_slice,lon=lon_slice)

    target_ds.to_netcdf(args.output_grid_path)

