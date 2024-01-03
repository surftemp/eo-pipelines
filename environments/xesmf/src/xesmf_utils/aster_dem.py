#!/usr/bin/env python

# -*- coding: utf-8 -*-

#     download and assemble ASTER DEM tiles
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

import argparse
import os
import math
import numpy as np
import xarray as xr

bash_code1 = """
#!/bin/bash

GREP_OPTIONS=''

cookiejar=$(mktemp cookies.XXXXXXXXXX)
netrc=$(mktemp netrc.XXXXXXXXXX)
chmod 0600 "$cookiejar" "$netrc"
function finish {
  rm -rf "$cookiejar" "$netrc"
}

trap finish EXIT
WGETRC="$wgetrc"

prompt_credentials() {
    echo "Enter your Earthdata Login or other provider supplied credentials"
    read -p "Username (niallmcc): " username
    username=${username:-niallmcc}
    read -s -p "Password: " password
    echo "machine urs.earthdata.nasa.gov login $username password $password" >> $netrc
    echo
}

exit_with_error() {
    echo
    echo "Unable to Retrieve Data"
    echo
    echo $1
    echo
    echo "https://e4ftl01.cr.usgs.gov//ASTER/ASTT/ASTGTM_NC.003/2000.03.01/ASTGTMV003_N64W052_dem.nc"
    echo
}

prompt_credentials
  detect_app_approval() {
    approved=`curl -s -b "$cookiejar" -c "$cookiejar" -L --max-redirs 5 --netrc-file "$netrc" https://e4ftl01.cr.usgs.gov//ASTER/ASTT/ASTGTM_NC.003/2000.03.01/ASTGTMV003_N64W052_dem.nc -w %{http_code} | tail  -1`
    if [ "$approved" -ne "302" ]; then
        # User didn't approve the app. Direct users to approve the app in URS
        exit_with_error "Please ensure that you have authorized the remote application by visiting the link below "
    fi
}

setup_auth_curl() {
    # Firstly, check if it require URS authentication
    status=$(curl -s -z "$(date)" -w %{http_code} https://e4ftl01.cr.usgs.gov//ASTER/ASTT/ASTGTM_NC.003/2000.03.01/ASTGTMV003_N64W052_dem.nc | tail -1)
    if [[ "$status" -ne "200" && "$status" -ne "304" ]]; then
        # URS authentication is required. Now further check if the application/remote service is approved.
        detect_app_approval
    fi
}

setup_auth_wget() {
    # The safest way to auth via curl is netrc. Note: there's no checking or feedback
    # if login is unsuccessful
    touch ~/.netrc
    chmod 0600 ~/.netrc
    credentials=$(grep 'machine urs.earthdata.nasa.gov' ~/.netrc)
    if [ -z "$credentials" ]; then
        cat "$netrc" >> ~/.netrc
    fi
}

fetch_urls() {
  if command -v curl >/dev/null 2>&1; then
      setup_auth_curl
      while read -r line; do
        # Get everything after the last '/'
        filename="${line##*/}"

        # Strip everything after '?'
        stripped_query_params="${filename%%\?*}"

        curl -f -b "$cookiejar" -c "$cookiejar" -L --netrc-file "$netrc" -g -o $stripped_query_params -- $line && echo || exit_with_error "Command failed with error. Please retrieve the data manually."
      done;
  elif command -v wget >/dev/null 2>&1; then
      # We can't use wget to poke provider server to get info whether or not URS was integrated without download at least one of the files.
      echo
      echo "WARNING: Can't find curl, use wget instead."
      echo "WARNING: Script may not correctly identify Earthdata Login integrations."
      echo
      setup_auth_wget
      while read -r line; do
        # Get everything after the last '/'
        filename="${line##*/}"

        # Strip everything after '?'
        stripped_query_params="${filename%%\?*}"

        wget --load-cookies "$cookiejar" --save-cookies "$cookiejar" --output-document $stripped_query_params --keep-session-cookies -- $line && echo || exit_with_error "Command failed with error. Please retrieve the data manually."
      done;
  else
      exit_with_error "Error: Could not find a command-line downloader.  Please install curl or wget"
  fi
}

fetch_urls <<'EDSCEOF'"""
bash_code2 = """EDSCEOF"""

crs = None


def fetch_dem(min_lat, max_lat, min_lon, max_lon, folder, username, password):

    def get_tile_template(lat, lon):
        # https://e4ftl01.cr.usgs.gov//ASTER/ASTT/ASTGTM_NC.003/2000.03.01/ASTGTMV003_N64W052_dem.nc
        lats = "N%02d" % int(lat) if lat > 0 else "S%02d" % int(-lat)
        lons = "W%03d" % int(-lon) if lon < 0 else "E%03d" % int(lon)
        filename = "ASTGTMV003_%s_dem.nc" % (lats + lons)
        if not os.path.exists(os.path.join(folder,filename)):
            return "https://e4ftl01.cr.usgs.gov//ASTER/ASTT/ASTGTM_NC.003/2000.03.01/" + filename + "\n"
        else:
            return ""

    latmin = min_lat
    latmax = max_lat
    lonmin = min_lon
    lonmax = max_lon

    latimin = int(latmin)
    latimax = math.ceil(latmax)
    lonimin = int(lonmin) - 1
    lonimax = math.ceil(lonmax)

    template = ""
    for y in range(latimin, latimax + 1):
        for x in range(lonimin, lonimax + 1):
            template += get_tile_template(y, x)

    if template != "":
        script = bash_code1 + "\n" + template + bash_code2
        cwd = os.getcwd()
        os.chdir(folder)
        with open("download.sh","w") as f:
            f.write(script)

        with open("passwd.txt","w") as f:
            f.write(username+"\n"+password+"\n")

        try:
            os.system("/bin/bash download.sh < passwd.txt")
        finally:
            os.unlink("passwd.txt")
            os.chdir(cwd)
    else:
        print("DEM files already downloaded")

def glue_dem(min_lat, max_lat, min_lon, max_lon, input_folder, output_path):

    lat_cache = {}
    lon_cache = {}

    def preprocessor(ds):
        # ASTER DEM tiles seem to overlap by 1 pixel in both dimensions
        # remove last latidude and longitude pixels from each tile to ensure they don't overlap
        # because this causes a problem in the merged DEM dataset
        nlat = len(ds.lat)
        nlon = len(ds.lon)
        ds2 = ds.isel(lat=range(0, nlat - 1), lon=range(0, nlon - 1))

        # get the min lat/lon of the tile
        lat_min = int(ds2.lat.min())
        lon_min = int(ds2.lon.min())

        # standardise lats/lons as there are occasionally small differences at about e-14
        # that will cause problems with dask

        if lat_min in lat_cache:
            ds2["lat"] = lat_cache[lat_min]
        else:
            lat_cache[lat_min] = ds2["lat"]

        if lon_min in lon_cache:
            ds2["lon"] = lon_cache[lon_min]
        else:
            lon_cache[lon_min] = ds2["lon"]

        global crs
        if crs is None:
            crs = ds2["crs"]
        ds2 = ds2.drop_vars("crs")
        ds2.set_coords(["lat","lon"])

        return ds2

    dem_path = input_folder + "/*.nc"

    ds = xr.open_mfdataset(dem_path, preprocess=preprocessor)
    ds = ds.sel(lat=slice(min_lat,max_lat),lon=slice(min_lon,max_lon))
    global crs
    if crs is not None:
        ds["crs"] = crs
    ds.to_netcdf(output_path, encoding={
        "ASTER_GDEM_DEM": {
            "zlib": True, "complevel": 5, "chunksizes": [1800, 1800], "dtype": "int16"
        }
    })

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output_path", help="path to write output file")
    parser.add_argument("--lat-min", type=float, help="minimum latitude, decimal degrees", required=True)
    parser.add_argument("--lon-min", type=float, help="minimum longitude, decimal degrees", required=True)
    parser.add_argument("--lat-max", type=float, help="maximum latitude, decimal degrees", required=True)
    parser.add_argument("--lon-max", type=float, help="maximum longitude, decimal degrees", required=True)
    parser.add_argument("--download-folder", help="folder for storing downloaded dem tiles", required=True)
    parser.add_argument("--skip-download", action="store_true", help="assume all tiles downloaded")

    args = parser.parse_args()

    if not args.skip_download:
        username = os.getenv("USGS_USERNAME")
        password = os.getenv("USGS_PASSWORD")

        if not username or not password:
            raise Exception("Please set USGS_USERNAME and USGS_PASSWORD environment variables")

        os.makedirs(args.download_folder, exist_ok=True)
        fetch_dem(args.lat_min,args.lat_max,args.lon_min,args.lon_max,args.download_folder,username,password)

    glue_dem(args.lat_min,args.lat_max,args.lon_min,args.lon_max,args.download_folder,args.output_path)

if __name__ == '__main__':
    main()

