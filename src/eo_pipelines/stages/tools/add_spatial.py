# MIT License
#
# Copyright (c) 2024 National Center for Earth Observation (NCEO)
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

import xarray as xr
import argparse


class AddSpatial:

    def __init__(self, input_path, output_path, dataset_path, dataset_variables):
        self.input_path = input_path
        self.output_path = output_path
        self.dataset_path = dataset_path
        self.dataset_variables = dataset_variables

    def run(self):
        ds = xr.open_dataset(self.input_path)

        dataset_ds = xr.open_dataset(self.dataset_path)

        ids = ds.copy(deep=False)
        if len(ids.lon.shape) == 2:
            ids["lon"] = xr.DataArray(ids.lon[0, :].data.flatten(), dims=("ni",))
        if len(ids.lat.shape) == 2:
            ids["lat"] = xr.DataArray(ids.lat[:, 0].data.flatten(), dims=("nj",))
        ids = ids.set_coords(["lon", "lat"]).set_xindex("lon").set_xindex("lat")

        for variable in self.dataset_variables:
            if ":" in variable:
                source_variable = variable.split(":")[0]
                dest_variable = variable.split(":")[1]
            else:
                source_variable = dest_variable = variable
            da = dataset_ds[source_variable].interp_like(ids, method="linear")
            ds[dest_variable] = xr.DataArray(da.data, dims=("nj", "ni"))

        ds.to_netcdf(self.output_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", help="path to input netcdf4 file", required=True)
    parser.add_argument("--output-path", help="path to output netcdf4 file", required=True)
    parser.add_argument("--dataset-path", help="path to dataset to contribute variables")
    parser.add_argument("--dataset-variables", nargs="+", help="names of 1 or more variables to add")

    args = parser.parse_args()

    add = AddSpatial(args.input_path, args.output_path, args.dataset_path, args.dataset_variables)
    add.run()
