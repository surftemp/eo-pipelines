import xarray as xr
import os

class Processor:

    def __init__(self, input_folder, output_folder, simplify_coords):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.simplify_coords = simplify_coords

    def process(self):
        os.makedirs(self.output_folder, exist_ok=True)
        for fname in os.listdir(self.input_folder):
            if fname.endswith(".nc"):
                self.process_file(os.path.join(self.input_folder,fname),os.path.join(self.output_folder,fname))

    def process_file(self, input_path, output_path):
        ds = xr.open_dataset(input_path)
        if self.simplify_coords:
            ds = self.process_simplify_coords(ds)
        ds.to_netcdf(output_path)

    def process_simplify_coords(self,ds):
        # convert the input dataset to use lat,lon as dimensions directly (rather than indirectly through nj, ni)
        # there should be an easier way to do this?
        ds2 = xr.Dataset(attrs=ds.attrs)
        ds2["time"] = ds["time"]

        # extract lat and lon as 1-d arrays from the redundant 2-d input arrays
        ds2["lon"] = xr.DataArray(data=ds["lon"].data[0,:],dims=("lon",),attrs=ds["lon"].attrs)
        ds2["lat"] = xr.DataArray(data=ds["lat"].data[:,0],dims=("lat",),attrs=ds["lat"].attrs)

        for var in ds.variables:
            if ds[var].dims == ("time","nj","ni"):
                ds2[var] = xr.DataArray(data=ds[var].data,dims=("time","lat","lon"),attrs=ds[var].attrs)
        return ds2

if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("input_folder")
    parser.add_argument("output_folder")
    parser.add_argument("--simplify-coords",action="store_true")

    args = parser.parse_args()
    p = Processor(args.input_folder, args.output_folder, simplify_coords=args.simplify_coords)
    p.process()
