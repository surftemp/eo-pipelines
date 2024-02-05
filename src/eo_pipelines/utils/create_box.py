import math
import numpy as np
import xarray as xr

m_per_degree = 111000  # approx m per degree of latitude

class Box:

    def __init__(self, min_lat, min_lon, max_lat, max_lon, resolution):
        self.resolution = resolution
        self.min_lat = min_lat
        self.min_lon = min_lon
        self.max_lat = max_lat
        self.max_lon = max_lon

    def get_resolution(self):
        mid_lat = (self.min_lat + self.max_lat) / 2
        resolution_degrees_y = self.resolution / m_per_degree  # height of each target pixel in degrees
        # work out width of each target pixel in degrees for the mid-latitude
        # width will increase at higher latitudes
        resolution_degrees_x = resolution_degrees_y / math.cos(math.radians(mid_lat))
        return (resolution_degrees_y, resolution_degrees_x)

    def export_netcdf(self, to_path, metadata={}):
        (resolution_degrees_y, resolution_degrees_x) = self.get_resolution()
        lats = np.flip(np.arange(self.min_lat, self.max_lat, resolution_degrees_y))
        lons = np.arange(self.min_lon, self.max_lon, resolution_degrees_x)
        shape = len(lats), len(lons)

        lats2d = np.broadcast_to(lats[None].T, shape)
        lons2d = np.broadcast_to(lons, shape)

        target_ds = xr.Dataset(attrs=metadata)
        target_ds["lat"] = xr.DataArray(lats2d, dims=("nj", "ni"),
                                                         attrs={"units": "degrees_north", "standard_name": "latitude"})
        target_ds["lon"] = xr.DataArray(lons2d, dims=("nj", "ni"),
                                                         attrs={"units": "degrees_east", "standard_name": "longitude"})

        target_ds.to_netcdf(to_path)


box = Box(-11.5, -76.7, -11.3, -76.5, 100)
box.export_netcdf("andes_box_100m.nc")
