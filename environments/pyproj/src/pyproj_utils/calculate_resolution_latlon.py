grid_path = "/home/dev/Projects/CHUK/EOCIS-CHUK-GRID-100M-v0.4.nc"

import xarray as xr
import pyproj
import math

ds = xr.open_dataset(grid_path)

height = ds.y.shape[0]
width = ds.x.shape[0]

cy = height // 2
cx = width // 2

# get center as lat/lon

transformer = pyproj.Transformer.from_crs(27700, 4326)
to_x, to_y = transformer.transform([ds.x[cx]],[ds.y[cy]])
print(to_x,to_y)

lat_resolution = 100/111111
lon_resolution = lat_resolution*math.cos(math.degrees(to_y[0]))

print(lat_resolution,lon_resolution)

