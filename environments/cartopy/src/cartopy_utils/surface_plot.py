
import xarray
import matplotlib.pyplot as plt


# ds = xr.open_dataset("/gws/nopw/j04/esacci_sst/software/gbcs/LUT/DEM-ESACCI-ASTER-2000-P16Y-v3.nc")
# ds = ds.sel(lat=slice(77,76),lon=slice(-55,-54))
# ds.to_netcdf("dem_900m_N76W055.nc")

input_path = "/home/dev/Downloads/ASTGTMV003_N76W055_dem.nc"
input_path_900m = "/home/dev/Downloads/dem_900m_N76W055.nc"

ds = xarray.open_dataset(input_path)
ds_900m = xarray.open_dataset(input_path_900m)
print(ds_900m)

# ds_900m["dem"].plot.surface()

# da = ds_900m["dem"]
# print(da.max())
# da.plot.surface(figsize=(10,10))
# amax = da.argmax(["lat","lon"])
# latmax = int(amax["lat"])
# lonmax = int(amax["lon"])

# area = da.isel(lat=slice(latmax-4,latmax+4),lon=slice(lonmax-4,lonmax+4))
# print(area)
# v = da.isel(lat=latmax,lon=lonmax).data
# area.plot.surface()

# plt.show()

# import sys
# sys.exit(-1)

da = ds["ASTER_GDEM_DEM"]
# da.plot.surface(figsize=(10,10))
amax = da.argmax(["lat","lon"])
latmax = int(amax["lat"])
lonmax = int(amax["lon"])

area = da.isel(lat=slice(latmax-100,latmax+100),lon=slice(lonmax-100,lonmax+100))
v = da.isel(lat=latmax,lon=lonmax).data
area.plot.surface()
print(v)
# smoothed_da = da.rolling(lat=30,lon=30).mean()
plt.show()
