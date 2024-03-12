path1 = "/home/dev/Downloads/rainfall_hadukgrid_uk_1km_day_18910101-18910131.nc"
path2 = "/home/dev/Projects/CHUK/EOCIS-CHUK-GRID-100M-v1.0.nc"
path3 = "/home/dev/Downloads/tasmax_rcp85_land-cpm_uk_5km_05_day_19801201-19901130.nc"

import xarray as xr

ds1 = xr.open_dataset(path1)
ds2 = xr.open_dataset(path2)
ds3 = xr.open_dataset(path3)

import matplotlib.pyplot as plt
import cartopy.crs as ccrs


import cartopy.feature as cfeature
import shapely.geometry as sgeom

import sys

import argparse

# xmin=180000
# xmax=183000
# ymin=30000
# ymax=33000
xmin=179000
xmax=186000
ymin=29000
ymax=36000

ds1 = ds1.sel(projection_x_coordinate=slice(xmin,xmax),projection_y_coordinate=slice(ymin,ymax))
ds2 = ds2.sel(x=slice(xmin,xmax),y=slice(ymax,ymin))
ds3 = ds3.sel(projection_x_coordinate=slice(xmin,xmax),projection_y_coordinate=slice(ymin,ymax))


fig = plt.figure(figsize=(40,40))
ax = fig.add_subplot(1, 1, 1, projection=ccrs.OSGB())
ax.set_extent([xmin, xmax, ymin, ymax], crs=ccrs.OSGB())

ax.add_feature(cfeature.LAND)
ax.add_feature(cfeature.OCEAN)
ax.add_feature(cfeature.COASTLINE)

for j in range(len(ds2.x)):
    for i in range(len(ds2.y)):
        polygon = sgeom.Polygon(shell=[(ds2.x_bnds[i][0],ds2.y_bnds[j][0]),(ds2.x_bnds[i][1],ds2.y_bnds[j][0]),(ds2.x_bnds[i][1],ds2.y_bnds[j][1]),(ds2.x_bnds[i][0],ds2.y_bnds[j][1])])
        ax.add_geometries([polygon], ccrs.OSGB(),facecolor="#FF000000",edgecolor="green")

for j in range(len(ds1.projection_y_coordinate)):
    for i in range(len(ds1.projection_x_coordinate)):
        polygon = sgeom.Polygon(shell=[(ds1.projection_x_coordinate_bnds[i][0],ds1.projection_y_coordinate_bnds[j][0]),(ds1.projection_x_coordinate_bnds[i][1],ds1.projection_y_coordinate_bnds[j][0]),(ds1.projection_x_coordinate_bnds[i][1],ds1.projection_y_coordinate_bnds[j][1]),(ds1.projection_x_coordinate_bnds[i][0],ds1.projection_y_coordinate_bnds[j][1])])
        ax.add_geometries([polygon], ccrs.OSGB(),facecolor="#FF000000",edgecolor="red")

for j in range(len(ds3.projection_y_coordinate)):
    for i in range(len(ds3.projection_x_coordinate)):
        polygon = sgeom.Polygon(shell=[(ds3.projection_x_coordinate_bnds[i][0],ds3.projection_y_coordinate_bnds[j][0]),(ds3.projection_x_coordinate_bnds[i][1],ds3.projection_y_coordinate_bnds[j][0]),(ds3.projection_x_coordinate_bnds[i][1],ds3.projection_y_coordinate_bnds[j][1]),(ds3.projection_x_coordinate_bnds[i][0],ds3.projection_y_coordinate_bnds[j][1])])
        ax.add_geometries([polygon], ccrs.OSGB(),facecolor="#FF000000",edgecolor="orange")

plt.show()
