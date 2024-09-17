import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import shapely.geometry as sgeom
import xarray as xr

path = "/home/dev/Downloads/EOCIS-CHUK-GRID-1000M-v1.0.nc"

ds = xr.open_dataset(path)

xmin=ds.x.min().item()
xmax=ds.x.max().item()
ymin=ds.y.min().item()
ymax=ds.y.max().item()

fig = plt.figure(figsize=(40,40))
ax = fig.add_subplot(1, 1, 1, projection=ccrs.OSGB())
ax.set_extent([xmin, xmax, ymin, ymax], crs=ccrs.OSGB())

ax.add_feature(cfeature.LAND)
ax.add_feature(cfeature.OCEAN)
ax.add_feature(cfeature.COASTLINE)

polygon = sgeom.Polygon(shell=[(xmin,ymin),(xmin,ymax),(xmax,ymax),(xmax,ymin)])
ax.add_geometries([polygon], ccrs.OSGB(),facecolor="#FF000000",edgecolor="green")

plt.show()
