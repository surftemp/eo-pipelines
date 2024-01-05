import geopandas


path = "/home/dev/Projects/greenland_lakes/lakes.geojson"
shp_path = "/home/dev/Projects/greenland_lakes/20170101-ESACCI-L3S_GLACIERS-IML-MERGED-fv1/20170101-ESACCI-L3S_GLACIERS-IML-MERGED-fv1.shp"

# df = geopandas.read_file(path,where="LakeID=765")
# Leverett lake ids are 765 766 1019 1020
# Sources are S1, S2
df = geopandas.read_file(path,where="Source='S2'")
df = df.cx[-50.164:-49.926, 67.076:67.02]
df.plot()
from matplotlib import pyplot as plt
plt.show()