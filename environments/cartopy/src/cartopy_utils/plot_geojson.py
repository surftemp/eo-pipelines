import geopandas


path = "/home/dev/github/eo-pipelines/environments/cartopy/src/cartopy_utils/extract/lakes/lake112.geojson"

# df = geopandas.read_file(path,where="LakeID=765")
# Leverett lake ids are 765 766 1019 1020
# Sources are S1, S2
df = geopandas.read_file(path)
# df = df.cx[-50.164:-49.926, 67.076:67.02]
df.plot()
from matplotlib import pyplot as plt
plt.show()