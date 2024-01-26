
import xarray as xr
import numpy as np

from cartopy_utils.netcdf2html.fragments.image import save_image, save_image_falsecolour

path = "/home/dev/Projects/CHUK/jun_min900m.nc"

ds = xr.open_dataset(path)

save_image_falsecolour(np.flipud(ds["B4"].data), np.flipud(ds["B3"].data), np.flipud(ds["B2"].data), "/home/dev/Projects/CHUK/jun_min900m.png")