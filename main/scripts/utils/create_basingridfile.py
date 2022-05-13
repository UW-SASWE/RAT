import numpy as np
import rioxarray as rxr
import rasterio
from shapely.geometry import mapping

from utils.utils import round_pixels


def create_basingridfile(basin_bounds,basin_geometry,basingridfile_path,xres,yres):    
    xmin,ymin,xmax,ymax=basin_bounds
    nx=round_pixels((xmax-xmin)/xres)
    ny=round_pixels((ymax-ymin)/yres)
    Z=np.ones((ny,nx))
    trf=rasterio.transform.from_origin(xmin, ymax, xres, yres)
    with rasterio.open(
        basingridfile_path[:-4]+'_boundbox.tif',
        'w',
        driver='GTiff',
        height=Z.shape[0],
        width=Z.shape[1],
        count=1,
        dtype=Z.dtype,
        crs='+init=epsg:4326',
        transform=trf
    ) as dst:
        dst.write(Z, 1)
    basin_grid = rxr.open_rasterio(basingridfile_path[:-4]+'_boundbox.tif')
    basin_grid=basin_grid.rio.clip(basin_geometry.apply(mapping),drop=False)
    basin_grid.rio.to_raster(basingridfile_path)