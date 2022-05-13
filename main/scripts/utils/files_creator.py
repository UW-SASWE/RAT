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

    def create_basin_domain_nc_file(elevation_tif_filepath,basingridfile_path,output_path):
        ele=rxr.open_rasterio(elevation_tif_filepath)
        basin_grid=rxr.open_rasterio(basingridfile_path)
        #Interpolating and cropping elevation xarray
        basin_ele=ele.interp(x=basin_grid['x'].values,y=basin_grid['y'].values)
        #Changing vars and their names in xarrays
        basin_ele=basin_ele.drop_vars(['band','spatial_ref']).rename({'x':'lon','y':'lat'})[0]
        basin_grid=basin_grid.drop_vars(['band','spatial_ref']).rename({'x':'lon','y':'lat'})[0]
        #Creating a dataset from two xarrays
        basin_domain = basin_grid.to_dataset(name = 'mask')
        basin_domain['elev'] = basin_ele
        #Saving dataset as netcdf
        basin_domain.to_netcdf(output_path)