import geopandas as gpd
import os
import xarray as xr
import rioxarray as rxr
import pandas as pd
from shapely.geometry import mapping

def create_meterological_ts(roi, nc_file_path, output_csv_path):
    """
    Create a meteorological timeseries for a given region of interest (ROI) using combined meteorological
    data stored in a NetCDF file produced by RAT, and save the output as a CSV file.

    This function extracts time-series data for the variables 'precip', 'tmin', 'tmax', and 'wind' for the specified
    geometry (ROI), calculates the spatial average for each time step, and stores the results in a CSV file.

    Parameters:
    ----------
    roi : shapely.geometry.Polygon 
        A shapely geometry representing the region of interest (ROI) for which the meteorological
        timeseries will be extracted. The geometry should be in the WGS84 coordinate reference system (CRS), same as the 
        NetCDF dataset.
        
    nc_file_path : str
        Path to the NetCDF file containing combined meteorological data produced by RAT. The NetCDF file should include
        the variables 'precip', 'tmin', 'tmax', and 'wind', and have appropriate spatial dimensions such as 'lat' and 'lon'.
         or 'longitude' and 'latitude'. If crs is not specified, it is assumed to be a WGS84.
        
    output_csv_path : str
        Path where the output CSV file will be saved (or appended in case, file exists). The file will contain the time series data for the spatially averaged
        values of the meteorological variables ('precip', 'tmin', 'tmax', 'wind').

    Returns:
    -------
    None
        The function does not return any value but saves the meteorological time series to the specified CSV file.
    
    Raises:
    ------
    ValueError : 
        If the spatial dimensions ('lon', 'lat', 'longitude', or 'latitude') are not found in the NetCDF dataset.
        
    Warning :
        If the CRS of the GeoDataFrame is not set, appropriate warnings will be raised and file will not be created.
        
    Notes:
    ------
    - The function clips the dataset based on the ROI geometry and calculates the spatial average for each time step.
    - The output CSV will contain columns for 'time', 'precip', 'tmin', 'tmax', and 'wind' with their spatially averaged values.
    - If the output CSV exists, data will be appended and in case of duplicate dates, latest data will be kept for each date.

    Example:
    --------
    # Create a GeoDataFrame representing the ROI (Polygon)
    roi = geopandas.GeoDataFrame(...)

    # Specify the path to the NetCDF file
    nc_file_path = 'path_to_netcdf_file.nc'

    # Specify the path to save the output CSV
    output_csv_path = 'output_timeseries.csv'

    # Create the meteorological time series and save it to CSV
    create_meterological_ts(roi, nc_file_path, output_csv_path)
    """
    
    print("Creating meterological timeseries for a given geometry using comibined meteorlogical NetCDF produced by RAT.")
    # Load the NetCDF file as an xarray Dataset
    ds = xr.open_dataset(nc_file_path)

    # Ensure spatial dimensions are set correctly as x and y for rioxarray use
    if 'lon' in ds.dims and 'lat' in ds.dims:
        ds = ds.rename({'lon': 'x', 'lat': 'y'})  
    elif 'longitude' in ds.dims and 'latitude' in ds.dims:
        ds = ds.rename({'longitude': 'x', 'latitude': 'y'})  
    else:
        raise ValueError("Spatial dimensions not found. Expected 'lon', 'lat', 'longitude', or 'latitude'.")

    # Set spatial dimensions
    ds = ds.rio.set_spatial_dims(x_dim='x', y_dim='y')

    # Sets default CRS for the dataset if missing
    if ds.rio.crs is None:
        print("CRS not found for dataset. Setting CRS to EPSG:4326.")
        ds.rio.write_crs("EPSG:4326", inplace=True)

    # Set CRS for GeoDataFrame
    if gdf.crs is None:
        print("CRS not found for GeoDataFrame. Please set it manually.")
        return None

    # Convert the combined geometry to a format that rioxarray can work with
    geometries = [mapping(roi)]

    # Clip the xarray Dataset using the ROI geometry
    try:
        ds_clipped = ds.rio.clip(geometries, from_disk=True)
    except Exception as e:
        print(f"Error during clipping: {e}")
        return

    # Calculate the spatial average for each time step
    spatial_mean = ds_clipped.mean(dim=['x', 'y'])

    # Convert the spatial mean to a pandas DataFrame
    df = pd.DataFrame({
        'time': spatial_mean.time.values,
        'precip': spatial_mean.precip.values,
        'tmin': spatial_mean.tmin.values,
        'tmax': spatial_mean.tmax.values,
        'wind': spatial_mean.wind.values
    })

    # Append the data if file exists
    if os.path.exists(output_csv_path):
        print(f"File {output_csv_path} exists. Appending new data and removing duplicates.")
        existing_df = pd.read_csv(output_csv_path, parse_dates=['time'])
        combined_df = pd.concat([existing_df, df], ignore_index=True)
        # Remove duplicates, keeping the latest entry for each date
        combined_df = combined_df.sort_values(by='time').drop_duplicates(subset='time', keep='last')
        combined_df.to_csv(output_csv_path, index=False)
    else:
        # Save the new data
        df.to_csv(output_csv_path, index=False)

    print(f"CSV file has been updated and saved to {output_csv_path}.")