import ee
import os
import geopandas as gpd
import pandas as pd
import numpy as np
from itertools import zip_longest,chain
from rat.ee_utils.ee_utils import poly2feature, simplify_geometry
from pathlib import Path
from scipy.optimize import minimize
from scipy.integrate import cumulative_trapezoid
from shapely.geometry import Point


WATER_SAREA_DIFF_Z_THRESHOLD = 3.0
BUFFER_DIST = 500
DEM = ee.Image('USGS/SRTMGL1_003')

def grouper(iterable, n, *, incomplete='fill', fillvalue=None):
    "Collect data into non-overlapping fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, fillvalue='x') --> ABC DEF Gxx
    # grouper('ABCDEFG', 3, incomplete='strict') --> ABC DEF ValueError
    # grouper('ABCDEFG', 3, incomplete='ignore') --> ABC DEF
    args = [iter(iterable)] * n
    if incomplete == 'fill':
        return zip_longest(*args, fillvalue=fillvalue)
    if incomplete == 'strict':
        return zip(*args, strict=True)
    if incomplete == 'ignore':
        return zip(*args)
    else:
        raise ValueError('Expected fill, strict, or ignore')

def _aec(n,elev_dem,roi, scale=30):
    ii = ee.Image.constant(n).reproject(elev_dem.projection())
    DEM141 = elev_dem.lte(ii)

    DEM141Count = DEM141.reduceRegion(
        geometry= roi,
        scale= scale,
        reducer= ee.Reducer.sum(),
        maxPixels = 1e16,
        bestEffort=True
    )
    area=ee.Number(DEM141Count.get('elevation')).multiply(30*30).divide(1e6)
    return area

def get_obs_aec_above_water_surface(aec):
    """
    Filters and processes the AEC (Area-Elevation Curve) data to obtain observations above a specified water surface height.

    Args:
        aec (pd.DataFrame): DataFrame containing AEC data with 'Elevation' and 'CumArea' columns.

    Returns:
        pd.DataFrame: A DataFrame containing the filtered and processed AEC data with 'Elevation' and 'CumArea' columns.
        Boolean: Boolean indicating whether the AEC data has water surface or not.
    """
    obs_aec_above_water = aec.sort_values('Elevation')
    obs_aec_above_water['CumArea_diff'] = obs_aec_above_water['CumArea'].diff()
    obs_aec_above_water['z_score'] = (obs_aec_above_water['CumArea_diff'] - obs_aec_above_water['CumArea_diff'].mean()) / obs_aec_above_water['CumArea_diff'].std()
    max_z_score = obs_aec_above_water['z_score'].max()
    max_z_core_idx = obs_aec_above_water['z_score'].idxmax()
    if max_z_score > WATER_SAREA_DIFF_Z_THRESHOLD:
        obs_aec_above_water = obs_aec_above_water.loc[max_z_core_idx:, :]
        obs_aec_above_water = obs_aec_above_water[['Elevation', 'CumArea']]
        water_surface_exists = True
        print(f"Clipped to elevations above water surface. Max Sarea difference zscore is {max_z_score}.")
    else:
        obs_aec_above_water = obs_aec_above_water[['Elevation', 'CumArea']]
        water_surface_exists = False
        print(f"Skipped clipping as No water surface was found using AEC because max 'sarea difference' zscore is {max_z_score}. Either the reservoir was created after DEM data was aquired or the AEC is already extrapolated.")
        pass
    
    return obs_aec_above_water, water_surface_exists

def calculate_storage(aec_df):
    """
    Calculate the storage of a reservoir from its Area-Elevation Curve (AEC).

    Parameters:
    aec_df (pd.DataFrame): DataFrame containing 'Elevation' and 'CumArea' columns.

    Returns:
    pd.DataFrame: DataFrame with an additional 'Storage' column representing the storage in cubic meters.
    """
    elevation_normalized = (aec_df['Elevation'] - aec_df['Elevation'].min())

    # cumulative_trapezoid takes two parameters.
    # y = y-axis locations of points. these values will be integrated. 
    # x = x-axis locations of points, where each y value is sampled. Area.
    storage = cumulative_trapezoid(
        elevation_normalized, 
        aec_df['CumArea'] * 1e6
    )
    storage = np.insert(storage, 0, 0)

    aec_df['Storage'] = storage
    aec_df['Storage (mil. m3)'] = storage * 1e-6
    return aec_df


# Function to return predicted y-values from the polynomial
def predict_y(params, x):
    return np.polyval(params, x)

# Objective function: residuals to minimize
def objective(params, x, y):
    predicted_y = predict_y(params, x)

    return np.sum((predicted_y - y)**2)

# Constraint 1: dy/dx > 0 -> derivative of the polynomial should be positive for all x
def constraint_derivative(params, x):
    # Sample points across the entire range of x (0 to np.max(x))
    x_sample = np.linspace(0, np.max(x), 100)
    
    # Get the derivative coefficients of the polynomial
    derivative_coeffs = np.polyder(params[::-1], 1)  # Take the first derivative
    
    # Evaluate the derivative at these sample points
    derivative_values = np.polyval(derivative_coeffs, x_sample)  # np.polyder gives the derivative coefficients

    # Return the minimum value of the derivative; it should be greater than 0
    return derivative_values

# Constraint 2: intercept should be within (dam_bottom, dam_bottom + 5)
def constraint_intercept(params, dam_bottom):
    a0 = params[-1]  # Intercept is the last parameter (highest degree)
    return a0 - dam_bottom, (dam_bottom + 5) - a0

# Function to perform the minimization
def fit_polynomial(x, y, degree, dam_bottom):
    # Initial guess for the polynomial parameters using np.polyfit
    initial_guess = np.polyfit(x, y, degree)
    print("Initial guess: ", initial_guess[::-1])
    
    # Set up constraints
    constraints = [
        {'type': 'ineq', 'fun': lambda params: constraint_derivative(params, x)},  # dy/dx > 0
        {'type': 'ineq', 'fun': lambda params: constraint_intercept(params, dam_bottom)[0]},  # intercept within lower bound
        {'type': 'ineq', 'fun': lambda params: constraint_intercept(params, dam_bottom)[1]}   # intercept within upper bound
    ]
    
    # Perform minimization
    result = minimize(objective, initial_guess, args=(x, y), constraints=constraints, options={'maxiter': 1000})
    
    # Print results
    print(f"Optimized polynomial for degree {degree}: {np.poly1d(result.x[::-1])}")
    return result, initial_guess


def dam_bottom_dem_percentile(
        dam_location, scale=30,
        percentiles=[1, 2, 3, 4, 5, 10], buffer_distance=500,
        ee_dem_name = 'MERIT/DEM/v1_0_3'
    ):
    """
    Calculates elevations corresponding to percentile values within a buffered region around 
    a dam location.

    Parameters:
        dam_location (shapely.geometry.point.Point): The location of the dam as a point geometry.
        scale (int, optional): Scale in meters for the DEM data. Default is 30.
        percentiles (list of int, optional): List of percentiles to calculate. Default is [1, 2, 3, 4, 5, 10].
        buffer_distance (int, optional): Buffer distance in meters around the point of interest. Default is 500.
        ee_dem_name (str, optional): Name of the Earth Engine DEM dataset. Default is 'MERIT/DEM/v1_0_3'.
    Returns:
        dict: A dictionary containing the calculated statistics (min, max, and specified percentiles).
    """
    lon, lat = dam_location.x, dam_location.y
    # Create a point geometry using the provided latitude and longitude
    point = ee.Geometry.Point([lon, lat])
    
    # Buffer the point to create a region of interest (ROI)
    feature = point.buffer(buffer_distance)
    # Load the DEM dataset
    dem = ee.Image(ee_dem_name)
    
    # Clip DEM to the buffered region (feature)
    dem_clipped = dem.clip(feature)
    
    # Define the reducer (min/max combined with percentiles)
    reducer = (ee.Reducer.minMax()
               .combine(ee.Reducer.percentile(percentiles), '', True))
    
    # Reduce the clipped DEM over the feature's geometry to get statistics
    stats = dem_clipped.reduceRegion(
        reducer=reducer,
        geometry=feature,
        scale=scale,  # Scale appropriate for MERIT DEM (30 meters)
        maxPixels=1e9
    )
    
    # Set the calculated statistics as properties of the feature
    return stats.getInfo()

def get_closest_point_to_dam(
        reservoir, dam_location, grwl_fp, buffer_distance=90
    ):
    """
    Gets the closest point to the dam for a given reservoir.

    Parameters:
        reservoir (gpd.GeoSeries): GeoSeries containing the reservoir geometry.
        dam_location (shapely.geometry.point.Point): The location of the dam as a point geometry.
        buffer_distance (int): The buffer distance around the reservoir in meters.
        grwl_fp (Path): The file path to the GRWL data.

    Returns:
        shapely.geometry.point.Point: The closest point to the dam.
    """
    grwl = gpd.read_file(grwl_fp)

    # Select the reservoir
    reservoir_df = reservoir

    # Clip GRWL by taking a 1 degree buffer around reservoir_df
    reservoir_df_buffered = reservoir_df.buffer(1)
    grwl_clipped = gpd.clip(grwl, reservoir_df_buffered)

    # Convert the CRS to the local UTM projection
    utm_crs = reservoir_df.estimate_utm_crs()
    grwl_utm = grwl_clipped.to_crs(utm_crs)
    reservoir_utm = reservoir_df.to_crs(utm_crs)

    # Take a buffer of buffer_distance meters around the grand reservoirs
    reservoir_utm_buffered = reservoir_utm.geometry.buffer(buffer_distance)

    # Convert buffered geometries back to GeoDataFrame
    reservoir_utm_buffered_gdf = gpd.GeoDataFrame(geometry=reservoir_utm_buffered, crs=utm_crs).reset_index(drop=True)

    # Reverse the clipping to keep the portion of GRWL polylines outside the reservoir polygon
    grwl_outside_reservoir = gpd.overlay(grwl_utm, reservoir_utm_buffered_gdf, how='difference')

    # Get the dam location in utm crs
    dam_location_geom = gpd.GeoDataFrame(geometry=[dam_location], crs=reservoir_df.crs).to_crs(utm_crs)

    # Convert the dam location and grwl_outside_reservoir back to lat/lon
    dam_location_latlon = dam_location_geom.to_crs(epsg=4326).geometry.iloc[0]
    grwl_outside_reservoir_latlon = grwl_outside_reservoir.to_crs(epsg=4326)

    # Find the closest point in the grwl_outside_reservoir polylines to the dam location
    closest_point_to_dam = grwl_outside_reservoir_latlon.geometry.apply(lambda geom: geom.interpolate(geom.project(dam_location_latlon)))
    closest_point_to_dam = closest_point_to_dam.iloc[closest_point_to_dam.distance(dam_location_latlon).idxmin()]

    return closest_point_to_dam


def get_dam_bottom(
        reservoir, 
        dam_location=None, 
        grwl_fp=None,
        buffer_distance=500, 
        grwl_intersection_buffer=250,
        grwl_buffer_from_dam=90,
        
    ):
    """
    Determines the dam bottom elevation for a given reservoir. It can either use centerlines of
    rivers (GRWL) to determine the likely location the downstream river bed, or use a buffered
    region around the dam location to find the 1 percentile elevation as an estimate of the 
    elevation of the dam bottom.

    Parameters:
        reservoir (gpd.GeoSeries): GeoSeries containing the reservoir geometry.
        dam_location (shapely.geometry.point.Point): The location of the dam as a point geometry.
        grwl_fp (Path): The file path to the GRWL data. If passed, GRWL river centerlines would
            be used to estimate the location of the dam bottom.  
        buffer_distance (int): Buffer distance around the dam in meters to use if the dam location
            is to be used for estimating the dam bottom.
        grwl_intersection_buffer (float): Buffer distance of a region to consider around the
            intersection of GRWL and reservoir boundary for finding the bottom-elevation from the
            distribution of elevations within that region. Defaults to 250 m.
        grwl_buffer_from_dam (float): Buffer distance to use along the GRWL centerline from the
            intersection of GRWL and reservoir boundary for estimating the location of the 
            dam bottom.

    Returns:
        tuple: (float, str)
            The dam bottom elevation and the method used ('grwl_intersection' or 'dam_location').
    """
    if grwl_fp is not None:
        # Load GRWL data
        grwl = gpd.read_file(grwl_fp)
        # Check if GRWL intersects with reservoir geometry
        intersection = grwl.intersects(reservoir.unary_union)
    else:
        intersection = np.array([False])

    if intersection.any():
        method = 'grwl_intersection'
        print("GRWL intersects with reservoir geometry.")
        closest_point_to_dam = get_closest_point_to_dam(
            reservoir, dam_location,
            grwl_fp=grwl_fp,
            buffer_distance=grwl_buffer_from_dam,  # 90 m buffer from reservoir = 3 pixels
        )
        
        # Extract latitude and longitude of the closest point
        lat, lon = closest_point_to_dam.y, closest_point_to_dam.x
        
        # Calculate the dam bottom elevation using the dam_bottom_dem_percentile function
        stats = dam_bottom_dem_percentile(closest_point_to_dam, buffer_distance=grwl_intersection_buffer)
        
        # Extract the 1 percentile elevation as the dam bottom elevation
        dam_bottom_elevation = stats['dem_p1']
    else:
        method = 'dam_location'
        if grwl_fp is None:
            print("GRWL dataset is not provided. You can download global_data which includes GRWL.")
        else:
            print("GRWL does not intersect with reservoir geometry.")
        
        # Calculate the dam bottom elevation using the dam_bottom_dem_percentile function with a 500 m buffer
        stats = dam_bottom_dem_percentile(dam_location, buffer_distance=buffer_distance)
        
        # Extract the 1 percentile elevation as the dam bottom elevation
        dam_bottom_elevation = stats['dem_p1']

    return dam_bottom_elevation, method


def extrapolate_reservoir(
    reservoir, dam_location, reservoir_name, dam_height, aec, aev_save_dir, buffer_distance=500,
    grwl_fp=None
):
    """
    Extrapolates the reservoir's Area-Elevation-Capacity (AEC) curve by fitting a polynomial to observed data and adds a column for storage.
    - The function first calculates the dam bottom elevation using the specified percentile or GRWL intersection.
    - It then fits polynomials of degrees 2 and 1 to the observed AEC data, stopping at the first successful fit.
    - The predicted AEC curve is generated and saved to a CSV file in the specified directory.
    - The initial guess and polynomial equation are stored as comments in the CSV file.
    
    Parameters:
        reservoir (gpd.GeoSeries): GeoSeries containing the reservoir geometry.
        dam_location (shapely.geometry.point.Point): The location of the dam as a point geometry.
        reservoir_name (str): Name of the reservoir.
        dam_height (float): Height of the dam.
        aec (pd.DataFrame): DataFrame containing observed AEC data with columns 'CumArea' and 'Elevation'.
        aev_save_dir (str): Directory path to save the predicted AEC file.
        buffer_distance (int, optional): Buffer distance around the dam. Default is 500. It will be used if GRWL
            doesn't intersect with the reservoir geometry or if not provided.
        grwl_fp (Path, optional): File path to the GRWL (Global River Widths from Landsat) dataset. Default is None.
    
    Returns:
        pd.DataFrame: DataFrame containing the predicted storage values with columns 'CumArea', 'Elevation', 'Storage', 'Storage (mil. m3)', and 'Elevation_Observed'.
    """
    aec_original = aec.copy()
    
    dam_bottom_elevation, method = get_dam_bottom(reservoir, buffer_distance=buffer_distance, dam_location=dam_location, grwl_fp=grwl_fp) # from GRWL downstream point
    print(f"Dam bottom elevation for {reservoir_name} is {dam_bottom_elevation}")
    
    if dam_height>0:
        dam_top_elevation = dam_bottom_elevation + dam_height
        print(f"Dam height for {reservoir_name} is {dam_height}")
        print(f"Dam top elevation for {reservoir_name} is {dam_top_elevation}")

        # Remove elevations below and above dam's bottom and top elevation. If less than 5 observations are left, then just remove elevations below dam bottom.
        aec = aec_original[(aec_original['Elevation'] > dam_bottom_elevation)&(aec_original['Elevation'] <= dam_top_elevation)]
        if len(aec) > 5:
            pass
        else:
            aec = aec_original[(aec_original['Elevation'] > dam_bottom_elevation)]
    else:
        print(f"Dam height is not available for {reservoir_name} : {dam_height}")
        print("Using all the elevation data above dam bottom elevation.")
        aec = aec_original[(aec_original['Elevation'] > dam_bottom_elevation)]
        
    x = aec['CumArea']
    y = aec['Elevation']

    # Try fitting polynomials starting from degree 3 down to 1
    for degree in [2, 1]:
        result, initial_guess = fit_polynomial(x, y, degree, dam_bottom_elevation)
        if result.success:
            print(f"Successfully fitted a degree {degree} polynomial for {reservoir_name}")
            break
        else:
            print(f"Failed to fit a degree {degree} polynomial for {reservoir_name}, trying lower degree")
    
    # Generate x_pred by combining a range of values from 0 to the maximum of x with the unique values from aec['CumArea']
    # and then sorting them. This ensures that x_pred includes all unique cumulative area values from the AEC data.
    # This was we can also store the observed SRTM AEC values.
    x_pred = np.arange(0, np.max(x), 0.25)
    x_pred = np.unique(np.concatenate((x_pred, aec['CumArea'].values)))
    x_pred = np.sort(x_pred)
    y_pred = predict_y(result.x, x_pred)

    # Create a new DataFrame with the predicted x and y values
    predicted_df = pd.DataFrame({
        'CumArea': x_pred, 
        'Elevation': y_pred
    })

    predicted_storage_df = calculate_storage(predicted_df)
    # Merge the predicted DataFrame with the observed AEC DataFrame to get the observed elevations
    predicted_storage_df = pd.merge(
        predicted_storage_df, 
        aec[['CumArea', 'Elevation']], 
        on='CumArea', 
        how='left', 
        suffixes=('', '_Observed')
    )
    # Store the initial guess and result as comments in the AEC file
    with open(os.path.join(aev_save_dir, f"{reservoir_name}.csv"), 'w') as f:
        f.write(f"# Initial guess: {initial_guess}\n")
        f.write(f"# Polynomial degree: {degree}\n")
        polynomial_equation = np.poly1d(result.x)
        for line in str(polynomial_equation).split('\n'):
            f.write(f"# {line}\n")
        f.write(f"# dam bottom method: {method}\n")
        predicted_storage_df.to_csv(f, index=False)
    print(f"Saved predicted AEC for {reservoir_name} to {os.path.join(aev_save_dir, f'{reservoir_name}.csv')}")

    return predicted_storage_df


def get_obs_aec_srtm(aec_dir_path, scale, reservoir, reservoir_name, clip_to_water_surf=False, simplification=True):
    """
    Generates an observed Area-Elevation Curve (AEC) file for a given reservoir using SRTM data.
    The function checks if an AEC file already exists for the given reservoir. If it does, it reads the file and returns the data.
    If not, it generates the AEC using SRTM data. It saves the data as a csv file and returns the AEC as a pandas dataframe.
    
    Parameters:
        aec_dir_path (str): The directory path where the AEC file will be saved.
        scale (int): The scale at which to perform the elevation calculations.
        reservoir (object): The reservoir object containing geometry information.
        reservoir_name (str): The name of the reservoir.
        clip_to_water_surf (bool): If True, clips the AEC data to elevations above the water surface. Default is False.
        simplification (bool): If true, reservoir geometry will be simplified before use (only if shape index is extremely high). Default is True.
        
    Returns:
        pd.DataFrame: A DataFrame containing the elevation and cumulative area data.
        Boolean : Boolean indicating whether the AEC data has water surface or not.
    """
    print(f"Generating observed AEC file for {reservoir_name} from SRTM")
    aec_dst_fp = os.path.join(aec_dir_path,reservoir_name+'.csv')

    if Path(aec_dst_fp).exists():
        print(f"SRTM AEC exists for {reservoir_name} at {aec_dst_fp}")
        aec_df = pd.read_csv(aec_dst_fp, comment='#')
        # if 'Elevation_Observed' column exists in the dataframe, that means the extrapolation was 
        # done already. In that case, the 'CumArea' and 'Elevation_Observed' represent the SRTM 
        # observed AEC.
        if 'Elevation_Observed' in aec_df.columns:
            # aec_df = aec_df[['CumArea', 'Elevation_Observed']].rename({'Elevation_Observed': 'Elevation'}, axis=1)
            # aec_df = aec_df.dropna(subset='Elevation')
            clip_to_water_surf = False # in this case, clipping is not required. set it to false
    else:
        reservoir_polygon = reservoir.geometry
        if simplification:
            # Below function simplifies geometry with shape index (complexity) higher than a threshold, otherwise original geometry is retained
            reservoir_polygon = simplify_geometry(reservoir_polygon)
        aoi = poly2feature(reservoir_polygon,BUFFER_DIST).geometry()
        min_elev = DEM.reduceRegion( reducer = ee.Reducer.min(),
                            geometry = aoi,
                            scale = scale,
                            maxPixels = 1e16,
                            bestEffort=True
                            ).get('elevation')
        max_elev = DEM.reduceRegion( reducer = ee.Reducer.max(),
                            geometry = aoi,
                            scale = scale,
                            maxPixels = 1e16,
                            bestEffort=True
                            ).get('elevation')

        elevs = ee.List.sequence(min_elev, max_elev, 1)
        elevs_list = elevs.getInfo()
        grouped_elevs_list = grouper(elevs_list,5)

        areas_list = []
        for subset_elevs_tuples in grouped_elevs_list:
                ## Removing None objects from grouped tuples and converting them to list and then ee.List and then calculating  area for that 
            subset_elevs_list = list(filter(lambda x: x != None, subset_elevs_tuples))
            subset_elevs = ee.List(subset_elevs_list)
            subset_areas = subset_elevs.map(lambda elevation_i: _aec(elevation_i, DEM, aoi, scale))
            subset_areas_list = subset_areas.getInfo()
                    ## Appendning areas to the main list
            areas_list.append(subset_areas_list)
                    
                ## Converting list of lists to list
        areas_list = list(chain(*areas_list))
        areas_list = np.round(areas_list,3)
                ## Preparing dataframe to write down to csv 
        aec_df = pd.DataFrame(
                    data = {
                        'Elevation' :elevs_list, 
                        'CumArea':areas_list
                        })
        print(f"Observed AEC obtained from SRTM for {reservoir_name}")

    if clip_to_water_surf:
        aec_df,water_surface_exists = get_obs_aec_above_water_surface(aec_df)
    else:
        water_surface_exists = False
    aec_df.to_csv(aec_dst_fp,index=False)

    return aec_df, water_surface_exists

def aec_file_creator(
        reservoir_shpfile, shpfile_column_dict, aec_dir_path, 
        scale=30, dam_bottom_elevation_percentile=1, 
        dam_buffer_distance=500, 
        grwl_fp=None
    ):
    """
    Creates AEC (Area-Elevation Curve) files for reservoirs based on the provided shapefile.

    Parameters:
    - reservoir_shpfile (str or gpd.geodataframe.GeoDataFrame): Path to the reservoir shapefile or a GeoDataFrame.
    - shpfile_column_dict (dict): Dictionary mapping shapefile columns to their respective data types. 
      Required keys:
        - 'unique_identifier': Column name for the unique identifier of the reservoir.
        - 'dam_height': Column name for the height of the dam.
        - 'dam_lat': Column name for the latitude of the dam.
        - 'dam_lon': Column name for the longitude of the dam.
    - aec_dir_path (str): Directory path where AEC files will be stored.
    - scale (int, optional): Scale for the AEC calculation. Default is 30.
    - dam_bottom_elevation_percentile (int, optional): Percentile for dam bottom elevation. Default is 1.
    - dam_buffer_distance (int, optional): Buffer distance around the dam. Default is 500. Will be used 
        if GRWL river centerlines doesn't intersect with reservoir boundary or if not passed. Buffer
        distance around dam location where distribution of elevation is used to estimate the
        elevation of the dam location.
    - grwl_fp (str, optional): File path to the GRWL (Global River Widths from Landsat) dataset. Default is None. 
        Can be passed as 'grwl' option in 'GLOBAL' section of config file.
    """
    # Obtaining list of csv files in aec_dir_path
    aec_filenames = []
    for f in os.listdir(aec_dir_path):
        if f.endswith(".csv"):
            aec_filenames.append(f[:-4])
    if isinstance(reservoir_shpfile, str):
        reservoirs_polygon = gpd.read_file(reservoir_shpfile)
    elif isinstance(reservoir_shpfile, gpd.geodataframe.GeoDataFrame): 
        reservoirs_polygon = reservoir_shpfile
    else:
        raise ValueError("reservoir_shpfile should be either a string or a geodataframe")
    for reservoir_no,reservoir in reservoirs_polygon.iterrows():
        # Reading reservoir information
        reservoir_gpd = gpd.GeoSeries(reservoir.geometry)
        reservoir_gpd = reservoir_gpd.set_crs(reservoirs_polygon.crs)

        reservoir_name = str(reservoir[shpfile_column_dict['unique_identifier']])
        if reservoir[shpfile_column_dict['dam_height']] is not None:
            dam_height = float(reservoir[shpfile_column_dict['dam_height']])
        else:
            dam_height = np.nan
        dam_lat = float(reservoir[shpfile_column_dict['dam_lat']])
        dam_lon = float(reservoir[shpfile_column_dict['dam_lon']])
        dam_location = Point(dam_lon, dam_lat)

        aec, water_surface_exists = get_obs_aec_srtm(aec_dir_path, scale, reservoir, reservoir_name, clip_to_water_surf=True)

        if water_surface_exists:
            extrapolate_reservoir(
                reservoir_gpd, dam_location, reservoir_name, dam_height, aec,
                aec_dir_path, grwl_fp=grwl_fp
            )
        else:
            print(f"No extrapolation was done in AEC for reservoir {reservoir_name} because of absence of water surface in AEC.")

    return 1
