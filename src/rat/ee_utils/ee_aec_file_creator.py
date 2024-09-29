import ee
import os
import geopandas as gpd
import pandas as pd
import numpy as np
from itertools import zip_longest,chain
from rat.ee_utils.ee_utils import poly2feature
from pathlib import Path
from scipy.optimize import minimize
from scipy.integrate import cumulative_trapezoid

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
        reducer= ee.Reducer.sum()
    )
    area=ee.Number(DEM141Count.get('elevation')).multiply(30*30).divide(1e6)
    return area

def dem_percentile(
        lat, lon, scale=30,
        percentiles=[1, 2, 3, 4, 5, 10], buffer_distance=1000,
        ee_dem_name = 'MERIT/DEM/v1_0_3'
    ):
    """
    Calculates elevations corresponding to percentile values within a buffered region around 
    a dam location.

    Parameters:
        lat (float): Latitude of the point of interest.
        lon (float): Longitude of the point of interest.
        scale (int, optional): Scale in meters for the DEM data. Default is 30.
        percentiles (list of int, optional): List of percentiles to calculate. Default is [1, 2, 3, 4, 5, 10].
        buffer_distance (int, optional): Buffer distance in meters around the point of interest. Default is 1000.
        ee_dem_name (str, optional): Name of the Earth Engine DEM dataset. Default is 'MERIT/DEM/v1_0_3'.
    Returns:
        dict: A dictionary containing the calculated statistics (min, max, and specified percentiles).
    """
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

def get_obs_aec_above_water_surface(aec, max_height):
    """
    Filters and processes the AEC (Area-Elevation Curve) data to obtain observations above a specified water surface height.

    Args:
        aec (pd.DataFrame): DataFrame containing AEC data with 'Elevation' and 'CumArea' columns.
        max_height (float): The maximum height of the water surface to filter the observations.

    Returns:
        pd.DataFrame: A DataFrame containing the filtered and processed AEC data with 'Elevation' and 'CumArea' columns.
    """
    obs_aec_above_water = aec[aec['Elevation'] < max_height]
    obs_aec_above_water = obs_aec_above_water.sort_values('Elevation')
    obs_aec_above_water['CumArea_diff'] = obs_aec_above_water['CumArea'].diff()
    obs_aec_above_water['z_score'] = (obs_aec_above_water['CumArea_diff'] - obs_aec_above_water['CumArea'].mean()) / obs_aec_above_water['CumArea'].std()
    max_z_core_idx = obs_aec_above_water['z_score'].idxmax()
    obs_aec_above_water = obs_aec_above_water.loc[max_z_core_idx:, :]
    obs_aec_above_water = obs_aec_above_water[['Elevation', 'CumArea']]

    return obs_aec_above_water

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


def aec_file_creator(
        reservoir_shpfile, shpfile_column_dict, aec_dir_path, 
        scale=30, dam_bottom_elevation_percentile=1, force_extrapolate=False):
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
        reservoir_name = str(reservoir[shpfile_column_dict['unique_identifier']])
        dam_height = float(reservoir[shpfile_column_dict['dam_height']])
        dam_lat = float(reservoir[shpfile_column_dict['dam_lat']])
        dam_lon = float(reservoir[shpfile_column_dict['dam_lon']])
        if (reservoir_name in aec_filenames) and (not force_extrapolate):
            print(f"Skipping {reservoir_name} as its AEC file already exists. Not extrapolating, force_extrapolate is turned off.")
        elif force_extrapolate:
            print(f"Extrapolating AEC file for {reservoir_name} as force_extrapolate is turned on.")
            aec = get_obs_aec_srtm(aec_dir_path, scale, reservoir, reservoir_name)

            # Check if 'Elevation_Observed' column exists in the AEC DataFrame
            if 'Elevation_Observed' in aec.columns:
                print(f"AEC for {reservoir_name} has already been extrapolated.")
            else:
                # Replace the $SELECTION_PLACEHOLDER$ with a call to the new function
                extrapolate_reservoir(
                    reservoir_name, dam_lat, dam_lon, dam_height, scale, 
                    dam_bottom_elevation_percentile, aec, aec_dir_path
                )
        else:
            get_obs_aec_srtm(aec_dir_path, scale, reservoir, reservoir_name)

    print("AEC file exists for all reservoirs in this basin")
    return 1

def extrapolate_reservoir(
    reservoir_name, dam_lat, dam_lon, dam_height, scale, 
    dam_bottom_elevation_percentile, aec, aec_dir_path
):
    """
    Extrapolates the reservoir's Area-Elevation-Capacity (AEC) curve by fitting a polynomial to observed data and adds column for storage.
    - The function first calculates the dam bottom elevation using the specified percentile.
    - It then fits polynomials of degrees 3, 2, and 1 to the observed AEC data, stopping at the first successful fit.
    - The predicted AEC curve is generated and saved to a CSV file in the specified directory.
    - The initial guess and polynomial equation are stored as comments in the CSV file.
    
    Parameters:
        reservoir_name (str): Name of the reservoir.
        dam_lat (float): Latitude of the dam.
        dam_lon (float): Longitude of the dam.
        dam_height (float): Height of the dam.
        scale (float): Scale factor for the DEM data.
        dam_bottom_elevation_percentile (float): Percentile to determine the dam bottom elevation.
        aec (pd.DataFrame): DataFrame containing observed AEC data with columns 'CumArea' and 'Elevation'.
        aec_dir_path (str): Directory path to save the predicted AEC file.
    Returns:
        pd.DataFrame: DataFrame containing the predicted storage values with columns 'CumArea', 'Elevation', and 'Elevation_Observed'.
    """
    # Obtain the elevation of the dam bottom using the specified percentile
    dam_bottom_stats = dem_percentile(
        lat=dam_lat,
        lon=dam_lon,
        scale=scale,
        percentiles=[dam_bottom_elevation_percentile],
        buffer_distance=1000
    )
    dam_bottom_elevation = dam_bottom_stats[f"dem_p{dam_bottom_elevation_percentile}"]
    dam_top_elevation = dam_bottom_elevation + dam_height
    print(f"Dam bottom elevation for {reservoir_name} is {dam_bottom_elevation}")
    print(f"Dam top elevation for {reservoir_name} is {dam_top_elevation}")

    obs_aec_above_water = get_obs_aec_above_water_surface(
        aec, dam_top_elevation
    )

    x = obs_aec_above_water['CumArea']
    y = obs_aec_above_water['Elevation']

    # Try fitting polynomials starting from degree 3 down to 1
    for degree in [3, 2, 1]:
        result, initial_guess = fit_polynomial(x, y, degree, dam_bottom_elevation)
        if result.success:
            print(f"Successfully fitted a degree {degree} polynomial for {reservoir_name}")
            break
        else:
            print(f"Failed to fit a degree {degree} polynomial for {reservoir_name}, trying lower degree")
    
    # Generate x_pred by combining a range of values from 0 to the maximum of x with the unique values from aec['CumArea']
    # and then sorting them. This ensures that x_pred includes all unique cumulative area values from the AEC data.
    x_pred = np.sort(np.unique(np.concatenate([np.arange(0, np.max(x), 0.25), obs_aec_above_water['CumArea'].values])))
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
    with open(os.path.join(aec_dir_path, f"{reservoir_name}.csv"), 'w') as f:
        f.write(f"# Initial guess: {initial_guess[::-1]}\n")
        f.write(f"# Polynomial degree: {degree}\n")
        polynomial_equation = np.poly1d(result.x[::-1])
        for line in str(polynomial_equation).split('\n'):
            f.write(f"# {line}\n")
        predicted_storage_df.to_csv(f, index=False)
    print(f"Saved predicted AEC for {reservoir_name} to {os.path.join(aec_dir_path, f'{reservoir_name}.csv')}")

    return predicted_storage_df

def get_obs_aec_srtm(aec_dir_path, scale, reservoir, reservoir_name):
    """
    Generates an observed Area-Elevation Curve (AEC) file for a given reservoir using SRTM data.
    The function checks if an AEC file already exists for the given reservoir. If it does, it reads the file and returns the data.
    If not, it generates the AEC using SRTM data. It saves the data as a csv file and returns the aecas a pandas dataframe.
    Parameters:
        aec_dir_path (str): The directory path where the AEC file will be saved.
        scale (int): The scale at which to perform the elevation calculations.
        reservoir (object): The reservoir object containing geometry information.
        reservoir_name (str): The name of the reservoir.
    Returns:
        pd.DataFrame: A DataFrame containing the elevation and cumulative area data.
    """
    print(f"Generating observed AEC file for {reservoir_name} from SRTM")
    aec_dst_fp = os.path.join(aec_dir_path,reservoir_name+'.csv')

    if Path(aec_dst_fp).exists():
        print(f"SRTM AEC exists for {reservoir_name} at {aec_dst_fp}")
        aec_df = pd.read_csv(aec_dst_fp, comment='#')
    else:
        reservoir_polygon = reservoir.geometry
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
        aec_df.to_csv(aec_dst_fp,index=False)
        print(f"Observed AEC obtained from SRTM for {reservoir_name}")

    return aec_df
