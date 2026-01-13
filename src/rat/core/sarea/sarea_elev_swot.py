import requests
from io import StringIO
import os
from logging import getLogger

import numpy as np
from shapely.ops import unary_union
import geopandas as gpd
import pandas as pd
from shapely.ops import unary_union

from rat.utils.logging import LOG_NAME, NOTIFICATION, LOG_LEVEL1_NAME
log_level2 = getLogger(f"{LOG_NAME}.{__name__}")
log_level1 = getLogger(f"{LOG_LEVEL1_NAME}.{__name__}")
    
# Function to get prior lakes for given reservoirs
def compute_swot_prior_lake_matching(rat_lakes, prior_lakes, rat_lake_id_field, prior_lake_id_field, drop=False):
    """
    Matches prior SWOT lake polygons to target rat_lake polygons.

    Parameters:
        rat_lakes (GeoDataFrame): Target lake polygons (e.g., GRAND)
        prior_lakes (GeoDataFrame): Prior polygons with time series (SWOT PLD)
        rat_lake_id_field (str): Unique ID field in rat_lakes
        prior_lake_id_field (str): Unique ID field in prior_lakes
        drop (bool): If True, drops rat lakes with no matches. If False, includes them with NaNs.

    Returns:
        GeoDataFrame: One row per matched pair, with target_coverage, target_intersection, total_target_coverage
    """
    log_level2.info("Finding SWOT Prior Lakes for the RAT reservoirs:")
    # Ensure CRS matches
    if rat_lakes.crs != prior_lakes.crs:
        prior_lakes = prior_lakes.to_crs(rat_lakes.crs)

    matches = []

    # Create spatial index for prior_lakes
    prior_sindex = prior_lakes.sindex

    for idx, rat_row in rat_lakes.iterrows():
        union_parts = []
        local_matches = []
        
        # Get rat lake's geometry and area
        rat_geom = rat_row.geometry
        if rat_geom:
            rat_area = rat_geom.area
            rat_id = rat_row[rat_lake_id_field]

            # Get candidate prior lakes via spatial index
            possible_matches_idx = list(prior_sindex.intersection(rat_geom.bounds))
            possible_matches = prior_lakes.iloc[possible_matches_idx]

            for _, prior_row in possible_matches.iterrows():
                # Get prior lake's geometry
                prior_geom = prior_row.geometry
                prior_id = prior_row[prior_lake_id_field]
                # Check if prior lake geometry intersects with rat lake's geometry
                if not rat_geom.intersects(prior_geom):
                    continue
                # Get the intersection geometry and make sure it is not empty
                intersection = rat_geom.intersection(prior_geom)
                if intersection.is_empty:
                    continue
                # Get the intersection area and prior lake area
                intersection_area = intersection.area
                prior_area = prior_geom.area

                target_coverage = intersection_area * 100 / rat_area
                target_intersection = intersection_area * 100 / prior_area

                union_parts.append(intersection)

                local_matches.append({
                    **rat_row.to_dict(),
                    prior_lake_id_field: prior_id,
                    'target_coverage': target_coverage,
                    'target_intersection': target_intersection,
                    'intersection_geom': intersection
                })
            else:
                pass

        if union_parts:
            union_geom = unary_union(union_parts)
            total_coverage = union_geom.area * 100 / rat_area
            for m in local_matches:
                m['total_target_coverage'] = total_coverage
            matches.extend(local_matches)

        elif not drop:
            # No matches but keep this lake with NaNs
            matches.append({
                **rat_row.to_dict(),
                prior_lake_id_field: pd.NA,
                'target_coverage': pd.NA,
                'target_intersection': pd.NA,
                'total_target_coverage': 0.0,
                'intersection_geom': pd.NA  
            })

    if len(matches) == 0:
        log_level2.info("No SWOT prior lakes were found for any of the RAT reservoirs.")
        return None
    else:
        pass
    
    # Convert to GeoDataFrame
    result_gdf = gpd.GeoDataFrame(matches, geometry='intersection_geom', crs=rat_lakes.crs)

    # Replace geometry with original rat lake geometry (clean if needed)
    result_gdf['geometry'] = result_gdf.geometry.buffer(0)
    result_gdf = result_gdf.drop(columns='intersection_geom')
    result_gdf = result_gdf.set_geometry('geometry')
    return result_gdf

# Function to extract surface area and elevation time series of a prior lake
def get_swot_ts_for_pld(feature_id, start_time, end_time, output='csv'):
    """
    Fetches surface water time series (e.g., water surface elevation and surface area) from NASA's Hydrocron API
    for a given Prior Lake (PLD feature) observed by the SWOT satellite.

    Parameters:
        feature_id (int or str): The unique `lake_id` from the SWOT Prior Lake Database (PLD).
        start_time (str): Start time in ISO format (e.g., '2024-07-15T00:00:00Z').
        end_time (str): End time in ISO format (e.g., '2024-07-26T00:00:00Z').
        output (str): Format of the output from the API. Default is 'csv'.

    Returns:
        pd.DataFrame: Time series data for the specified PLD feature including fields like:
            - lake_id
            - time_str
            - wse (Water Surface Elevation)
            - area_total (Surface Area)
            - quality_f
            - collection_shortname
            - crid
            - PLD_version
            - range_start_time

    Raises:
        Exception: If the request fails or returns an invalid response.

    Example:
        df = get_swot_ts_for_pld(
            feature_id=2320040342,
            start_time='2024-07-15T00:00:00Z',
            end_time='2024-07-26T00:00:00Z'
        )
    """
    feature = 'PriorLake'
    fields = 'lake_id,time_str,wse,area_total,quality_f,collection_shortname,crid,PLD_version,range_start_time'
    log_level2.info(f"Fetching SWOT Prior Lake data for lake id-{feature_id} for the time between {start_time} & {end_time}.")
    try:
        response = requests.get(
            f"https://soto.podaac.earthdatacloud.nasa.gov/hydrocron/v1/timeseries?"
            f"feature={feature}&feature_id={feature_id}&start_time={start_time}&end_time={end_time}"
            f"&fields={fields}&output={output}"
        ).json()

        if not response.get('status'):
            raise Exception(f"No SWOT time series could be extracted for prior lake id-{feature_id} for {start_time}-{end_time}.")
        elif response.get('status').split(' ')[0] == '200':
            result_df = pd.read_csv(StringIO(response['results']['csv']))
            result_df = result_df[result_df['time_str']!='no_data']
            result_df['time'] = pd.to_datetime(result_df['time_str'],errors='coerce')
            return result_df
        elif response.get('status'):
            raise Exception(f"Hydrocron API returned status: {response.get('status')}")
        elif response.get('error'):
            raise Exception(f"Hydrocron API returned error: {response.get('error')}")
        else:
            raise Exception(f"Hydrocron API returned response: {response}")
    
    except Exception as e:
        print(f"SWOT data could not be extracted using Hydrocron API due to the following error:\n{e}")
        return None

# Function to extract surface area & elevation time series for all prior lakes 
def fetch_swot_timeseries_for_reservoir_prior_lakes(
    reservoir_gdf,
    prior_lake_id_field,
    start_time,
    end_time
):
    """
    Fetches and concatenates SWOT time series for all prior lakes matched to each reservoir
    in a long-format DataFrame.

    Parameters:
        reservoir_gdf (GeoDataFrame): Contains mapping of RAT reservoir and their matched prior lake IDs.
        prior_lake_id_field (str): Column name for matched prior SWOT lake ID.
        start_time (str): Start of time range (ISO format).
        end_time (str): End of time range (ISO format).

    Returns:
        pd.DataFrame: Long-format DataFrame with columns ['time', 'lake_id', 'wse', 'area_total'].
    """
    all_records = []

    log_level2.info("Fetching swot tome series for all prior lakes .....")
    for idx, row in reservoir_gdf.iterrows():
        lake_id = row[prior_lake_id_field]

        try:
            df = get_swot_ts_for_pld(feature_id=lake_id, start_time=start_time, end_time=end_time)
            if df is not None and not df.empty:
                df = df[['time', 'wse', 'area_total']].copy()
                df['lake_id'] = lake_id

                # Make 'time' timezone-naive
                df['time'] = pd.to_datetime(df['time']).dt.tz_localize(None)
                df['time'] = df['time'].dt.date

                all_records.append(df)
        except Exception as e:
            print(f"Failed to fetch for lake_id {lake_id}: {e}")
    
    if not all_records:
        print("No time series data fetched.")
        return pd.DataFrame(columns=['time', 'lake_id', 'wse', 'area_total'])

    result_df = pd.concat(all_records, ignore_index=True)
    return result_df[['time', 'lake_id', 'wse', 'area_total']]

# Function to combine swot time series for different prior lake parts of same reservoir
def combine_swot_ts_for_lake_parts(lake_id_gdf, ts_df):
    """
    Combines SWOT time series for multiple prior lake IDs contributing to a single reservoir.

    Parameters:
        lake_id_gdf (GeoDataFrame): Must include 'target_coverage' and 'lake_id' columns assosciated with a single reservoir.
        ts_df (DataFrame): Long-format DataFrame with columns ['time', 'lake_id', 'wse', 'area_total'].

    Returns:
        pd.DataFrame: Combined time series with columns:
                      'time', 'wse', 'area', 'lake_id_list', 'polygon_coverage'
    """
    log_level2.info("Merging data from all prior lakes of the reservoir ....")
    # Merge target_coverage info into ts_df
    merged_df = ts_df.merge(
        lake_id_gdf[['lake_id', 'target_coverage']],
        on='lake_id',
        how='left'
    )

    output_rows = []

    for time, group in merged_df.groupby('time'):
       # Sort so that rows with non-NaN area and wse come first
        group_sorted = group.copy()
        group_sorted['priority'] = (
            group_sorted['area_total'].notna() & group_sorted['wse'].notna()
        ).astype(int)

        # Sort by priority first, then optionally by highest coverage
        group_sorted = group_sorted.sort_values(
            by=['priority', 'target_coverage'],
            ascending=[False, False]
        )

        # Drop duplicates keeping best row per lake_id
        group_unique = group_sorted.drop_duplicates(subset='lake_id', keep='first')

        # Remove helper column
        group_unique = group_unique.drop(columns='priority')

        # Now the rest of your aggregation
        area_sum = 0.0
        wse_weighted_sum = 0.0
        wse_weight_total = 0.0
        contributing_lake_ids = []
        polygon_coverage_sum = 0.0

        for _, row in group_unique.iterrows():
            lake_id = row['lake_id']
            area = row['area_total']
            wse = row['wse']
            coverage = row['target_coverage']

            if pd.notna(area) and area >= 0:
                contributing_lake_ids.append(lake_id)
                area_sum += area

                if pd.notna(coverage):
                    polygon_coverage_sum += coverage

                    if pd.notna(wse) and wse >= 0:
                        wse_weighted_sum += wse * coverage
                        wse_weight_total += coverage

        wse_combined = wse_weighted_sum / wse_weight_total if wse_weight_total > 0 else np.nan

        output_rows.append({
            'time': time,
            'wse': wse_combined,
            'area': area_sum if area_sum > 0 else np.nan,
            'polygon_coverage': polygon_coverage_sum if polygon_coverage_sum > 0 else np.nan,
            'lake_id_list': contributing_lake_ids
        })
            

    return pd.DataFrame(output_rows)

# Function to get swot area and elevation ts for a single reservoir
def get_swot_ts_for_reservoir(
    reservoir_gdf,
    rat_lake_id_field,
    prior_lake_id_field,
    start_time,
    end_time
):
    """
    Fetches and combines SWOT time series for a single RAT reservoir using its associated prior lake parts.

    Parameters:
        reservoir_gdf (GeoDataFrame): A GeoDataFrame representing a single reservoir
                                      and its matched prior lakes with their SWOT lake IDs in multiple rows. 
                                      Only one unique value should be in column rat_lake_id_field.
        rat_lake_id_field (str): Column name identifying the reservoir's unique ID.
        prior_lake_id_field (str): Column name containing the associated prior lake IDs (SWOT lake_ids).
        start_time (str): ISO string indicating the start of the time window.
        end_time (str): ISO string indicating the end of the time window.

    Returns:
        pd.DataFrame: Combined time series with columns:
                      'time', 'wse', 'area', 'lake_id_list', 'polygon_coverage'
    """
    
    # Ensure only one unique reservoir is provided
    if reservoir_gdf[rat_lake_id_field].nunique() != 1:
        raise ValueError(f"reservoir_gdf must contain exactly one unique reservoir ID in '{rat_lake_id_field}'.")

    # Fetch long-format time series (directly returned now)
    ts_df = fetch_swot_timeseries_for_reservoir_prior_lakes(
        reservoir_gdf,
        prior_lake_id_field,
        start_time,
        end_time
    )

    if ts_df.empty:
        log_level2.info("No time series data retrieved for the reservoir.")
        return pd.DataFrame(columns = ['time', 'wse', 'area', 'lake_id_list', 'polygon_coverage'])

    # Prepare lake_id_gdf with only necessary columns
    lake_id_gdf = reservoir_gdf[[prior_lake_id_field, "target_coverage"]].copy()
    lake_id_gdf = lake_id_gdf.rename(columns={prior_lake_id_field: "lake_id"})

    # Combine time series for lake parts
    combined_ts = combine_swot_ts_for_lake_parts(lake_id_gdf, ts_df)

    return combined_ts

# Main Function to get swot area and elevation ts for a single reservoir & save it using hydrocron API.
def hydrocron_ts_swot(
    reservoir_gdf,
    rat_lake_id_field,
    prior_lake_id_field,
    swot_save_dir,
    reservoir_name,
    start_date,
    end_date
):
    """
    Fetches and saves the SWOT surface area (in Km2) and elevation(in m) time series for a single reservoir.

    This function wraps `get_swot_ts_for_reservoir`, handling date conversion, and saving the result as a CSV.

    Parameters:
        reservoir_gdf (GeoDataFrame): GeoDataFrame of a single reservoir and its associated prior SWOT lakes.
        rat_lake_id_field (str): Column name for the unique RAT reservoir ID.
        prior_lake_id_field (str): Column name for associated prior SWOT lake IDs.
        swot_save_dir (str): Base directory where the output CSV will be saved.
        reservoir_name (str): Name of the reservoir; used for folder and file naming.
        start_date (str or datetime): Start date of the time series (can be a string or datetime object).
        end_date (str or datetime): End date of the time series (can be a string or datetime object).

    Returns:
        pd.DataFrame: Combined SWOT time series with columns:
                      ['time', 'wse', 'area', 'lake_id_list', 'polygon_coverage'].
    """
    # Ensure start_date and end_date are datetime objects
    if isinstance(start_date, str):
        start_date = pd.to_datetime(start_date)
    if isinstance(end_date, str):
        end_date = pd.to_datetime(end_date)

    # Convert to ISO string format for get_swot_ts_for_reservoir
    start_time_iso = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time_iso = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")

    log_level2.info(f"Downloading SWOT time series data using Hydrocron API for reservoir {reservoir_name}")
    try:
        # Fetch combined time series
        swot_ts = get_swot_ts_for_reservoir(
            reservoir_gdf,
            rat_lake_id_field,
            prior_lake_id_field,
            start_time_iso,
            end_time_iso
        )

        if swot_ts.empty:
            log_level2.info(f"There are no data points in SWOT time series for reservoir: {reservoir_name}. File is not being saved.")
            # Return empty DataFrame with correct columns
            return None
        else:
            log_level2.info(f"Downloaded SWOT time series data successfully using Hydrocron API for reservoir {reservoir_name}")
        
        # Save CSV
        save_path = os.path.join(swot_save_dir, f"{reservoir_name}.csv")
        if os.path.exists(save_path):
            # Read existing data
            existing_df = pd.read_csv(save_path)
            # Concatenate and remove duplicates on 'time'
            existing_df["time"] = pd.to_datetime(existing_df["time"])
            swot_ts["time"] = pd.to_datetime(swot_ts["time"])
            combined_df = pd.concat([existing_df, swot_ts], ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=["time"], keep="last")
            combined_df = combined_df.sort_values(by="time")
        else:
            combined_df = swot_ts

        # Save final version
        combined_df.to_csv(save_path, index=False)
        log_level2.info(f"Saved SWOT surface area & elevation time series for {reservoir_name} at {save_path}")

        return save_path
    
    except:
        log_level2.exception("SWOT time series could not be downloaded for reservoir: {reservoir_name}.")
        return None
    