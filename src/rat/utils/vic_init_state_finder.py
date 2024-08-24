import os
import pandas as pd
import datetime

def get_vic_init_state_date(initial_date, days, data_dir_path, region_name, basin_name):
    """
    Check for the presence of a VIC state file in a directory within a given date range.

    Args:
        initial_date (str or datetime.datetime): The starting date from which to begin the search.
        days (int): The number of days to search backwards from the initial_date.
        data_dir_path (str): The data directory path of RAT.
        region_name (str): The name of the region for which vic_init_state_date is required.
        basin_name (str): The name of the basin for which vic_init_state_date is required.

    Returns:
        datetime.datetime or None: The date as a datetime.datetime object if a file is found,
                                   otherwise None if no file is found within the given days from initial date.
    """
    # Convert the initial_date to a datetime object if it isn't already
    if isinstance(initial_date, str):
        date = pd.to_datetime(initial_date).to_pydatetime()
    else:
        date = initial_date

    # Check for no of days + the initial_date
    for _ in range(days+1):
        # Construct the file path for the current date
        file_path = os.path.join(data_dir_path, region_name, 'basins', basin_name, 'vic', 'vic_init_states', f'state_.{date:%Y%m%d}_00000.nc')
        # print(file_path)
        
        # Check if the file exists at the constructed file path
        if os.path.isfile(file_path):
            return date
        
        # Move to the previous date
        date -= datetime.timedelta(days=1)
    
    # If no file is found in the entire range, return None
    return None