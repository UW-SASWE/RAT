from pathlib import Path #to work with relative paths
from scipy import stats
import pandas as pd

## Function to convert a relative path to absolute path
def rel2abs(relative_path: str) -> str:
    '''
    Convert a relative path to an absolute path.

    Parameters:
    - relative_path (str): The relative path to be converted.

    Returns:
    - str: The absolute path.

    Example:
    ```
    relative_path = 'subfolder/file.txt'
    absolute_path = rel2abs(relative_path)
    ```
    '''
    # Get the absolute path
    absolute_path = Path(relative_path).resolve()

    # Convert Path object to string
    return str(absolute_path)

def get_quantile_from_area(filepath, area_value=None):
    """
    Calculate the quantile of a given area value from a time series stored in a CSV file. 
    If the area value is not provided, the function returns the quantile of the last area value in the dataset.

    Parameters:
    - filepath (str): Path to the CSV file containing the time series data. The file should have at least 'date' and 'area' columns.
    - area_value (float, optional): The area value for which to calculate the quantile. 
      If None, the quantile of the last area value in the DataFrame is returned.

    Returns:
    - quantile_value (float): The quantile of the given area value or the last area value in the dataset.
    """

    # Step 1: Read the CSV file
    df = pd.read_csv(filepath)

    # Step 2: Convert 'date' column to datetime format if it's not already
    if not pd.api.types.is_datetime64_any_dtype(df['date']):
        df['date'] = pd.to_datetime(df['date'])

    # Step 3: Filter the data to ensure the time series spans complete years
    df.set_index('date', inplace=True)
    df = df.asfreq('D')  # Ensure daily frequency

    start_year = df.index[0].year
    end_year = df.index[-1].year

    # Adjust start and end to ensure full years
    start_date = f'{start_year}-01-01'
    end_date = f'{end_year}-12-31'

    # Filter the dataframe for complete years
    df_filtered= df[start_date:end_date]

    # Step 4: If area_value is None, use the last value in the 'area' column
    if area_value is None:
        area_value = df['area'].iloc[-1]

    # Step 5: Compute the quantile of the given area value
    area_values = df_filtered['area'].dropna()
    quantile_value = stats.percentileofscore(area_values, area_value) / 100

    return quantile_value