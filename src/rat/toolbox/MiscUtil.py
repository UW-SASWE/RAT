from pathlib import Path #to work with relative paths

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