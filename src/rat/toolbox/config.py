import ruamel_yaml as ryaml
from pathlib import Path

#Function to update configuration file given a parameters dictionary
def update_config(file_path, update_params):
    """
    Update RAT's existing configuration file in YAML format .

    Parameters:
    - file_path (str): The path to the configuration file.
    - update_params (dict): A nested dictionary containing the parameters to update.

    Example:
    update_params = {'GLOBAL': {'steps': [1,2], 'multiprocessing': 4},
                     'BASIN': {'basin_name': 'ganga'}}
    update_config('path/to/config.yml', update_params)
    """
    try:
        # Reading config with comments
        config_file_path = Path(file_path).resolve()
        ryaml_client = ryaml.YAML()
        config = ryaml_client.load(config_file_path.read_text())
    except FileNotFoundError:
        raise FileNotFoundError(f"The file '{file_path}' does not exist.")
    
    # Update the configuration with the new parameters
    for section, params in update_params.items():
        # If a section is not already present, add section with one line space at the end
        if section not in config:
            # Add a CommentedMap with one line space from the previous section
            prev_section = list(config.keys())[-1] if config else None
            config[section] = ryaml.comments.CommentedMap()
            if prev_section:
                config.yaml_set_comment_before_after_key(
                    section, before='\n', after=''
                )
        # Updating keys in each section
        for key, value in params.items():
            config[section][key] = value

    # Write the updated configuration back to the file
    ryaml_client.dump(config, config_file_path.open('w'))