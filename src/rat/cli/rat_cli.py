import argparse
import os
from pathlib import Path
import subprocess
import shutil
import requests, zipfile, io
import yaml
import ruamel_yaml as ryaml


def init_func(args):
    print("Initializing RAT using: ", args)

    #### Directory creation
    print("Creating directories ...")
    ## Resolving Project Directory
    if args.project_dir is None:
        project_dir_input = input(f"Enter path of RAT project directory: ")
        project_dir = Path(project_dir_input).resolve()
    else:
        project_dir = Path(args.project_dir).resolve()
    
    try:
        project_dir.mkdir(exist_ok=True)
    except Exception as e:
        print(f"Failed creating RAT project directory: {e}")
        raise e
    
    ## Resolving Globala Data Directory and its flag to download
    # if downloading global data 
    if args.global_data is True :
        global_data = "Y"
        # and parent directory is specified
        if args.global_data_dir is not None:
            global_data_parent_dir = Path(args.global_data_dir).resolve()
        # and parent directory is not specified
        else:
            global_data_parent_dir = project_dir
        # gtting global_data directory
        global_data_dir = global_data_parent_dir.joinpath('global_data')
    # if not downloading global data 
    else:
        # and global_data directory is specified
        if args.global_data_dir is not None:
            global_data_dir = Path(args.global_data_dir).resolve()
        # and global_data directory is not specified
        else:
            global_data_dir = None


    secrets_fp = None
    if args.secrets is not None:
        secrets_fp = Path(args.secrets).resolve()
        assert secrets_fp.exists(), f"Secrets file {secrets_fp} does not exist"
        

    # create additional directories
    data_dir = project_dir.joinpath('data')
    data_dir.mkdir(exist_ok=True)
    models_dir = project_dir.joinpath('models')
    models_dir.mkdir(exist_ok=True)
    params_dir = project_dir.joinpath('params')
    params_dir.mkdir(exist_ok=True)
    rat_config_fp = params_dir.joinpath('rat_config.yaml')

    #### Model installation
    # install metsim
    metsim_path = models_dir.joinpath('metsim')
    cmd = f"conda create -p {metsim_path} -c conda-forge metsim pandas=1.5 -y".split(" ")
    print(f"Installing Metsim: {' '.join(cmd)}")
    try:
        subprocess.run(cmd)
    except:
        print("Failed to install Metsim.")
    
    # install vic
    vic_path = models_dir.joinpath('vic')
    cmd = f"conda create -p {vic_path} -c conda-forge vic -y".split(" ")
    print(f"Installing VIC: {' '.join(cmd)}")
    try:
        subprocess.run(cmd)
    except:
        print("Failed to install VIC.")
    
    # import download links
    from rat.cli.rat_init_config import DOWNLOAD_LINKS

    # download route
    print(f"Downloading source code of routing model...")
    try:
        route_model_src_dl_path = DOWNLOAD_LINKS["route_model"]
        r = requests.get(route_model_src_dl_path)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(models_dir)
        route_model = models_dir.joinpath("routing")
    except:
        print("Failed to download routing model.")
    
    # install route
    cmd = f"make"
    print(f"Installing VIC-Route using make in directory: {route_model}")
    try:
        subprocess.run(cmd, cwd=route_model)
    except:
        print("Failed to install routing model.")

    #### download params
    print("Downloading parameter files for RAT...")
    try:
        params_template_dl_path = DOWNLOAD_LINKS["params"]
        r = requests.get(params_template_dl_path)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(project_dir)
    except:
        print("Failed to download parameter files for RAT.")

    #### download global data
    print("Downloading global database for RAT...")
    try:
        global_data_dl_path = DOWNLOAD_LINKS["global_data"]
        global_vic_params_dl_path = DOWNLOAD_LINKS["global_vic_params"]
        
        if global_data == 'Y':
            r = requests.get(global_data_dl_path)
            z = zipfile.ZipFile(io.BytesIO(r.content))
            z.extractall(global_data_parent_dir)  # saved as the `global_data` value naturally in project dir or in a user specified directory

            # extract inner zips
            [zipfile.ZipFile(inner_z, 'r').extractall(global_data_dir) for inner_z in global_data_dir.glob("*.zip")]

            # download and extract vic params in the global data dir
            r = requests.get(global_vic_params_dl_path)
            z = zipfile.ZipFile(io.BytesIO(r.content))
            z.extractall(global_data_dir)

            # extract inner inner zips (vic params)
            [zipfile.ZipFile(inner_z, 'r').extractall(global_data_dir.joinpath('global_vic_params')) for inner_z in global_data_dir.joinpath('global_vic_params').glob("*.zip")]

            # cleanup
            [p.unlink() for p in global_data_dir.glob("**/*.zip")]
    except:
        print("Failed to download global database for RAT.")

    ## cleanup
    # delete all __MACOSX folders
    [shutil.rmtree(junk_dir) for junk_dir in project_dir.glob("**/__MACOSX/")]

    # determine how many cores are available
    print("Determining number of cores...")
    n_cores = None
    try:
        from multiprocessing import cpu_count
        n_cores = cpu_count()
    except Exception as e:
        print(f"Failed to determine number of cores: {e}")
        print("Leaving GLOBAL:Multiprocessing empty in config file. Please update manually if needed.")

    #### update params
    update_param_file(
        project_dir,
        config_path=rat_config_fp,
        global_data_dir=global_data_dir,
        n_cores=n_cores,
        secrets=secrets_fp
    )

def test_func(args):
    from rat.cli.rat_test_config import DOWNLOAD_LINK, PATHS, PARAMS, TEST_PATHS
    from rat.cli.rat_test_verify import Verify_Tests

    test_basin_options = PARAMS.keys()
    test_basin = args.test_basin
    assert test_basin in test_basin_options, f"Please specify the correct test basin. Acceptable values are {test_basin_options})"

    project_dir = Path(args.project_dir).resolve()
    assert project_dir.exists(), f"{project_dir} does not exist - please pass a valid rat project directory after initialization using the `rat init` command."

    test_param_fp = project_dir / 'params' / 'test_config.yml'
    
    secrets_fp = Path(args.secrets).resolve()
    assert secrets_fp.exists(), f"{secrets_fp} does not exist - rat requires secrets.ini file to be passed. Please refer to documentation for more details."

    # Download test data
    data_dir = project_dir / 'data'
    assert data_dir.exists(), f"{data_dir} does not exist - rat has not been initialized properly."

    print("Downloading test data.")
    test_data_link = DOWNLOAD_LINK['test_data']
    r = requests.get(test_data_link)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(data_dir)
    print("Test data downloaded successfully.")

    n_cores = 4
    try:
        from multiprocessing import cpu_count
        n_cores = cpu_count()
    except Exception as e:
        print(f"Failed to determine number of cores: {e}")
    
    print("Preparing test configuration file.")
    # Updating Test Config file
    update_param_file(
        project_dir=project_dir, 
        config_path=test_param_fp,
        n_cores=n_cores,
        secrets=secrets_fp,
        any_other_suffixes=PATHS[test_basin],
        any_other_args=PARAMS[test_basin]
    )
    print("Test configuration file prepared successfully.")

    print("Running RAT using test config file ....")
    # Running RAT with test config file
    run_args = argparse.Namespace()
    run_args.param = test_param_fp
    run_args.operational_latency = None
    run_func(run_args);

    # Verifying RAT run results with true test results 
    print("Verifying Test Results for basin - "+test_basin+" ....")
    expected_output_dir = Path(project_dir,TEST_PATHS[test_basin]['expected_outputs'])
    rat_produced_dir = Path(project_dir,TEST_PATHS[test_basin]['rat_produced_outputs'])
    verify_rat_test_run = Verify_Tests(expected_output_dir,rat_produced_dir)
    verify_rat_test_run.verify_test_results()

def configure_func(args):
    # Resolving parameters
    project_dir = Path(args.project_dir).resolve()
    assert project_dir.exists(), f"{project_dir} does not exist - please pass a valid rat project directory after initialization using the `rat init` command."

    params_fp = Path(args.param).resolve()
    assert params_fp.is_file(), f"{params_fp} does not exist - please pass a valid rat parameter file from 'params' in project directory."

    if args.global_data_dir is not None:
        global_data_dir = Path(args.global_data_dir).resolve()
        assert global_data_dir.exists(), f"Global database {global_data_dir} does not exist"

    if args.n_cores is not None:
        n_cores = int(args.n_cores)

    secrets_fp = None
    if args.secrets is not None:
        secrets_fp = Path(args.secrets).resolve()
        assert secrets_fp.exists(), f"Secrets file {secrets_fp} does not exist"

    #Updating parameter file
    update_param_file(
        project_dir=project_dir,
        config_template_path = params_fp,
        config_path=params_fp,
        global_data_dir=global_data_dir,
        n_cores=n_cores,
        secrets=secrets_fp,
    )


def update_param_file(
        project_dir: Path,
        config_template_path = None,
        config_path: Path = None,
        global_data_dir: Path = None,    # if global data was downloaded or global_data_dir is specified by user, then we can use the global_data dir
        n_cores: int = None,
        secrets: Path = None,
        any_other_suffixes: dict = None,        # any other suffixes that need to be added to the config file
        any_other_args: dict = None             # any other args that need to be added to the config file
    ):
    """Creates RAT config file with project specific information.

    Args:
        project_dir (Path): Directory of the rat project.
        config_template_path (Path, optional): Path of the configuration file to use as template. Defaults to `project_dir/params/rat_config_template.yml`.
        config_path (Path, optional): Where to save the configuration path? Defaults to `project_dir/params/rat_config.yml`.
        global_data_dir (Path, optional): Path of the RAT global database.
        n_cores (int, optional): Number of cores to use. Defaults to None. Please refer to documentation for more details. Defaults to None.
        secrets (Path, optional): Path of the `secrets.ini` file. Please refer to documentation for more details. Defaults to None.
        any_other_suffixes (dict, optional): _description_. Defaults to None.
    """

    if config_template_path is None:
        config_template_path = (project_dir / 'params' / 'rat_config_template.yml')

    if config_path is None:
        config_path = (project_dir / 'params' / 'rat_config.yml')

    ryaml_client = ryaml.YAML()
    config_template = ryaml_client.load(config_template_path.read_text())
    
    # read suffixes file
    from rat.cli.rat_init_config import SUFFIXES_GLOBAL, SUFFIXES_NOTGLOBAL

    for k1, v1 in SUFFIXES_NOTGLOBAL.items():
            for k2, v2 in v1.items():
                config_template[k1][k2] = str(project_dir.joinpath(v2))

    # if global data was downloaded, update the suffixes of the config file's contents by prepending the project dir path
    if global_data_dir is not None:
        for k1, v1 in SUFFIXES_GLOBAL.items():
            for k2, v2 in v1.items():
                config_template[k1][k2] = str(global_data_dir.joinpath(v2))

    if any_other_args is not None:
        for k1, v1 in any_other_args.items():
            for k2, v2 in v1.items():
                config_template[k1][k2] = v2
    
    if any_other_suffixes is not None:
        for k1, v1 in any_other_suffixes.items():
            for k2, v2 in v1.items():
                config_template[k1][k2] = str(project_dir.joinpath(v2))

    # update the number of cores
    config_template['GLOBAL']['multiprocessing'] = n_cores

    # if secrets were provided, update the config file
    if secrets is not None:
        config_template['CONFIDENTIAL']['secrets'] = str(secrets)

    ryaml_client.dump(config_template, config_path.open('w'))


def run_func(args):
    from rat.run_rat import run_rat
    run_rat(args.param, args.operational_latency)

def main():
    ## CLI interface
    p = argparse.ArgumentParser(description='Reservoir Assessment Tool')

    # Treat the different commands, such as `init`, and `run` as different sub parsers
    command_parsers = p.add_subparsers()

    # Init command
    init_parser = command_parsers.add_parser('init', help='Initialize RAT')

    # options for init parser
    init_parser.add_argument(
        '-d', '--dir', 
        help='Specify RAT project directory', 
        action='store',
        dest='project_dir',
        required=False
    )
    init_parser.add_argument(
        '-g', '--global_data', 
        help='Flag if provided, global data will be downloaded',
        action='store_true',
        dest='global_data',
        required=False,
        default=False
    )
    init_parser.add_argument(
        '-gp', '--global_data_dir', 
        help='Specify directory path, of global data or to download global data',
        action='store',
        dest='global_data_dir',
        required=False
    )
    init_parser.add_argument(
        '-s', '--secrets', 
        help='Specify the path of secrets.ini file', 
        action='store',
        dest='secrets',
        required=False
    )

    init_parser.set_defaults(func=init_func)

    # Configure command - prepares configuration file
    configure_parser = command_parsers.add_parser('configure', help='Prepares RAT configuretion file')
    # options for configure parser
    configure_parser.add_argument(
        '-d', '--dir',
        help='Specify RAT project directory (where rat init was run)', 
        action='store',
        dest='project_dir',
        required=True
    )
    configure_parser.add_argument(
        '-p', '--param',
        help='RAT Parameter file',
        action='store',
        dest='param',
        required=True
    )
    configure_parser.add_argument(
        '-gp', '--global_data_dir', 
        help='Specify directory path of global data',
        action='store',
        dest='global_data_dir',
        required=False
    )
    run_parser.add_argument(
        '-nc', '--n_cores',
        help='Number of cores to use for executing RAT',
        action='store',
        dest='n_cores',
        required=False
    )
    configure_parser.add_argument(
        '-s', '--secrets', 
        help='Specify the path of secrets.ini file', 
        action='store',
        dest='secrets',
        required=False
    )
    
    configure_parser.set_defaults(func=configure_func)

    # Run command
    run_parser = command_parsers.add_parser('run', help='Run RAT')

    run_parser.add_argument(
        '-p', '--param',
        help='RAT Parameter file',
        action='store',
        dest='param',
        required=True
    )
    run_parser.add_argument(
        '-o', '--operational',
        help='RAT Operational Latency in days',
        action='store',
        dest='operational_latency',
        required=False
    )
    
    run_parser.set_defaults(func=run_func)
    
    # Test command
    test_parser = command_parsers.add_parser('test', help='Test RAT')

    test_parser.add_argument(
        '-b', '--basin', 
        help='Specify name of test basin', 
        action='store',
        dest='test_basin',
        required=True
    )

    test_parser.add_argument(
        '-d', '--dir', 
        help='Specify RAT project directory (where rat init was run)', 
        action='store',
        dest='project_dir',
        required=True
    )
    
    test_parser.add_argument(
        '-s', '--secrets', 
        help='Specify the path of secrets.ini file', 
        action='store',
        dest='secrets',
        required=True
    )
    
    test_parser.set_defaults(func=test_func)

    args = p.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()