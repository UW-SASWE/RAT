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
    
    if args.global_data is True:
        global_data = "Y"
    else:
        global_data = str(input(f"Do you want to download global data? (y/N) : ")).capitalize()
    global_data_dir = project_dir.joinpath("global_data")

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
    cmd = f"conda create -p {metsim_path} -c conda-forge metsim -y".split(" ")
    print(f"Installing Metsim: {' '.join(cmd)}")
    subprocess.run(cmd)
    
    # install vic
    vic_path = models_dir.joinpath('vic')
    cmd = f"conda create -p {vic_path} -c conda-forge vic -y".split(" ")
    print(f"Installing VIC: {' '.join(cmd)}")
    subprocess.run(cmd)
    
    # import download links
    from rat.cli.rat_init_config import DOWNLOAD_LINKS

    # install route
    print(f"Downloading source code of routing model")
    route_model_src_dl_path = DOWNLOAD_LINKS["route_model"]
    r = requests.get(route_model_src_dl_path)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(models_dir)
    route_model = models_dir.joinpath("routing")
    cmd = f"make"
    print(f"Installing VIC-Route using make in directory: {route_model}")
    subprocess.run(cmd, cwd=route_model)

    #### download params
    params_template_dl_path = DOWNLOAD_LINKS["params"]
    r = requests.get(params_template_dl_path)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(project_dir)

    #### download global data
    global_data_dl_path = DOWNLOAD_LINKS["global_data"]
    global_vic_params_dl_path = DOWNLOAD_LINKS["global_vic_params"]
    
    if global_data == 'Y':
        r = requests.get(global_data_dl_path)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(project_dir)  # saved as the `global_data` value naturally

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

    ## cleanup
    # delete all __MACOSX folders
    [shutil.rmtree(junk_dir) for junk_dir in project_dir.glob("**/__MACOSX/")]

    # determine how many cores are available
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
        global_data_downloaded=(global_data == 'Y'),
        n_cores=n_cores,
        secrets=secrets_fp
    )

def test_func(args):
    from rat.cli.rat_test_config import DOWNLOAD_LINK, PATHS, PARAMS

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

    test_data_link = DOWNLOAD_LINK['test_data']
    r = requests.get(test_data_link)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(data_dir)

    n_cores = 4
    try:
        from multiprocessing import cpu_count
        n_cores = cpu_count()
    except Exception as e:
        print(f"Failed to determine number of cores: {e}")

    update_param_file(
        project_dir=project_dir, 
        config_path=test_param_fp,
        global_data_downloaded=False, 
        n_cores=n_cores,
        secrets=secrets_fp,
        any_other_suffixes=PATHS[test_basin],
        any_other_args=PARAMS[test_basin]
    )

    run_args = argparse.Namespace()
    run_args.param = test_param_fp
    run_args.operational_latency = None
    run_func(run_args)


def update_param_file(
        project_dir: Path,
        config_path: Path = None,
        global_data_downloaded: bool = False,   # if global data was downloaded, then we can use the global_data dir
        n_cores: int = None,
        secrets: Path = None,
        any_other_suffixes: dict = None,        # any other suffixes that need to be added to the config file
        any_other_args: dict = None             # any other args that need to be added to the config file
    ):
    """Creates RAT config file with project specific information.

    Args:
        project_dir (Path): Directory of the rat project.
        config_path (Path, optional): Where to save the configuration path? Defaults to `project_dir/params/rat_config.yml`.
        global_data_downloaded (bool, optional): Do global datasets need to be downloaded to initialize rat? . Defaults to False.
        n_cores (int, optional): Number of cores to use. Defaults to None.
        secrets (Path, optional): Path of the `secrets.ini` file. Please refer to documentation for more details. Defaults to None.
        any_other_suffixes (dict, optional): _description_. Defaults to None.
    """

    config_template_path = (project_dir / 'params' / 'rat_config_template.yml')

    if config_path is None:
        config_path = (project_dir / 'params' / 'rat_config.yml')

    ryaml_client = ryaml.YAML()
    config_template = ryaml_client.load(config_template_path.read_text())
    
    # read suffixes file
    from rat.cli.rat_init_config import SUFFIXES_GLOBAL, SUFFIXES_NOTGLOBAL

    # if global data was downloaded, update the suffixes of the config file's contents by prepending the project dir path
    if global_data_downloaded:
        for k1, v1 in SUFFIXES_GLOBAL.items():
            for k2, v2 in v1.items():
                config_template[k1][k2] = str(project_dir.joinpath(v2))
    else:
        for k1, v1 in SUFFIXES_NOTGLOBAL.items():
            for k2, v2 in v1.items():
                config_template[k1][k2] = str(project_dir.joinpath(v2))

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
        help='Flag to download global data will be downloaded',
        action='store_true',
        dest='global_data',
        required=False,
        default=False
    )
    init_parser.add_argument(
        '-s', '--secrets', 
        help='Specify the path of secrets.ini file', 
        action='store',
        dest='secrets',
        required=False
    )

    init_parser.set_defaults(func=init_func)

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