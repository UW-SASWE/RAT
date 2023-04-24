# Development guide

## Installing RAT in development mode

1. First create project directory for ease-of-storage of directories.
```
mkdir rat3_project
cd rat3_project

mkdir models
mkdir data
mkdir params
mkdir secrets
```
2. We will use the `rat3_project` as the project directory for running RAT. We will use `models` to store the models vic, metsim, vic-routing and RAT. `data`, `params` and `secrets` directories would be used to store various sorts of data, parameter files and secrets repectively.

Next, we will clone RAT from github into the `models` directory.
```
git clone https://github.com/UW-SASWE/RAT.git models/
```
3. Create conda environment.
```
conda create --prefix ./.env

conda activate ./.env
mamba env update --file models/RAT/environment.yml
mamba install conda-build
conda develop models/RAT/src/
```
4. Now you will be able to `import rat` and its components in the `./.env` environment. Any changes you make in the RAT's source code in models/RAT/src/ directory will be imported. While installing rat as a conda package gives access to the cli command `rat` allowing interface to rat. The same behavior can be imitated in a development install by calling passing the arguments to the script `models/RAT/src/rat/cli/rat_cli.py`. Let's use this interface to initialize RAT. RAT requires some secrets to be able to use various functionalities, which have to be stored in the `secrets/secrets.ini` file. Read more about it [](here).
```
conda activate ./.env
python models/RAT/src/rat/cli/rat_cli.py init -d . -g -s secrets/secrets.ini
```
5. RAT has not been initialized (hydrological models set up, global data downloaded and the parameter file has been populated). Follow instructions [](here) to complete the rest of the parameter file so that RAT can be run. To run RAT using the parameter file,

## Getting RAT from github

1. Create a conda environment having `conda-build` package where RAT will be installed.
```
conda create -n rat_dev conda-build;
```

2.  Clone the `RAT` project into a directory and navigate into it. 
```
git clone https://github.com/UW-SASWE/RAT.git;
cd RAT
``` 
This will clone the RAT project in the directory.

3. Install `RAT` requirements into the `rat_dev` environment, optionally using mamba (recommended). 
```
conda activate rat_dev;
conda install mamba; 
mamba env update --file 'environment.yml'
``` 
This will install the requirements of `RAT` in the environment.

4. Now "install" `RAT` in development mode.
```
 conda develop src
``` 
You should see a message that states that the src was added to path. 

    !!! tip "Congratulations!"
        `RAT` is now installed! You should be able to import it using `import rat` in python.

5. `RAT` uses a Command Line Interface (CLI) to perform various tasks such as initialization and run. The CLI functionality gets automatically activated when it is installed via a conda command (`conda install rat`), but it doesn't automatically get registered when installing in the "development" mode. To use the CLI functionality, you can directly call the python script that handles the CLI and pass along any of the arguments. For instance, to initialize rat (which would be `rat init` in a normal install), you would write `python src/cli/rat_cli.py init`.

