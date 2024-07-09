# Guide to install RAT's Dev Version

1. First create project directory structure.
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
git clone https://github.com/UW-SASWE/RAT.git models/RAT
```

3. Create the conda environment, and install RAT using conda develop.
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

5. RAT has not been initialized (hydrological models set up, global data downloaded and the parameter file has been populated). Follow instructions [](here) to complete the rest of the parameter file so that RAT can be run. To run RAT using the parameter file `rat_config.yaml`
```
conda activate ./.env
python models/RAT/src/rat/cli/rat_cli.py run -p params/rat_config.yaml
```
