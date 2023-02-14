# Development guide

## Getting RAT from github

1. Create a conda environment where RAT will be installed - `conda create -n rat_dev`

2. Clone the `RAT` project into a directory and navigate into it - `git clone https://github.com/UW-SASWE/RAT.git; cd RAT`. This will clone the RAT project in the directory.

3. Install `RAT` requirements into the `rat_dev` environment, optionally using mamba (recommended). `conda activate rat_dev; conda install mamba; mamba env update --file environment.yml`. This will install the requirements of `RAT` in the environment.

4. Now "install" `RAT` in development mode - `conda develop src/`. You should see a message that states that the src was added to path. 

Congratulations, `RAT` is now installed! You should be able to import it using `import rat`.

5. `RAT` uses a Command Line Interface (CLI) to perform various tasks such as initialization and run. The CLI functionality gets automatically activated when it is installed via a conda command (`conda install rat`), but it doesn't automatically get registered when installing in the "development" mode. To use the CLI functionality, you can directly call the python script that handles the CLI and pass along any of the arguments. For instance, to initialize rat (which would be `rat init` in a normal install), you would write `python src/cli/rat_cli.py init`.