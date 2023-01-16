import argparse
import os

from rat.utils.utils import create_directory

def init_func(args):
    print("Initializing RAT using: ", args)
    
    #### Directory creation
    if args.project_dir is None:
        project_dir = os.path.abspath(input(f"Enter path of RAT project directory: "))
    else:
        project_dir = os.path.abspath(args.project_dir)
    
    try:
        os.mkdir(project_dir)
    except Exception as e:
        print(f"Failed creating RAT project directory: {e}")
        raise e

    # create additional directories
    data_dir = create_directory(os.path.join(project_dir, 'data'))
    models_dir = create_directory(os.path.join(project_dir, 'models'))
    params_dir = create_directory(os.path.join(project_dir, 'params'))

def run_func(args):
    print("Running RAT using: ", args) # TODO: debug line, delete later

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
    
    run_parser.set_defaults(func=run_func)

    args = p.parse_args(['init', '-d', '/mnt/2tb/pritam/rat_test/RAT'])
    args.func(args)


if __name__ == '__main__':
    main()