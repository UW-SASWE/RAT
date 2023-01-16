import argparse


def init_func(args):
    print("Initializing RAT using: ", args) # TODO: debug line, delete later

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

    print(p.parse_args(['init', '-d', 'test_directory']))
    print(p.parse_args(['run', '-p', 'test_param']))


if __name__ == '__main__':
    main()