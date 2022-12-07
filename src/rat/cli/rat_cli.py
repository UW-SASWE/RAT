import argparse

from rat.run_rat import run_rat


def main():
    parser = argparse.ArgumentParser(description='Reservoir Assessment Tool 2.1')

    parser.add_argument('input', action='store', nargs=1)
    
    args = parser.parse_args()

    print(args.input)


if __name__ == '__main__':
    main()