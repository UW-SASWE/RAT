import os
import argparse
import re
import datetime
import yaml


def main():
    config_path = "/houston2/pritam/rat_mekong_v3/backend/params/rat_mekong.yml"

    config = yaml.safe_load(open(config_path, 'r'))

    lag_of_days = 3

    previous_end = (config['GLOBAL']['end'] - datetime.timedelta(days=lag_of_days)).strftime('%Y-%m-%d')
    new_end = (datetime.datetime.now() - datetime.timedelta(days=lag_of_days)).strftime('%Y-%m-%d')


    lines = []
    with open(config_path, 'r') as f:
        lines = f.readlines()
    
    resulting_lines = []
    for line in lines:
        if line.startswith('  end:'):
            line = f"  end: {new_end}\n"
        if line.startswith('  previous_end:'):
            line = f"  previous_end: {previous_end}\n"
        resulting_lines.append(line)
    
    with open(config_path, 'w') as dst:
        dst.writelines(resulting_lines)


if __name__ == '__main__':
    main()