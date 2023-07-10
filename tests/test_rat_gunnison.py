import rat
from rat.cli.rat_cli import init_func
from rat.cli.rat_cli import test_func as rat_t_func
from argparse import Namespace
from pathlib import Path
import pytest

def assert_directory_structure(project_dir):
    assert project_dir.exists()
    assert (project_dir / "data").exists()
    assert (project_dir / "models").exists()

def test_rat_gunnison():
    args = Namespace()
    args.project_dir = Path.home() / "rat_test"
    args.global_data = False
    args.global_data_dir = None
    args.drive = None
    args.secrets = "GA"

    init_func(args)

    # Test that the project directory was created
    assert_directory_structure(args.project_dir)

    # run rat for gunnision
    args = Namespace()
    args.project_dir = Path.home() / "rat_test"
    args.test_basin = "GUNNISON"
    args.secrets = "GA"
    args.drive = None

    rat_t_func(args)