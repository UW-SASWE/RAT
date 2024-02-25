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

    run_log_dir = args.project_dir / "data" / "test_output" / "runs" / "logs"
    run_log_files = list(run_log_dir.glob("RAT_run*.log"))
    assert run_log_dir.exists()
    assert len(run_log_files) == 1 # there should only be one log file
    print("run_log_files", run_log_files)
    if len(run_log_files) == 1:
        print(run_log_files[0].read_text())

    basin_log_dir = args.project_dir / "data" / "test_output" / "colorado" / "logs" / "gunnison"
    basin_log_files = list(basin_log_dir.glob("RAT-gunnison*.log"))
    assert basin_log_dir.exists()
    assert len(basin_log_files) == 1 # there should only be one log file
    print("basin_log_files", basin_log_files)
    if len(basin_log_files) == 1:
        print(basin_log_files[0].read_text())