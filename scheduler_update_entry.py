import os.path
import argparse
import re
from datetime import datetime
from repo.persist_repo_async import run_with_path
from scheduler_update_loop import run_loop


def is_valid_time_format(value):
    # Regular expression to validate time format "HH:MM"
    pattern = re.compile(r'^([01]\d|2[0-3]):([0-5]\d)$')
    return bool(pattern.match(value))


def is_valid_ofml_repo_path(value):
    return os.path.exists(value)


parser = argparse.ArgumentParser(
    description="""
    A script that schedules the task og updating repository to
    db once every day at the given time value in the format "HH:MM".
    """,
    usage="python scheduler_db_update.py time")
parser.add_argument('time', type=str, help='Time value in the format "HH:MM"')
parser.add_argument('ofml_repo_path', type=str, help='Existing path to a ofml repo')
args = parser.parse_args()


if not is_valid_time_format(args.time):
    print("Error: Invalid time format. Please use \"HH:MM\".")
    exit(1)


if not is_valid_ofml_repo_path(args.ofml_repo_path):
    print(f"Error: ofml_repo_path \"{args.ofml_repo_path}\" doesnt exist.")
    exit(1)

run_loop(time_schedule=args.time, ofml_repo_path=args.ofml_repo_path)