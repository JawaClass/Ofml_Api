import os.path
import schedule
import time
from repo.persist_repo_async import run_with_path
import argparse
import re
import email_notifier
from loguru import logger
from datetime import datetime


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


def print_scheduled_jobs():
    jobs = schedule.get_jobs()
    logger.info(f"Scheduled jobs ({len(jobs)}):")
    for _ in jobs:
        logger.info(f" - {_.__repr__()}")


def job():
    now = datetime.now().strftime("%H:%M:%S")
    logger.info(f"execute job at {now}")
    try:
        run_with_path(args.ofml_repo_path)
    except Exception as e:
        message = f"Update from {args.ofml_repo_path} raised exception: {e}"
        email_notifier.send("ERROR :: Update OFML Database", message)
        logger.info(message)
    else:
        message = f"Update from {args.ofml_repo_path} was successfully"
        email_notifier.send("SUCCESS :: Update OFML Database", message)
        logger.info(message)
    print_scheduled_jobs()


schedule.every().day.at(args.time, "Europe/Berlin").do(job)
logger.info(f'The provided time is: "{args.time}". Now is {datetime.now().strftime("%H:%M:%S")}.')
print_scheduled_jobs()
while True:
    schedule.run_pending()
    idle_seconds = schedule.idle_seconds()
    logger.info(f"sleep until next scheduled job: {idle_seconds} seconds")
    time.sleep(idle_seconds)
