
import schedule
import time
from loguru import logger
from datetime import datetime
from repo.persist_repo_async import run_with_path
import email_notifier


def print_scheduled_jobs():
    jobs = schedule.get_jobs()
    logger.info(f"Scheduled jobs ({len(jobs)}):")
    for _ in jobs:
        logger.info(f" - {_.__repr__()}")


def job(ofml_repo_path: str):
    now = datetime.now().strftime("%H:%M:%S")
    logger.info(f"execute job at {now}")
    try:
        run_with_path(ofml_repo_path)
        # print("FAKE run_with_path :::", ofml_repo_path)
    except Exception as e:
        message = f"Update from {ofml_repo_path} raised exception: {e}"
        email_notifier.send("ERROR :: Update OFML Database", message)
        logger.info(message)
    else:
        message = f"Update from {ofml_repo_path} was successfully"
        email_notifier.send("SUCCESS :: Update OFML Database", message)
        logger.info(message)
    print_scheduled_jobs()


def seconds_to_hms(seconds):
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return hours, minutes, round(seconds, 0)

def run_loop(time_schedule: str, ofml_repo_path: str):
    schedule.every().day.at(time_schedule, "Europe/Berlin").do(job, ofml_repo_path=ofml_repo_path)
    logger.info(f'The provided time is: "{time_schedule}". Now is {datetime.now().strftime("%H:%M:%S")}.')
    print_scheduled_jobs()
    while True:
        schedule.run_pending()
        idle_seconds = schedule.idle_seconds()
        h, m, s = seconds_to_hms(idle_seconds)
        logger.info(f"sleep until next scheduled job: {idle_seconds} seconds. {h}h {m}m {s}s")
        time.sleep(idle_seconds)
