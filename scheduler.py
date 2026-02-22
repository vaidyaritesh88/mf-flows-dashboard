"""
Monthly scheduler — runs the pipeline automatically around the 12th of each month
(AMFI typically publishes monthly AUM data by the 10th).

Run this as a persistent background process:
    python scheduler.py

Or use cron:
    0 9 12 * * /path/to/venv/bin/python /path/to/mf_flows/scheduler.py
"""

import logging
from datetime import date
from dateutil.relativedelta import relativedelta
from apscheduler.schedulers.blocking import BlockingScheduler
import pipeline as pl

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def monthly_job():
    """
    Runs on the 12th of each month at 9:30 AM IST.
    Processes the previous month's data (which AMFI would have published by now).
    """
    today = date.today()
    target = date(today.year, today.month, 1) - relativedelta(months=1)
    log.info("Scheduler triggered: processing %s/%s", target.month, target.year)
    try:
        pl.compute_flows_for_month(target.year, target.month)
        log.info("Pipeline completed successfully.")
    except Exception as e:
        log.error("Pipeline failed: %s", e, exc_info=True)


if __name__ == "__main__":
    pl.init_db()
    scheduler = BlockingScheduler(timezone="Asia/Kolkata")

    # Run on 12th of every month at 09:30 AM IST
    scheduler.add_job(monthly_job, "cron", day=12, hour=9, minute=30)

    log.info("Scheduler started. Will run on 12th of each month at 09:30 IST.")
    log.info("Press Ctrl+C to exit.")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopped.")
