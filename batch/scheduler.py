import time

import schedule

import hourly_report
import large_trades
import trading_patterns
from common import setup_logging

log = setup_logging("scheduler")


def safe(fn, name):
    def wrap():
        try:
            log.info("running %s", name)
            fn()
        except Exception:
            log.exception("%s failed", name)
    return wrap


def main() -> None:
    job_hourly   = safe(hourly_report.run,    "hourly_report")
    job_patterns = safe(trading_patterns.run, "trading_patterns")
    job_impact   = safe(large_trades.run,     "large_trades")

    schedule.every(5).minutes.do(job_hourly)
    schedule.every().hour.do(job_patterns)
    schedule.every(30).minutes.do(job_impact)

    job_hourly()
    job_patterns()
    job_impact()

    log.info("scheduler started; pending=%d", len(schedule.jobs))
    while True:
        schedule.run_pending()
        time.sleep(15)


if __name__ == "__main__":
    main()
