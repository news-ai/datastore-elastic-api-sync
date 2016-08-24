# Stdlib imports
from subprocess import call

# Third-party app imports
from apscheduler.schedulers.blocking import BlockingScheduler


def some_job():
    print call(["bash", "/var/apps/elastic-api-sync/scripts/run_reindex.sh"])

scheduler = BlockingScheduler()
scheduler.add_job(some_job, 'interval', hours=6)
scheduler.start()
