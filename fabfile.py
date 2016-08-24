# Third-party app imports
from fabric.api import *

env.hosts = [
    '162.209.99.95'
]

env.user = "root"


def update_upgrade():
    """
        Update the default OS installation's
        basic default tools.
    """
    run("sudo apt update")
    run("sudo apt -y upgrade")


def update_server():
    update_upgrade()


def celery_purge():
    with cd("/var/apps/elastic-api-sync"), prefix('source /var/apps/env/bin/activate'):
        with cd("/var/apps/elastic-api-sync"):
            run('python sync.py')


def deploy():
    with cd("/var/apps/elastic-api-sync"), prefix('source /var/apps/env/bin/activate'):
        with cd("/var/apps/elastic-api-sync"):
            run('git pull origin master')
            run('pip install -r requirements.txt')
