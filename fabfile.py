# Third-party app imports
from fabric.api import *

env.hosts = [
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
    with cd("/var/apps/datastore-elastic-api-sync"), prefix('source /var/apps/env/bin/activate'):
        with cd("/var/apps/datastore-elastic-api-sync"):
            run('python sync.py')


def deploy():
    with cd("/var/apps/datastore-elastic-api-sync"), prefix('source /var/apps/env/bin/activate'):
        with cd("/var/apps/datastore-elastic-api-sync"):
            run('git pull origin master')
            run('pip install -r requirements.txt')
