from fabric.api import (
    env,
    local,
    task)


@task
def ssh():
    local('ssh {}'.format(env.hosts[0]))
