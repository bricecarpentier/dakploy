from fabric.api import settings, hide, local


def get_current_branch():
    with settings(hide('running')):
        return local('git symbolic-ref HEAD',
                     capture=True).split('/')[-1]
