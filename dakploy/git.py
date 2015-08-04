import os.path
from StringIO import StringIO


from fabric.api import *
from fabric.contrib.files import (
    comment,
    exists,
    uncomment,
    upload_template,
)
from fabtools import require


def get_current_branch():
    with settings(hide('running')):
        return local('git symbolic-ref HEAD',
                     capture=True).split('/')[-1]


@task
def setup_push_to_deploy(repository, deploy_script):
    require.git.command()

    if not exists(repository):
        run('mkdir -p {}'.format(repository))

        with cd(repository):
            run('git init --bare')

    post_receive_hook_path = os.path.join(
        repository, 'hooks', 'post-receive')

    if isinstance(deploy_script, StringIO):
        put(deploy_script,
            post_receive_hook_path,
            mode='0755')
    else:
        upload_template(deploy_script,
                        post_receive_hook_path,
                        context=env,
                        mode='0755')


@task
def deactivate_push_to_deploy(repository):
    post_receive_hook_path = os.path.join(
        repository, 'hooks', 'post-receive')
    comment(post_receive_hook_path, r'^.*$')


@task
def activate_push_to_deploy(repository):
    post_receive_hook_path = os.path.join(
        repository, 'hooks', 'post-receive')
    uncomment(post_receive_hook_path, r'^.*$')
