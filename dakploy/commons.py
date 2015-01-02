import posixpath

from StringIO import StringIO

from contextlib import contextmanager

from fabric.api import *
from fabric.contrib.files import exists, upload_template

from fabtools import require


@task
def setup_push_to_deploy(repository, deploy_script):
    require.git.command()

    if not exists(repository):
        run('mkdir -p {}'.format(repository))

        with cd(repository):
            run('git init --bare')

    post_receive_hook_path = posixpath.join(
        repository, 'hooks', 'post-receive')

    upload_template(deploy_script,
                    post_receive_hook_path,
                    context=env,
                    mode='0755')


@task
def generate_conf(filename, **kwargs):
    content = "\n".join(["export {}={}".format(key, value) for key, value in kwargs.iteritems()])
    put(StringIO(content), filename)


@contextmanager
def source(filename):
    with prefix(". {}".format(filename)):
        yield
