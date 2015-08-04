import os

from fabric.api import (
    env,
    local,
    task)


@task
def manage(args=''):
    python = os.path.join(env.venv_dir, 'bin', 'python')
    manage_py = os.path.join(env.src_dir, 'manage.py')
    local("ssh -t {host} 'cd {src_dir} && {python} {manage} {args}'".format(
        host=env.hosts[0],
        src_dir=env.src_dir,
        python=python,
        manage=manage_py,
        args=args))
