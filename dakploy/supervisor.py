"""
Supervisor helpers
"""
from fabric.api import (
    hide,
    run,
)


def get_unallocated_ports(excluded_programs=None, min=8000, max=8999):
    """
    Generator that yields unused ports
    """

    ports = set(allocated_ports(excluded_programs))

    for port in xrange(min, max + 1):
        if port not in ports:
            yield port

    raise RuntimeError('All ports in range %d-%d are already used')


def allocated_ports(excluded_programs=None):

    if excluded_programs is None:
        excluded_programs = []

    # run_gunicorn old syntax
    options = " ".join("--exclude={}.conf".format(name) for name in excluded_programs)
    cmd = "grep {} gunicorn /etc/supervisor/conf.d/*.conf | cut -d':' -f3 | cut -d' ' -f1".format(options)
    
    with hide('running', 'stdout'):
        res = run(cmd)

    for port in res.split():
        try:
            yield int(port)
        except ValueError:
            pass

    # gunicorn new syntax
    options = " ".join("--exclude={}.conf".format(name) for name in excluded_programs)
    cmd = "grep {} gunicorn /etc/supervisor/conf.d/*.conf | cut -d':' -f4 | cut -d' ' -f1".format(options)

    with hide('running', 'stdout'):
        res = run(cmd)

    for port in res.split():
        try:
            yield int(port)
        except ValueError:
            pass
