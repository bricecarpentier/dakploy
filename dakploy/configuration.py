from StringIO import StringIO
from fabric.api import *

import json

CONFIGURATION_FILE_NAME = '.environment'


def fetch_configuration():
    configuration = {}
    with settings(warn_only=True):
        fd = StringIO()
        result = get(CONFIGURATION_FILE_NAME, fd)
        if result.succeeded:
            configuration = json.loads(fd.getvalue())

    return configuration


def put_configuration(configuration):
    fd = StringIO(json.dumps(configuration))
    put(fd, CONFIGURATION_FILE_NAME)


@task
def list_vars():
    configuration = fetch_configuration()
    for key, value in iter(configuration.items()):
        print "{}={}".format(key, value)


@task
def set_var(name, value):
    configuration = fetch_configuration()
    configuration[name] = value
    put_configuration(configuration)


@task
def get_var(name):
    configuration = fetch_configuration()
    return configuration.get(name, '')


@task
def set_vars(**kwargs):
    configuration = fetch_configuration()
    configuration.update(kwargs)
    put_configuration(configuration)
