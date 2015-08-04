from StringIO import StringIO

from contextlib import contextmanager

from fabric.api import *


@task
def generate_conf(filename, **kwargs):
    content = "\n".join(["export {}={}".format(key, value) for key, value in kwargs.iteritems()])
    put(StringIO(content), filename)


@contextmanager
def source(filename):
    with prefix(". {}".format(filename)):
        yield
