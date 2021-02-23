#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import unicode_literals


from itertools import cycle
from namekox_zookeeper.exceptions import RegServiceNotFound


class Allotter(object):
    def _raise(self, exc, errs=None):
        raise (exc if errs is None else exc(errs))

    def __init__(self, sdepd=None):
        self.iters = {}
        self.sdepd = sdepd

    def get(self, name):
        name not in self.sdepd.services and self._raise(RegServiceNotFound, name)
        data = self.sdepd.services[name]
        self.iters.setdefault(name, cycle(data))
        return self.iters[name].next()

    def set(self, sdepd):
        self.iters = {}
        self.sdepd = sdepd
