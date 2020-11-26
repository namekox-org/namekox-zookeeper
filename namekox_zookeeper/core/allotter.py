#! -*- coding: utf-8 -*-

# author: forcemain@163.com


from itertools import cycle


class Allotter(object):
    def __init__(self, sdepd=None):
        self.iters = {}
        self.sdepd = sdepd

    def get(self, name):
        data = self.sdepd.services[name]
        self.iters.setdefault(name, cycle(data))
        return self.iters[name].next()

    def set(self, sdepd):
        self.iters = {}
        self.sdepd = sdepd
