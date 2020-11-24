#! -*- coding: utf-8 -*-

# author: forcemain@163.com


from itertools import cycle


class Allotter(object):
    def __init__(self, data):
        self.iters = {}
        self.items = data

    def get(self, name):
        data = self.items[name]
        self.iters.setdefault(name, cycle(data))
        return self.iters[name].next()

    def set(self, data):
        self.iters = {}
        self.items = data
