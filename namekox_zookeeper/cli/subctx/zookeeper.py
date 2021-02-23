#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import unicode_literals


from namekox_zookeeper.core.proxy import ZooKeeperProxy


class ZooKeeper(object):
    def __init__(self, config):
        self.config = config
        self.proxy = ZooKeeperProxy(config)

    @classmethod
    def name(cls):
        return 'zookeeper'
