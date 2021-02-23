#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import unicode_literals


from kazoo.client import KazooClient
from namekox_core.core.friendly import AsLazyProperty
from namekox_zookeeper.constants import ZOOKEEPER_CONFIG_KEY


class ZooKeeperProxy(object):
    def __init__(self, config, **options):
        self.config = config
        self.client = None
        self.options = options

    @AsLazyProperty
    def configs(self):
        return self.config.get(ZOOKEEPER_CONFIG_KEY, {})

    def __call__(self, dbname, **options):
        self.options.update(options)
        config = self.configs[dbname].copy()
        config.update(self.options)
        self.client = KazooClient(**config)
        self.client.start()
        return self.client
