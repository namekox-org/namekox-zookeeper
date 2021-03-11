#! -*- coding: utf-8 -*-
#
# author: forcemain@163.com

from __future__ import unicode_literals

import six
import json
import socket


from kazoo.client import KazooClient
from kazoo.protocol.states import KazooState
from namekox_core.core.friendly import AsLazyProperty
from namekox_core.core.generator import generator_uuid
from namekox_core.core.friendly import ignore_exception
from namekox_core.core.service.dependency import Dependency
from kazoo.handlers.eventlet import SequentialEventletHandler
from namekox_zookeeper.constants import ZOOKEEPER_CONFIG_KEY, DEFAULT_ZOOKEEPER_SESSION_TIMEOUT


class ZooKeeperHelper(Dependency):
    def __init__(self, dbname, serverid=None, watching=None, allotter=None, coptions=None, roptions=None):
        self.coptions = coptions
        self.services = {}
        self.instance = None
        self.dbname = dbname
        self.watching = watching
        self.allotter = allotter
        self.coptions = coptions or {}
        self.roptions = roptions or {}
        self.serverid = serverid or generator_uuid()
        super(ZooKeeperHelper, self).__init__(dbname, serverid, watching, allotter, coptions, roptions)

    @AsLazyProperty
    def configs(self):
        return self.container.config.get(ZOOKEEPER_CONFIG_KEY, {})

    @staticmethod
    def get_host_byname():
        name = socket.gethostname()
        return ignore_exception(socket.gethostbyname)(name)

    def get_serv_name(self, name):
        return name.rsplit('/', 1)[-1].split('.', 1)[0]

    def gen_serv_name(self, name):
        return '{}/{}.{}'.format(self.watching, name, self.serverid)

    def update_zookeeper_services(self, c):
        services = {}
        children = c.get() if hasattr(c, 'get') else c
        for name in children:
            path = '{}/{}'.format(self.watching, name)
            data = ignore_exception(json.loads)(self.instance.get(path)[0])
            name = self.get_serv_name(name)
            data and services.setdefault(name, [])
            data and (data not in services[name]) and services[name].append(data)
        self.services = services
        self.allotter and self.allotter.set(self)

    def fetch_children(self):
        self.instance.get_children_async(self.watching).rawlink(self.update_zookeeper_services)

    def setup_watching(self):
        self.instance.ChildrenWatch(self.watching)(self.update_zookeeper_services)

    def setup_register(self):
        r_options = self.roptions.copy()
        host_addr = self.get_host_byname()

        r_options.setdefault('port', 80)
        r_options.setdefault('address', host_addr or '127.0.0.1')

        host_info = json.dumps(r_options)
        base_path = self.gen_serv_name(self.container.service_cls.name)
        self.instance.create_async(base_path, host_info, ephemeral=True, makepath=True)

    def setup_listener(self, state):
        state not in (KazooState.LOST, KazooState.SUSPENDED) and self.setup_register()
        state not in (KazooState.LOST, KazooState.SUSPENDED) and self.fetch_children()

    def setup(self):
        config = self.configs.get(self.dbname, {}).copy()
        config.setdefault('handler', SequentialEventletHandler())
        config.setdefault('timeout', DEFAULT_ZOOKEEPER_SESSION_TIMEOUT)
        [config.update({k: v}) for k, v in six.iteritems(self.coptions)]

        self.instance = KazooClient(**config)
        self.instance.add_listener(self.setup_listener)

        self.coptions = config

    def start(self):
        self.watching and self.setup_watching()
        self.instance and self.instance.start_async().wait(timeout=15)
        self.watching and self.setup_register()

    def stop(self):
        self.instance and ignore_exception(self.instance.stop)()
