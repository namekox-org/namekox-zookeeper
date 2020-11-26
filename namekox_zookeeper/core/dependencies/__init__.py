#! -*- coding: utf-8 -*-

# author: forcemain@163.com


import six
import json
import socket


from kazoo.client import KazooClient
from namekox_core.core.friendly import AsLazyProperty
from namekox_core.core.generator import generator_uuid
from namekox_core.core.friendly import ignore_exception
from namekox_core.core.service.dependency import Dependency
from namekox_zookeeper.constants import ZOOKEEPER_CONFIG_KEY, DEFAULT_ZOOKEEPER_SERVICE_ROOT_PATH


class ZooKeeperHelper(Dependency):
    def __init__(self, dbname, watching=None, allotter=None, coptions=None, roptions=None):
        self.coptions = coptions
        self.services = {}
        self.instance = None
        self.dbname = dbname
        self.watching = watching
        self.allotter = allotter
        self.coptions = coptions or {}
        self.roptions = roptions or {}
        self.callback = lambda children: self.set_zk_service()
        super(ZooKeeperHelper, self).__init__(dbname, watching, allotter, coptions, roptions)

    @AsLazyProperty
    def configs(self):
        return self.container.config.get(ZOOKEEPER_CONFIG_KEY, {})

    @staticmethod
    def get_host_byname():
        name = socket.gethostname()
        return ignore_exception(socket.gethostbyname)(name)

    def set_zk_service(self):
        nservices = {}
        base_root = DEFAULT_ZOOKEEPER_SERVICE_ROOT_PATH
        for name in self.instance.get_children(base_root):
            path = '{}/{}'.format(base_root, name)
            data = ignore_exception(json.loads)(self.instance.get(path)[0])
            name = name.split('.', 1)[0]
            nservices.setdefault(name, [])
            if not data or data in nservices[name]:
                continue
            nservices[name].append(data)
        self.services = nservices
        self.allotter and self.allotter.set(nservices)

    def setup_watching(self):
        self.callback = self.instance.ChildrenWatch(self.watching)(self.callback)

    def setup_register(self):
        r_options = self.roptions.copy()
        host_addr = self.get_host_byname()
        r_options.setdefault('port', 80)
        r_options.setdefault('weight', 0)
        r_options.setdefault('address', host_addr or '127.0.0.1')
        serv_name = self.container.service_cls.name
        base_root = DEFAULT_ZOOKEEPER_SERVICE_ROOT_PATH
        base_path = '{}/{}.{}'.format(base_root, serv_name, generator_uuid())
        self.instance.ensure_path(base_root)
        host_info = json.dumps(r_options)
        self.instance.create(base_path, host_info, ephemeral=True)

    def setup(self):
        config = self.configs.get(self.dbname, {}).copy()
        [config.update({k: v}) for k, v in six.iteritems(self.coptions)]
        self.instance = KazooClient(**config)
        self.watching and self.setup_watching()
        self.coptions = config

    def start(self):
        self.instance and self.instance.start()
        self.watching and self.setup_register()

    def stop(self):
        self.instance and self.instance.stop()
