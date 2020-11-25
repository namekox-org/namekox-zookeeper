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
    def __init__(self, dbname, s_ipport=None, s_weight=0, watching=None, allotter=None, **options):
        self.options = options
        self.services = {}
        self.instance = None
        self.dbname = dbname
        self.s_ipport = s_ipport
        self.s_weight = s_weight
        self.watching = watching
        self.allotter = allotter
        self.callback = lambda children: self.set_zk_service()
        super(ZooKeeperHelper, self).__init__(dbname, s_ipport, s_weight, watching, allotter, **options)

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
        host_addr = self.get_host_byname()
        host_data = {'host': host_addr or '127.0.0.1'}
        serv_name = self.container.service_cls.name
        base_root = DEFAULT_ZOOKEEPER_SERVICE_ROOT_PATH
        base_path = '{}/{}.{}'.format(base_root, serv_name, generator_uuid())
        self.instance.ensure_path(base_root)
        host_info = json.dumps({
            'weight': self.s_weight,
            'server': self.s_ipport.format(**host_data)
        })
        self.instance.create(base_path, host_info, ephemeral=True)

    def setup(self):
        config = self.configs.get(self.dbname, {}).copy()
        [config.update({k: v}) for k, v in six.iteritems(self.options)]
        self.instance = KazooClient(**config)
        self.watching and self.setup_watching()
        self.options = config

    def start(self):
        self.instance and self.instance.start()
        self.s_ipport and self.setup_register()

    def stop(self):
        self.instance and self.instance.stop()
