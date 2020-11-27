# Install
```shell script
pip install -U namekox-zookeeper
```

# Example
```python
# ! -*- coding: utf-8 -*-
#
# author: forcemain@163.com


from namekox_zookeeper.core.allotter import Allotter
from namekox_webserver.core.entrypoints.app import app
from namekox_zookeeper.core.dependencies import ZooKeeperHelper
from namekox_zookeeper.constants import DEFAULT_ZOOKEEPER_SERVICE_ROOT_PATH


class Ping(object):
    name = 'ping'

    # https://kazoo.readthedocs.io/en/2.5.0/
    # ZooKeeperHelper(
    #       dbname, 
    #       watching=None, 
    #       allotter=None,
    # https://kazoo.readthedocs.io/en/2.5.0/api/client.html#kazoo.client.KazooClient.__init__ 
    #       coptions=None, 
    # {'address': '127.0.0.1', 'port': 80}
    #       roptions=None
    # )
    zk = ZooKeeperHelper(
        name,
        allotter=Allotter(),
        watching=DEFAULT_ZOOKEEPER_SERVICE_ROOT_PATH
    )

    @app.api('/api/assign/server/', methods=['GET'])
    def assign_server(self, request):
        return self.zk.allotter.get(self.name)
```

# Running
> config.yaml
```yaml
ZOOKEEPER:
  ping:
    hosts: 127.0.0.1:2181
WEBSERVER:
  host: 0.0.0.0
  port: 80
```
> namekox run ping
```shell script
2020-11-24 16:05:56,374 DEBUG load container class from namekox_core.core.service.container:ServiceContainer
2020-11-24 16:05:56,375 DEBUG starting services ['ping']
2020-11-24 16:05:56,376 DEBUG starting service ping entrypoints [ping:namekox_webserver.core.entrypoints.app.handler.ApiServerHandler:assign_server, ping:namekox_webserver.core.entrypoints.app.server.WebServer:server]
2020-11-24 16:05:56,378 DEBUG spawn manage thread handle ping:namekox_webserver.core.entrypoints.app.server:handle_connect(args=(), kwargs={}, tid=handle_connect)
2020-11-24 16:05:56,379 DEBUG service ping entrypoints [ping:namekox_webserver.core.entrypoints.app.handler.ApiServerHandler:assign_server, ping:namekox_webserver.core.entrypoints.app.server.WebServer:server] started
2020-11-24 16:05:56,379 DEBUG starting service ping dependencies [ping:namekox_zookeeper.core.dependencies.ZooKeeperHelper:zk]
2020-11-24 16:05:56,380 INFO Connecting to 127.0.0.1:2181
2020-11-24 16:05:56,381 DEBUG Sending request(xid=None): Connect(protocol_version=0, last_zxid_seen=0, time_out=10000, session_id=0, passwd='\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', read_only=None)
2020-11-24 16:05:56,382 INFO Zookeeper connection established, state: CONNECTED
2020-11-24 16:05:56,383 DEBUG Sending request(xid=1): GetChildren(path='/namekox', watcher=<bound method ChildrenWatch._watcher of <kazoo.recipe.watchers.ChildrenWatch object at 0x103459150>>)
2020-11-24 16:05:56,384 DEBUG Received response(xid=1): []
2020-11-24 16:05:56,385 DEBUG Sending request(xid=2): GetChildren(path='/namekox', watcher=None)
2020-11-24 16:05:56,386 DEBUG Received response(xid=2): []
2020-11-24 16:05:56,392 DEBUG Sending request(xid=3): Exists(path='/namekox', watcher=None)
2020-11-24 16:05:56,393 DEBUG Received response(xid=3): ZnodeStat(czxid=74, mzxid=74, ctime=1606123632647, mtime=1606123632647, version=0, cversion=62, aversion=0, ephemeralOwner=0, dataLength=0, numChildren=0, pzxid=310)
2020-11-24 16:05:56,398 DEBUG Sending request(xid=4): Create(path='/namekox/ping.e31f59b3-4748-4212-b553-42dfe902cf19', data='{"address": "127.0.0.1", "port": 80}', acl=[ACL(perms=31, acl_list=['ALL'], id=Id(scheme='world', id='anyone'))], flags=1)
2020-11-24 16:05:56,402 DEBUG Received EVENT: Watch(type=4, state=3, path=u'/namekox')
2020-11-24 16:05:56,403 DEBUG Received response(xid=4): u'/namekox/ping.e31f59b3-4748-4212-b553-42dfe902cf19'
2020-11-24 16:05:56,403 DEBUG Sending request(xid=5): GetChildren(path='/namekox', watcher=<bound method ChildrenWatch._watcher of <kazoo.recipe.watchers.ChildrenWatch object at 0x103459150>>)
2020-11-24 16:05:56,405 DEBUG service ping dependencies [ping:namekox_zookeeper.core.dependencies.ZooKeeperHelper:zk] started
2020-11-24 16:05:56,407 DEBUG services ['ping'] started
2020-11-24 16:05:56,408 DEBUG Received response(xid=5): [u'ping.e31f59b3-4748-4212-b553-42dfe902cf19']
2020-11-24 16:05:56,408 DEBUG Sending request(xid=6): GetChildren(path='/namekox', watcher=None)
2020-11-24 16:05:56,409 DEBUG Received response(xid=6): [u'ping.e31f59b3-4748-4212-b553-42dfe902cf19']
2020-11-24 16:05:56,410 DEBUG Sending request(xid=7): GetData(path='/namekox/ping.e31f59b3-4748-4212-b553-42dfe902cf19', watcher=None)
2020-11-24 16:05:56,410 DEBUG Received response(xid=7): ('{"port": 80, "address": "127.0.0.1"}', ZnodeStat(czxid=312, mzxid=312, ctime=1606205156399, mtime=1606205156399, version=0, cversion=0, aversion=0, ephemeralOwner=72057605710938198, dataLength=39, numChildren=0, pzxid=312))
```
> curl http://127.0.0.1/api/assign/server/
```json
{
    "errs": "", 
    "code": "Request:Success", 
    "data": {
        "port": 80,
        "address": "127.0.0.1"
    }, 
    "call_id": "e19a2c8c-09ff-4543-95f7-81bedafb9485"
}
```

# Debug
> config.yaml
```yaml
CONTEXT:
  - namekox_zookeeper.cli.subctx.zookeeper:ZooKeeper
ZOOKEEPER:
  ping:
    hosts: 127.0.0.1:2181
WEBSERVER:
  host: 0.0.0.0
  port: 80
```
> namekox shell
```shell script
In [1]: nx.zookeeper.proxy('ping').get_children('/namekox')
2020-11-24 16:07:17,577 INFO Connecting to 127.0.0.1:2181
2020-11-24 16:07:17,578 DEBUG Sending request(xid=None): Connect(protocol_version=0, last_zxid_seen=0, time_out=10000, session_id=0, passwd='\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', read_only=None)
2020-11-24 16:07:17,579 INFO Zookeeper connection established, state: CONNECTED
2020-11-24 16:07:17,580 DEBUG Sending request(xid=1): GetChildren(path='/namekox', watcher=None)
2020-11-24 16:07:17,581 DEBUG Received response(xid=1): [u'ping.e31f59b3-4748-4212-b553-42dfe902cf19']
Out[1]: [u'ping.e31f59b3-4748-4212-b553-42dfe902cf19']
In [2]: nx.zookeeper.proxy('ping').get('/namekox/ping.e31f59b3-4748-4212-b553-42dfe902cf19')[0]
2020-11-24 16:08:32,539 INFO Connecting to 127.0.0.1:2181
2020-11-24 16:08:32,540 DEBUG Sending request(xid=None): Connect(protocol_version=0, last_zxid_seen=0, time_out=10000, session_id=0, passwd='\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00', read_only=None)
2020-11-24 16:08:32,542 INFO Zookeeper connection established, state: CONNECTED
2020-11-24 16:08:32,544 DEBUG Sending request(xid=1): GetData(path='/namekox/ping.e31f59b3-4748-4212-b553-42dfe902cf19', watcher=None)
2020-11-24 16:08:32,545 DEBUG Received response(xid=1): ('{"address": "127.0.0.1", "port": 80}', ZnodeStat(czxid=312, mzxid=312, ctime=1606205156399, mtime=1606205156399, version=0, cversion=0, aversion=0, ephemeralOwner=72057605710938198, dataLength=39, numChildren=0, pzxid=312))
Out[2]: '{"port": 80, "address": "127.0.0.1"}'
```
