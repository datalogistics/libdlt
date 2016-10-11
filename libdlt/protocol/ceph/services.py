from rados.core import Cluster

class ProtocolService(object):
    def __init__(self):
        self.cluster_cache = dict()

    def _get_cluster(self, **kwds):
        conf = kwds.get("config", '')
        name = kwds.get("name", '')
        cname = kwds.get("clustername", None)
        cluster = self.cluster_cache.get(conf, None)
        if not cluster:
            cluster = Cluster(conffile=conf, clustername=cname)
            cluster.connect()
            self.cluster_cache[conf] = cluster
        return cluster
        
    def write(self, oid, data, **kwds):
        cluster = self._get_cluster(**kwds)
        pool = kwds.get("pool", "dlt")
        ioctx = cluster.open_ioctx(pool)
        ioctx.write_full(oid, data)
        ioctx.close()
        
    def read(self, p, oid, size, **kwds):
        cluster = self._get_cluster(**kwds)
        ioctx = cluster.open_ioctx(p)
        ret = ioctx.read(oid, size)
        ioctx.close()
        return ret
