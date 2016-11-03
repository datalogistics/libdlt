from rados.core import Cluster

from libdlt.logging import debug, info

class ProtocolService(object):
    @debug("Ceph.ProtocolService")
    def __init__(self):
        self.cluster_cache = dict()

    @debug("Ceph.ProtocolService")
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
        
    @info("Ceph.ProtocolService")
    def copy(self, p, src_oid, dst_oid, size, **kwds):
        cluster = self._get_cluster(**kwds)
        ioctx = cluster.open_ioctx(p)
        data = ioctx.read(src_oid, size)
        
        pool = kwds.get("pool", "dlt")
        ioctx.write_full(dst_oid, size)
        ioctx.cloise()
    
    @info("Ceph.ProtocolService")
    def write(self, oid, data, **kwds):
        cluster = self._get_cluster(**kwds)
        pool = kwds.get("pool", "dlt")
        ioctx = cluster.open_ioctx(pool)
        ioctx.write_full(oid, data)
        ioctx.close()
        
    @info("Ceph.ProtocolService")
    def read(self, p, oid, size, **kwds):
        cluster = self._get_cluster(**kwds)
        ioctx = cluster.open_ioctx(p)
        ret = ioctx.read(oid, size)
        ioctx.close()
        return ret
