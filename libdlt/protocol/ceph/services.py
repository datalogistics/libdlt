import asyncio

from rados.core import Cluster

from libdlt.logging import debug, info

class ProtocolService(object):
    @debug("Ceph.ProtocolService")
    def __init__(self):
        self.cluster_cache = dict()
        
    @debug("Ceph.ProtocolService")
    async def _get_cluster(self, loop, **kwds):
        conf = kwds.get("config", '')
        name = kwds.get("name", '')
        cname = kwds.get("clustername", None)
        cluster = self.cluster_cache.get(conf, None)
        if not cluster:
            cluster = Cluster(conffile=conf, clustername=cname)
            await loop.run_in_executor(None, cluster.connect)
            self.cluster_cache[conf] = cluster
        return cluster
        
    @info("Ceph.ProtocolService")
    def copy(self, p, src_oid, dst_oid, size, src_kwds, dst_kwds):
        src_cluster = self._get_cluster(**src_kwds)
        dst_cluster = self._get_cluster(**dst_kwds)
        ioctx = src_cluster.open_ioctx(p)
        data = ioctx.read(src_oid, size)
        
        ioctx.close()
        
        pool = dst_kwds.get("pool", "dlt")
        ioctx = dst_cluster.open_ioctx(pool)
        ioctx.write_full(dst_oid, data)
        ioctx.close()
    
    @info("Ceph.ProtocolService")
    async def write(self, oid, data, loop, **kwds):
        cluster = await self._get_cluster(loop, **kwds)
        pool = kwds.get("pool", "dlt")
        ioctx = cluster.open_ioctx(pool)
        await loop.run_in_executor(None, ioctx.write_full, oid, data)
        ioctx.close()
        
    @info("Ceph.ProtocolService")
    def read(self, p, oid, size, **kwds):
        cluster = self._get_cluster(**kwds)
        ioctx = cluster.open_ioctx(p)
        ret = ioctx.read(oid, size)
        ioctx.close()
        return ret
