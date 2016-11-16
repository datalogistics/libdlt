import uuid
from uritools import urisplit

from unis.models import Extent
from libdlt.logging import info, debug
from libdlt.protocol.ceph.allocation import CephExtent
from libdlt.protocol.ceph.services import ProtocolService
from libdlt.protocol.exceptions import AllocationException

ceph = ProtocolService()

@info("Ceph.factory")
def buildAllocation(obj):
    if type(obj) is str:
        try:
            obj = json.loads(obj)
        except Exception as exp:
            raise AllocationException("Could not decode json")
    if type(obj) is dict:
        alloc = CephExtent(obj)
    elif type(obj) in [CephExtent, Extent]:
        alloc = obj
    else:
        raise AllocationException("Invalid input type")
    return CephAdaptor(alloc)

@info("Ceph.factory")
async def makeAllocation(data, offset, depot, loop, **kwds):
    alloc = CephExtent()
    pool = kwds.get("pool", "dlt")
    oid = str(uuid.uuid4())
    alloc.location = "{0}/{1}/{2}".format(depot.endpoint, pool, oid)
    alloc.pool = pool
    alloc.offset = offset
    alloc.size = len(data)
    await ceph.write(oid, data, loop, **kwds)
    print(offset)
    return CephAdaptor(alloc)

class CephAdaptor(object):
    @debug("CephAdaptor")
    def __init__(self, alloc, **kwds):
        self._allocation = alloc
        
    @info("CephAdaptor")
    def getMetadata(self):
        return self._allocation
        
    @info("CephAdaptor")
    def read(self, **kwds):
        o = urisplit(self._allocation.location)
        parts = o.path.split('/')
        size = self._allocation.size
        return ceph.read(parts[1], parts[2], size, **kwds)
    
    @info("CephAdaptor")
    def copy(self, depot, src_kwds, dst_kwds):
        dst_alloc = CephExtent()
        dst_oid = str(uuid.uuid4())
        pool = dst_kwds.get('pool', 'dlt')
        dst_alloc.location = "{}/{}/{}".format(depot.endpoint, pool, dst_oid)
        dst_alloc.pool = pool
        dst_alloc.size = self._allocation.size
        dst_alloc.offset = self._allocation.offset
        
        src = urisplit(self._allocation.location)
        src = src.path.split('/')
        size = self._allocation.size
        
        ceph.copy(src[1], src[2], dst_oid, size, src_kwds, dst_kwds)
        return CephAdaptor(dst_alloc)
