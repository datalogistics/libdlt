import uuid
from uritools import urisplit

from unis.models import Extent
from libdlt.protocol.ceph.allocation import CephExtent
from libdlt.protocol.ceph.services import ProtocolService
from libdlt.protocol.exceptions import AllocationException

ceph = ProtocolService()

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

def makeAllocation(data, offset, depot, **kwds):
    alloc = CephExtent()
    pool = kwds.get("pool", "dlt")
    oid = str(uuid.uuid4())
    alloc.location = "{0}/{1}/{2}".format(depot.endpoint, pool, oid)
    alloc.pool = pool
    alloc.offset = offset
    alloc.size = len(data)
    ceph.write(oid, data, **kwds)
    return CephAdaptor(alloc)

class CephAdaptor(object):
    def __init__(self, alloc, **kwds):
        self._allocation = alloc
        
    def GetMetadata(self):
        return self._allocation

    def Read(self, **kwds):
        o = urisplit(self._allocation.location)
        parts = o.path.split('/')
        size = self._allocation.size
        return ceph.read(parts[1], parts[2], size, **kwds)
