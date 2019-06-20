
import json
import datetime

from libdlt.protocol.exceptions import AllocationError
from libdlt.protocol.ibp.allocation import IBPExtent
from libdlt.depot import Depot
import libdlt.protocol.ibp.services as services
import libdlt.protocol.ibp.flags as flags

from lace import logging
from lace.logging import trace
from unis.models import Extent

# construct adaptor from existing metadata
@trace.info("IBP.factory")
def buildAllocation(json):
    if type(json) is str:
        try:
            json = json.loads(json)
        except Exception as exp:
            logger.warn("{func:>20}| Could not decode allocation - {exp}".format(func = "buildAllocation", exp = exp))
            raise AllocationError("Could not decode json")

    if isinstance(json, IBPExtent):
        alloc = json
    elif isinstance(json, dict):
        alloc = IBPExtent(json)
    else:
        raise AllocationError("Invalid input type")
    
    alloc.depot = Depot(alloc.location)
    tmpAdapter = IBPAdaptor(alloc)
    
    return tmpAdapter

# create a new object and metadata given data and depot target
@trace.info("IBP.factory")
def makeAllocation(data, offset, depot, **kwds):
    try:
        return IBPAdaptor(data=data, offset=offset, depot=depot, **kwds)
    except:
        raise AllocationError("Failed to generate allocation")
    
class IBPAdaptor(object):
    @trace.debug("IBPAdaptor")
    def __init__(self, alloc=None, data=None, offset=None, depot=None, **kwds):
        self.log = logging.getLogger()
        self._service = services.ProtocolService()
        
        if data:
            self._allocation = self._service.allocate(depot, offset, len(data), **kwds)
            self.write(data,**kwds)
        else:
            self._allocation = alloc
    
    @trace.info("IBPAdaptor")
    def getMetadata(self):
        return self._allocation
        
    @trace.info("IBPAdaptor")
    def read(self, **kwds):
        return self._service.load(self._allocation, **kwds)
        
    @trace.info("IBPAdaptor")
    def write(self, data, **kwds):
        try:
            self._service.store(self._allocation, data, len(data), **kwds)
        except:
            import traceback
            traceback.print_exc()
            raise
        
    @trace.info("IBPAdaptor")
    def check(self, **kwds):
        depot_status = self._service.getStatus(self._allocation.depot)
        
        if not depot_status:
            raise AllocationError("could not contact Depot")
        
        alloc_status = self._service.probe(self._allocation)
        self._log.debug("IBPAdapter.Check: {status}".format(status = alloc_status))
        
        if not alloc_status:
            raise AllocationError("Could not retrieve status")
        
        if "duration" in alloc_status:
            self._allocation.end = datetime.datetime.utcnow() + datetime.timedelta(seconds = int(alloc_status["duration"]))
        
        return True
        
    @trace.info("IBPAdaptor")
    def copy(self, destination, src_kwargs, dst_kwargs, **kwds):
        host   = self._allocation.depot.host
        port   = self._allocation.depot.port
        offset = kwds.get("offset", 0)
        size   = kwds.get("size", self._allocation.depotSize - offset)
        
        dest_alloc = buildAllocation(self._allocation.to_JSON())
        
        response = self._service.allocate(destination, size, **kwds)
        if not response:
            return False
        
        dest_alloc._allocation.setReadCapability(str(response.getReadCapability()))
        dest_alloc._allocation.setWriteCapability(str(response.getWriteCapability()))
        dest_alloc._allocation.setManageCapability(str(response.getManageCapability()))
        dest_alloc._allocation.depot = response.depot
        dest_alloc._allocation.location = response.location
        dest_alloc.offset = offset
        del dest_alloc._allocation.function
        duration = self._service.send(self._allocation, dest_alloc, **kwds)
        
        if not duration:
            return False
        
        dest_alloc._allocation.start = datetime.datetime.utcnow()
        dest_alloc._allocation.end = datetime.datetime.utcnow() + datetime.timedelta(seconds = duration)
        
        return dest_alloc
        
    @trace.info("IBPAdaptor")
    def move(self, destination, **kwds):
        return self.copy(destination, **kwds)
        
    @trace.info("IBPAdaptor")
    def release(self):
        details = self._service.probe(self._allocation)
        self._allocation.end = datetime.datetime.utcnow()
        
        if details:
            for i in range(1, int(details["read_count"]) + 1):
                result = self._service.manage(self._allocation, mode = flags.IBP_DECR, cap_type = flags.IBP_READCAP)
                if not result:
                    return False
                    
            return True
        else:
            return False

    @trace.info("IBPAdaptor")
    def manage(self, **kwds):
        if not self._service.manage(self._allocation, **kwds):
            return False

        #####################
        # FOR DEBUGGING ONLY#
        status = self._service.probe(self._allocation)
        self.log.debug("Manage result: {status}".format(status = status))
        #####################

        if "duration" in kwds:
            self._allocation.end = datetime.datetime.utcnow() + datetime.timedelta(seconds = kwds["duration"])

    def __eq__(self, other):
        if type(other) is IBPAdaptor:
            return str(self._allocation.getReadCapability()) == str(other._allocation.getReadCapability())
        else:
            return NotImplemented

    def __ne__(self, other):
        if type(other) is IBPAdaptor:
            return str(self._allocation.getReadCapability()) != str(self._allocation.getReadCapability())
        else:
            return NotImplemented


    def __cmp__(self, other):
        if type(other) is IBPAdaptor:
            if self._allocation.timestamp < other._allocation.timestamp:
                return -1
            elif self._allocation.timestamp == other._allocation.timestamp:
                return 0
            else:
                return 1
        elif type(other) is datetime.datetime:
            if self._allocation.end < other:
                return -1
            elif self._allocation.end == other:
                return 0
            else:
                return 1
        else:
            raise TypeError("Cannot compare {t1} and {t2}".format(t1 = type(self), t2 = type(other)))
