
import json
import datetime
import logging

from libdlt.logging import getLogger, debug, info
from libdlt.protocol.exceptions import AllocationException
from libdlt.protocol.ibp.allocation import IBPExtent
from libdlt.depot import Depot
import libdlt.protocol.ibp.services as services
import libdlt.protocol.ibp.flags as flags

from unis.models import Extent

# construct adaptor from existing metadata
@info("IBP.factory")
def buildAllocation(json):
    if type(json) is str:
        try:
            json = json.loads(json)
        except Exception as exp:
            logger.warn("{func:>20}| Could not decode allocation - {exp}".format(func = "buildAllocation", exp = exp))
            raise AllocationException("Could not decode json")

    if isinstance(json, IBPExtent):
        alloc = json
    elif isinstance(json, dict):
        alloc = IBPExtent(json)
    else:
        raise AllocationException("Invalid input type")
    
    alloc.depot = Depot(alloc.location)
    tmpAdapter = IBPAdaptor(alloc)
    
    return tmpAdapter

# create a new object and metadata given data and depot target
@info("IBP.factory")
def makeAllocation(data, offset, depot, **kwds):
    return IBPAdaptor(data=data, offset=offset, depot=depot, **kwds)
    
class IBPAdaptor(object):
    @debug("IBPAdaptor")
    def __init__(self, alloc=None, data=None, offset=None, depot=None, **kwds):
        self.log = getLogger()
        self._service = services.ProtocolService()
        
        if data:
            self._allocation = self._service.allocate(depot, offset, len(data), **kwds)
            self.write(data,**kwds)
        else:
            self._allocation = alloc
    
    @info("IBPAdaptor")
    def getMetadata(self):
        return self._allocation
        
    @info("IBPAdaptor")
    def read(self, loop, **kwds):
        return self._service.load(self._allocation, loop, **kwds)
        
    @info("IBPAdaptor")
    def write(self, data, **kwds):
        try:
            self._service.store(self._allocation, data, len(data), **kwds)
        except:
            import traceback
            traceback.print_exp()
            raise
        
    @info("IBPAdaptor")
    def check(self, **kwds):
        depot_status = self._service.GetStatus(self._allocation.depot)
        
        if not depot_status:
            raise AllocationException("could not contact Depot")
        
        alloc_status = self._service.Probe(self._allocation)
        self._log.debug("IBPAdapter.Check: {status}".format(status = alloc_status))
        
        if not alloc_status:
            raise AllocationException("Could not retrieve status")
        
        if "duration" in alloc_status:
            self._allocation.end = datetime.datetime.utcnow() + datetime.timedelta(seconds = int(alloc_status["duration"]))
        
        return True
        
    @info("IBPAdaptor")
    def copy(self, destination, **kwds):
        host   = self._allocation.host
        port   = self._allocation.port
        offset = kwds.get("offset", 0)
        size   = kwds.get("size", self._allocation.depotSize - offset)
        
        dest_alloc = buildAllocation(self._allocation.to_JSON())
        
        response = self._service.Allocate(destination, size, **kwds)
        if not response:
            return False
        
        dest_alloc._allocation.Inherit(response)
        dest_alloc.offset = offset
        duration = self._service.Send(self._allocation, alloc, **kwds)
        
        if not duration:
            return False
        
        dest_alloc._allocation.start = datetime.datetime.utcnow()
        dest_alloc._allocation.end = datetime.datetime.utcnow() + datetime.timedelta(seconds = duration)
        
        return dest_alloc
        
    @info("IBPAdaptor")
    def move(self, destination, **kwds):
        return self.Copy(destination, **kwds)
        
    @info("IBPAdaptor")
    def release(self):
        details = self._service.Probe(self._allocation)
        self._allocation.end = datetime.datetime.utcnow()
        
        if details:
            for i in range(1, int(details["read_count"]) + 1):
                result = self._service.Manage(self._allocation, mode = flags.IBP_DECR, cap_type = flags.IBP_READCAP)
                if not result:
                    return False
                    
            return True
        else:
            return False

    @info("IBPAdaptor")
    def manage(self, **kwds):
        if not self._service.Manage(self._allocation, **kwds):
            return False

        #####################
        # FOR DEBUGGING ONLY#
        status = self._service.Probe(self._allocation)
        self.log.debug("Manage result: {status}".format(status = status))
        #####################

        if "duration" in kwds:
            self._allocation.end = datetime.datetime.utcnow() + datetime.timedelta(seconds = kwds["duration"])

    def __eq__(self, other):
        if type(other) is IBPAdaptor:
            return str(self._allocation.GetReadCapability()) == str(other._allocation.GetReadCapability())
        else:
            return NotImplemented

    def __ne__(self, other):
        if type(other) is IBPAdaptor:
            return str(self._allocation.GetReadCapability()) != str(self._allocation.GetReadCapability())
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
