
import json
import datetime
import logging

from libdlt.protocol.exceptions import AllocationException
from libdlt.protocol.ibp.allocation import Allocation
import libdlt.protocol.ibp.services as services
import libdlt.protocol.ibp.flags as flags

from unis.models import Extent

def buildAllocation(json):
    if type(json) is str:
        try:
            json = json.loads(json)
        except Exception as exp:
            logging.getLogger().warn("{func:>20}| Could not decode allocation - {exp}".format(func = "buildAllocation", exp = exp))
            raise AllocationException("Could not decode json")

    if type(json) is dict:
        alloc = Allocation(json)
    elif type(json) is Allocation:
        alloc = json
    else:
        raise AllocationException("Invalid input type")
            
    tmpAdapter = IBPAdaptor(alloc)
    
    return tmpAdapter

def makeAllocation(offset, data, depot, **kwds):
    ps = services.ProtocolService()
    alloc = ps.Allocate(depot, offset, len(data), **kwds)
    # XXX: write/send first
    return IBPAdaptor(alloc)
    
class IBPAdaptor(object):
    def __init__(self, alloc):
        self._log = logging.getLogger()
        self._service = services.ProtocolService()
        self._allocation = alloc
    
    def GetMetadata(self):
        return self._allocation

    def Read(self, **kwargs):
        pass
    
    def Check(self, **kwargs):
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
        
    def Copy(self, destination, **kwargs):
        host   = self._allocation.host
        port   = self._allocation.port
        offset = kwargs.get("offset", 0)
        size   = kwargs.get("size", self._allocation.depotSize - offset)
        
        dest_alloc = buildAllocation(self._allocation.to_JSON())

        response = self._service.Allocate(destination, size, **kwargs)
        if not response:
            return False
        
        dest_alloc._allocation.Inherit(response)
        dest_alloc.offset = offset
        duration = self._service.Send(self._allocation, alloc, **kwargs)
        
        if not duration:
            return False
        
        dest_alloc._allocation.start = datetime.datetime.utcnow()
        dest_alloc._allocation.end = datetime.datetime.utcnow() + datetime.timedelta(seconds = duration)
        
        return dest_alloc
        
    def Move(self, destination, **kwargs):
        return self.Copy(destination, **kwargs)
        
    def Release(self):
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

    def Manage(self, **kwargs):
        if not self._service.Manage(self._allocation, **kwargs):
            return False

        #####################
        # FOR DEBUGGING ONLY#
        status = self._service.Probe(self._allocation)
        self._log.debug("Manage result: {status}".format(status = status))
        #####################

        if "duration" in kwargs:
            self._allocation.end = datetime.datetime.utcnow() + datetime.timedelta(seconds = kwargs["duration"])

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
