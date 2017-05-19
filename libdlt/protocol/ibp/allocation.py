'''
@Name:   Allocation.py
@Author: Jeremy Musser
@Data:   04/01/2015

------------------------------

Allocation is a formal definition of the
allocation structure.  It contains keys
that can be used to access data on an IBP
depot.
'''

from datetime import datetime
from lace import logging
from lace.logging import trace

from unis.models import Lifetime, schemaLoader
from libdlt.depot import Depot

IBP_EXTENT_URI = "http://unis.crest.iu.edu/schema/exnode/ext/1/ibp#"

IBPExtent = schemaLoader.get_class(IBP_EXTENT_URI)

class Allocation(IBPExtent):
    @trace.debug("IBP.Allocation")
    def initialize(self, data={}):
        super(Allocation, self).initialize(data)
        self.log       = logging.getLogger()
        self.timestamp  = 0
        self.depot      = Depot(self.location) if hasattr(self, "location") else None
        self.lifetime   = Lifetime()
        self.readcap    = None
        self.writecap   = None
        self.managecap  = None
        
        if data:
            self.setReadCapability(self.mapping.read)
            self.setWriteCapability(self.mapping.write)
            self.setManageCapability(self.mapping.manage)
        
    def getStartTime(self):
        return datetime.strptime(self.lifetimes.start, "%Y-%m-%d %H:%M:%S")
        
    def getEndTime(self):
        return datetime.strptime(self.lifetimes.start, "%Y-%m-%d %H:%M:%S")
        
    def setStartTime(self, dt):
        self.lifetime.start = dt.strftime("%Y-%m-%d %H:%M:%S")

    def setEndTime(self, dt):
        self.lifetime.end = dt.strftime("%Y-%m-%d %H:%M:%S")
        
    def getReadCapability(self):
        return self.readcap
        
    def getWriteCapability(self):
        return self.writecap
        
    def getManageCapability(self):
        return self.managecap
        
    def setReadCapability(self, read):
        try:
            tmpCap = Capability(read)
        except ValueError as exp:
            self.log.warn("{func:>20}| Unable to create capability - {exp}".format(func = "SetReadCapability", exp = exp))
            return False
        self.mapping.read = str(tmpCap)
        self.readcap = tmpCap
        
    def setWriteCapability(self, write):
        try:
            tmpCap = Capability(write)
        except ValueError as exp:
            self.log.warn("{func:>20}| Unable to create capability - {exp}".format(func = "SetWriteCapability", exp = exp))
            return False
        self.mapping.write = str(tmpCap)
        self.writecap = tmpCap

    def setManageCapability(self, manage):
        try:
            tmpCap = Capability(manage)
        except ValueError as exp:
            self.log.warn("{func:>20}| Unable to create capability - {exp}".format(func = "SetManageCapability", exp = exp))
            return False
        self.mapping.manage = str(tmpCap)
        self.managecap = tmpCap

class Capability(object):
    def __init__(self, cap_string):
        try:
            self._cap       = cap_string
            tmpSplit        = cap_string.split("/")
            tmpAddress      = tmpSplit[2].split(":")
            self.key        = tmpSplit[3]
            self.wrmKey     = tmpSplit[4]
            self.code       = tmpSplit[5]
        except Exception as exp:
            raise ValueError('Malformed capability string')

    def __str__(self):
        return self._cap

    def __repr__(self):
        return self.__str__()
