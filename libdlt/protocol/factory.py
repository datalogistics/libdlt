import json

import libdlt.protocol.ibp.factory as ibp
import libdlt.protocol.ceph.factory as ceph
#import libdlt.protocol.rdma.factory as rdma
#import libdlt.protocol.gdrive.factory as gdrive

from lace import logging
from lace.logging import trace
from libdlt.protocol.ibp.allocation import IBP_EXTENT_URI
from libdlt.protocol.ceph.allocation import CEPH_EXTENT_URI
from libdlt.depot import Depot
#from libdlt.protocol.rdma.allocation import RDMA_EXTENT_URI
#from libdlt.protocol.gdrive.allocation import GOOGLE_EXTENT_URI

from unis.models import Extent
from urllib.parse import urlparse

PROTOCOL_MAP = {
    "ceph": ceph,
    "ibp": ibp,
#    "rdma": rdma,
#    "google": gdrive
}

SCHEMA_MAP = {
    CEPH_EXTENT_URI: ceph,
    IBP_EXTENT_URI: ibp,
#    RDMA_EXTENT_URI: rdma,
#    GOOGLE_EXTENT_URI: gdrive
}

@trace.info("libdlt.factory")
def buildAllocation(json):
    if type(json) is str:
        try:
            json = json.loads(json)
        except Exception as exp:
            logging.getLogger("libdlt").warn("{func:>20}| Could not decode allocation - {exp}".format(func = "buildAllocation", exp = exp))
            return False

    if isinstance(json, Extent):
        schema = getattr(json, "$schema")
    else:
        schema = json["schema"]
    return SCHEMA_MAP[schema].buildAllocation(json)

@trace.info("libdlt.factory")
def makeProxy(alloc):
    if not hasattr(alloc, 'depot'):
        alloc.depot = Depot(getattr(alloc, 'location', ''))
    return SCHEMA_MAP[getattr(alloc, "$schema", IBP_EXTENT_URI)].services.ProtocolService()

@trace.info("libdlt.factory")
def makeProxyFromURI(uri):
    uri = urlparse(uri)
    return PROTOCOL_MAP[uri.scheme].services.ProtocolService()

@trace.info("libdlt.factory")
def makeAllocation(data, offset, depot, **kwds):
    return PROTOCOL_MAP[depot.scheme].makeAllocation(data, offset, depot, **kwds)
