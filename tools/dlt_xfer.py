#!/usr/bin/env python3

import os
import time
import getpass
import argparse
from uritools import urisplit
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import cycle
from pprint import pprint

import libdlt.util.util as util
import libdlt.protocol.factory as factory
from unis.runtime import Runtime
from unis.models import Exnode

UNIS_URL = "http://localhost:8888"
DEPOTS = {
#    "ceph://stark": {
#        "clustername": 'ceph',
#        "config": "/etc/ceph/ceph.conf",
#        "pool": "test",
#        "crush_map": None
#            },
#    "ceph://um-mon01.osris.org": {
#        "clustername": 'osiris',
#        "config": "/etc/ceph/osiris.conf",
#        "pool": "dlt",
#        "crush_map": None
#            },
    "ibp://ibp2.crest.iu.edu:6714": {
        "max_alloc_lifetime": 2592000
    }
}

def read_action(ext):
    alloc = factory.buildAllocation(ext)
    # FIXME: using a static DEPOT list instead of discovering
    # the location service within the allocation abstraction
    o = urisplit(ext.location)
    depot = "{0}://{1}".format(o.scheme, o.authority)
    return alloc.Read(**DEPOTS[depot])
    
def chunked(f, bsize):
    return iter(lambda: f.read(bsize), '')

def DLTUpload(f, bs, rt):
    stat = os.stat(f)
    fh = open(f, 'rb')

    ex = Exnode()
    ex.parent = None
    ex.created = int(time.time())
    ex.modified = ex.created
    ex.mode = "file"
    ex.size = stat.st_size
    ex.permission = format(stat.st_mode & 0o0777, 'o')
    ex.owner = getpass.getuser()
    ex.group = ex.owner
    ex.name = os.path.basename(f)
    
    executor = ThreadPoolExecutor(max_workers=5)
    futures = []
    
    depotiter = cycle(DEPOTS.keys())
    offset = 0
    time_s = time.time()
    for depot in depotiter:
        if (offset+bs) > ex.size:
            bs = ex.size - offset
            
        block = next(chunked(fh, bs))
        if not block:
            break
        
        fut = executor.submit(factory.makeAllocation, offset, block,
                              depot, **DEPOTS[depot])
        futures.append(fut)
        offset = offset + bs
        
    rt.insert(ex, commit=True)
    
    for fut in as_completed(futures):
        ext = fut.result().GetMetadata()
        ext.parent = ex
        rt.insert(ext, commit=True)
        ex.extents.append(ext)

    time_e = time.time()
    
    rt.flush()
    fh.close()
    return (time_e-time_s, ex)
                    
def DLTDownload(f, bs, rt):
    time_s = time.time()
    ex = rt.find(f)
    fh = open(ex.name, "wb")

    # FIXME: This downloads *every* extent, does not consider replication and overlap
    with ThreadPoolExecutor(max_workers=5) as executor:
        for ext, fut in zip(ex.extents, executor.map(read_action, ex.extents)):
            fh.seek(ext.offset)
            fh.write(fut)

    fh.close()
    time_e = time.time()
    return (time_e-time_s, ex)


def main():
    parser = argparse.ArgumentParser(description="DLT File Transfer Tool")
    parser.add_argument('files', metavar='FILES', type=str, nargs='+',
                        help='Files to transfer')
    parser.add_argument('-u', '--upload', action='store_true',
                        help='Perform file upload (default is download)')
    parser.add_argument('-H', '--host', type=str, default=UNIS_URL,
                        help='UNIS instance for uploading eXnode metadata')
    parser.add_argument('-b', '--bs', type=str, default='5m',
                        help='Block size')

    args = parser.parse_args()
    xfer = DLTUpload if args.upload else DLTDownload
    bs = util.human2bytes(args.bs)

    rt = Runtime(args.host, defer_update=True)
    
    for f in args.files:
        diff, res = xfer(f, bs, rt)
        print ("{0} ({1} {2:.2f} MB/s) {3}".format(res.name, res.size,
                                                   res.size/1e6/diff,
                                                   res.selfRef))

if __name__ == "__main__":
    main()
                
