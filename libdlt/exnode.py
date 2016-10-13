import os
import time
import getpass
from uritools import urisplit
from itertools import cycle
from concurrent.futures import ThreadPoolExecutor, as_completed

from unis.models import Exnode
from .protocol import factory

DEF_BS     = 262144
DEF_COPIES = 1

def read_action(ext, depots):
    alloc = factory.buildAllocation(ext)
    # FIXME: using a static DEPOT list instead of discovering
    # the location service within the allocation abstraction
    o = urisplit(ext.location)
    d = "{0}://{1}".format(o.scheme, o.authority)
    return alloc.Read(**depots[d])
    
def chunked(f, bsize):
    return iter(lambda: f.read(bsize), '')

def upload(rt, f, folder=None, bs=DEF_BS, copies=DEF_COPIES, depots=None):
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
    
    depotiter = cycle(depots.keys())
    offset = 0
    time_s = time.time()
    for d in depotiter:
        if (offset+bs) > ex.size:
            bs = ex.size - offset
            
        block = next(chunked(fh, bs))
        if not block:
            break
    
        fut = executor.submit(factory.makeAllocation, offset,
                              block, d, **depots[d])
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
                    
def download(rt, f, bs, depots=None):
    time_s = time.time()
    ex = rt.find(f)
    fh = open(ex.name, "wb")

    # FIXME: This downloads *every* extent, does not consider replication and overlap
    with ThreadPoolExecutor(max_workers=5) as executor:
        for ext, fut in zip(ex.extents,
                            executor.map(lambda x: read_action(x, depots),
                                         ex.extents)):
            fh.seek(ext.offset)
            fh.write(fut)

    fh.close()
    time_e = time.time()
    return (time_e-time_s, ex)
