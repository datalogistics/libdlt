
import getpass
import os
import re
import time
import types

from itertools import cycle
from concurrent.futures import ThreadPoolExecutor, as_completed

from libdlt.protocol import factory
from libdlt.settings import DEPOT_TYPES, THREADS, COPIES, BLOCKSIZE, TIMEOUT
from unis.models import Exnode, Service
from unis.runtime import Runtime


class Session(object):
    def __init__(self, url, depots, bs=BLOCKSIZE, timeout=TIMEOUT, **kwargs):
        self._validate_url(url)
        self._runtime = Runtime(url, defer_update=True, **kwargs)
        self._do_flush = True
        self._blocksize = bs
        self._timeout = timeout
        self._plan = cycle
        self._depots = {}
        
        if not depots:
            for depot in self._runtime.services.where(lambda x: x.serviceType in DEPOT_TYPES):
                self._depots[depot.selfRef] = depot
        elif isinstance(depots, str):
            self._validate_url(depots)
            with Runtime(depots) as rt:
                for depot in rt.services.where(lambda x: x.serviceType in DEPOT_TYPES):
                    self._depots[depot.selfRef] = depot
        elif isinstance(depots, dict):
            for name, depot in depots.items():
                if isinstance(depot, dict):
                    self._depots[name] = Service(depot)
        else:
            ValueError("depots must contain a list of depot description objects or a valid unis url")
        if not self._depots:
            ValueError("no depots found for session, unable to continue")
            
    
    def upload(self, filepath, folder=None, copies=COPIES, duration=None):
        def _chunked(fh, bs, size):
            offset = 0
            while True:
                bs = min(bs, size - offset)
                data = fh.read(bs)
                if not data:
                    return
                yield (offset, bs, data)
                offset += bs
            
            
        if isinstance(folder, str):
            do_flush = self._do_flush
            self._do_flush = False
            folder = self.mkdir(folder)
            self._do_flush = do_flush
        
        stat = os.stat(filepath)
        ex = Exnode({ "parent": folder, "created": int(time.time()), "mode": "file", "size": stat.st_size,
                      "permission": format(stat.st_mode & 0o0777, 'o'), "owner": getpass.getuser(),
                      "name": os.path.basename(filepath) })
        ex.group = ex.owner
        ex.updated = ex.created
        self._runtime.insert(ex, commit=True)
        
        executor = ThreadPoolExecutor(max_workers=THREADS)
        futures = []
        time_s = time.time()
        
        depots = self._get_plan(self._depots):
        with open(filepath) as fh:
            for offset, size, data in _chunked(fh, bs, ex.size):
                for n in range(copies):
                    futures.append(executor.submit(factory.makeAllocation, offset, data, next(depots)))
                    
        for future in as_completed(futures):
            ext = future.result().GetMetadata()
            rt.insert(ext, commit=True)
            ex.extents.append(ext)
            
        if self._do_flush:
            rt.flush()
        return (time.time() - time_e, ex)
    
    def download(self, href, filepath, length=0, offset=0):
        def _download_chunk(ls):
            for ext in ls:
                try:
                    alloc = factory.buildAllocation(ext)
                    depot = self._depots[Depot(ext.location).endpoint]
                    return alloc.Read(**depots[depot].to_JSON())
                except Exception as exp:
                    pass
            return None

        self._validate_url(url)
        time_s = time.time()
        ex = self._runtime.find(href)
        exts = ex.extents
        chunks = {}
        
        # bin extents
        for ext in exts:
            if ext.offset in chunks:
                chunks[ext.offset].append(ext)
            else:
                chunks[ext.offset] = [ext]
        
        
        with open(filepath) as fh:
            with ThreadPoolExecutor(max_workers=THREADS) as executor:
                for ext, data in zip(exts, executor.map(lambda x: _download_chunk(x), chunks)):
                    fh.seek(ext.offset)
                    fh.write(data)
        
        return (time.time() - time_s, ex)
        
        
    def mkdir(self, path):
        def _traverse(ls, obj):
            if not ls:
                return ([], obj)
            for child in obj.children:
                if child.name == ls[0]:
                    return _traverse(ls[1:], child)
            return (ls, obj)
        
        path = list(filter(None, path.split('/')))
        if not path:
            return
        
        folder_ls = list(self._runtime.exnodes.where({"name": path[0], "mode": "directory", "parent": None}))
        if folder_ls:
            path, root = _traverse(path, folder_ls[0])
        else:
            root = None
        
        for folder in path:
            new_folder = Exnode({"name": folder, "parent": root, "mode": "directory"})
            self._runtime.insert(new_folder, commit=True)
            if root:
                if not hasattr(root, "children"):
                    root.children = [new_folder]
                    root.commit("children")
                else:
                    root.children.append(new_folder)
                    
            root = new_folder
        
        if self._do_flush:
            self._runtime.flush()
        
        return root
    
    def setDistributionPlan(self, plan):
        assert isinstance(plan, types.FunctionType), "Plan is not a function"
        self._plan = plan
        
    def _validate_url(self, url):
        regex = re.compile(
            r'^(?:http)s?://'
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'
            r'localhost|'
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
            r'(?::\d+)?'
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        
        if not regex.match(url):
            raise ValueError("invalid url - {u}".format(u=url))
            
    def _get_plan(self, depots):
        return self._plan(depots)
