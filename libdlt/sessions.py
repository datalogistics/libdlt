import concurrent.futures
import getpass
import os
import re
import time
import types
import uuid

from itertools import cycle
from concurrent.futures import ThreadPoolExecutor, as_completed
from uritools import urisplit
from socketIO_client import SocketIO

from libdlt.util import util
from libdlt.depot import Depot
from libdlt.logging import getLogger, debug, info
from libdlt.protocol import factory
from libdlt.schedule import BaseDownloadSchedule, BaseUploadSchedule
from libdlt.settings import DEPOT_TYPES, THREADS, COPIES, BLOCKSIZE, TIMEOUT
from unis.models import Exnode, Service
from unis.runtime import Runtime

class Session(object):
    __WS_MTYPE = {
        'r' : 'peri_download_register',
        'c' : 'peri_download_clear',
        'p' : 'peri_download_pushdata'
    }
    
    @debug("Session")
    def __init__(self, url, depots, bs=BLOCKSIZE, timeout=TIMEOUT, **kwargs):
        self._validate_url(url)
        self._runtime = Runtime(url, defer_update=True)
        self._do_flush = True
        self._blocksize = bs if isinstance(bs, int) else int(util.human2bytes(bs))
        self._timeout = timeout
        self._plan = cycle
        self._depots = {}
        self._viz = kwargs.get("viz_url", None)
        self._id = uuid.uuid4().hex  # use if we're matching a webGUI session
        self.log = getLogger()

        if self._viz:
            try:
                o = urisplit(self._viz)
                self._sock = SocketIO(o.host, o.port)
            except Exception as e:
                self.log.warn("Session.__init__: websocket connection failed: {}".format(e))
                # non-fatal
        
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
            raise ValueError("depots argument must contain a list of depot description objects or a valid unis url")

        if not len(self._depots):
            raise ValueError("No depots found for session, unable to continue")
    
    @debug("Session")
    def _viz_register(self, name, size, conns):
        if self._viz:
            try:
                msg = {"sessionId": self._id,
                       "filename": name,
                       "size": size,
                       "connections": conns,
                       "timestamp": time.time()*1e3
                   }
                self._sock.emit(self.__WS_MTYPE['r'], msg)
            except Exception as e:
                pass
            
    @debug("Session")
    def _viz_progress(self, depot, size, offset):
        if self._viz:
            try:
                d = Depot(depot)
                msg = {"sessionId": self._id,
                       "host":  d.host,
                       "length": size,
                       "offset": offset,
                       "timestamp": time.time()*1e3
                   }
                self._sock.emit(self.__WS_MTYPE['p'], msg)
            except Exception as e:
                pass
        
    #### TODO ####
    #  Upload assumes sucessful push to all depots
    #  Needs better success/failure metrics
    ##############
    @info("Session")
    def upload(self, filepath, folder=None, copies=COPIES, duration=None, schedule=BaseUploadSchedule()):
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
        ex = Exnode({ "parent": folder, "created": int(time.time() * 1000000), "mode": "file", "size": stat.st_size,
                      "permission": format(stat.st_mode & 0o0777, 'o'), "owner": getpass.getuser(),
                      "name": os.path.basename(filepath) })
        ex.group = ex.owner
        ex.updated = ex.created
        self._runtime.insert(ex, commit=True)
        
        # register download with Periscope
        self._viz_register(ex.name, ex.size, len(self._depots))
        
        executor = ThreadPoolExecutor(max_workers=THREADS)
        futures = []
        time_s = time.time()
        
        schedule.setSource(self._depots)
        with open(filepath, "rb") as fh:
            for offset, size, data in _chunked(fh, self._blocksize, ex.size):
                for n in range(copies):
                    d = Depot(schedule.get({"offset": offset, "size": size, "data": data}))
                    futures.append(executor.submit(factory.makeAllocation, data, offset, d, duration=duration,
                                                   **self._depots[d.endpoint].to_JSON()))
                    
        for future in as_completed(futures):
            ext = future.result().getMetadata()
            self._viz_progress(ext.location, ext.size, ext.offset)
            self._runtime.insert(ext, commit=True)
            ext.parent = ex
            ex.extents.append(ext)
            
        time_e = time.time()
            
        if self._do_flush:
            self._runtime.flush()
        return (time_e - time_s, ex)
    
    @info("Session")
    def _dl_generator(self, executor, schedule, ex):
        def _download_chunk(ext):
            try:
                alloc = factory.buildAllocation(ext)
                d = Depot(ext.location)
                return ext, alloc.read(**self._depots[d.endpoint].to_JSON())
            except Exception as exp:
                print ("READ Error: {}".format(exp))
            return ext, False
        
        in_flight = []
        pending = []
        current = 0
        
        # Begin first THREADS requests
        for _ in range(THREADS):
            alloc = schedule.get({ "offset": current })
            if alloc:
                in_flight.append(executor.submit(_download_chunk, alloc))
                current += alloc.size
            
        # If there is remaining file to download
        if current < ex.size:
            pending.append((current, ex.size))
            
        # Wait for the first result
        done, in_flight = concurrent.futures.wait(in_flight, return_when=concurrent.futures.FIRST_COMPLETED)
        while done:
            in_flight = list(in_flight)
            for response in done:
                alloc, data = response.result()
                
                # If download was successful
                if data:
                    yield (alloc, data)
                else:
                    # Return the request to the pending list
                    pending.append((alloc.offset, alloc.size + alloc.offset))
                        
            if pending:
                segment = pending.pop()
                alloc = schedule.get({ "offset": segment[0] })
                end = alloc.offset + alloc.size
                if end < segment[1]:
                    pending.append((end, segment[1]))
                    in_flight.append(executor.submit(_download_chunk, alloc))
            done, in_flight = concurrent.futures.wait(in_flight, return_when=concurrent.futures.FIRST_COMPLETED)
    
    @info("Session")
    def download(self, href, filepath, length=0, offset=0, schedule=BaseDownloadSchedule()):
        self._validate_url(href)
        ex = self._runtime.find(href)
        allocs = ex.extents
        schedule.setSource(allocs)
        locs = {}
        
        # bin extents and locations
        for alloc in allocs:
            if alloc.location not in locs:
                locs[alloc.location] = []
            locs[alloc.location].append(alloc)
        
        if not filepath:
            filepath = ex.name
        
        # register download with Periscope
        self._viz_register(ex.name, ex.size, len(locs))
        
        time_s = time.time()
        with open(filepath, "wb") as fh:
            with ThreadPoolExecutor(max_workers=THREADS) as executor:
                for alloc, data in self._dl_generator(executor, schedule, ex):
                    self._viz_progress(alloc.location, alloc.size, alloc.offset)
                    fh.seek(alloc.offset)
                    fh.write(data)
        
        return (time.time() - time_s, ex)
        
    @info("Session")
    def copy(self, href, duration=None, download_schedule=BaseDownloadSchedule(), upload_schedule=BaseUploadSchedule()):
        self._validate_url(href)
        ex = self._runtime.find(href)
        allocs = ex.extents
        futures = []
        download_schedule.setSource(allocs)
        upload_schedule.setSource(self._depots)
        
        time_s = time.time()
        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            for alloc, data in self._dl_generator(executor, download_schedule, ex):
                d = upload_schedule.get({"offset": alloc.offset, "size": alloc.size, "data": data})
                futures.append(executor.submit(factory.makeAllocation, data, alloc.offset, duration, d,
                                               **self._depots[d.endpoint].to_JSON()))
                
        for future in as_completed(futures):
            alloc = future.result().getMetadata()
            self._runtime.insert(alloc, commit=True)
            alloc.parent = ex
            ex.extents.append(alloc)
            
        time_e = time.time()
        
        if self._do_flush:
            self._runtime.flush()
        return (time_e - time_s, ex)
        
    @info("Session")
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
            path, root = _traverse(path[1:], folder_ls[0])
        else:
            root = None
        
        for folder in path:
            owner = getpass.getuser()
            now = int(time.time() * 1000000)
            new_folder = Exnode({"name": folder, 
                                 "parent": root, 
                                 "owner": owner, 
                                 "group": owner, 
                                 "created": now, 
                                 "updated": now, 
                                 "children": [], 
                                 "permission": format(0o0755, 'o'), 
                                 "mode": "directory"})
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
    
    @debug("Session")
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
            
    def __enter__(self):
        pass

    def __exit__(self):
        self._runtime.shutdown()
