import concurrent.futures
import getpass
import os
import re
import time
import types
import uuid

from contextlib import contextmanager
from itertools import cycle
from lace import logging
from lace.logging import trace
from concurrent.futures import ThreadPoolExecutor, as_completed
from uritools import urisplit
from socketIO_client import SocketIO

from libdlt.util import util
from libdlt.depot import Depot
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
    
    __static_ips = {
        '149.165.232.115': 'mon01.crest.iu.edu',
        '10.10.1.1': 'mon1.apt.emulab.net'
    }
    
    @trace.debug("Session")
    def __init__(self, url, depots, bs=BLOCKSIZE, timeout=TIMEOUT, threads=THREADS, **kwargs):
        self._validate_url(url)
        self._runtime = Runtime(url, defer_update=True, auto_sync=False, subscribe=False, inline=True)
        self._runtime.exnodes.createIndex("name")
        self._do_flush = True
        self._blocksize = bs if isinstance(bs, int) else int(util.human2bytes(bs))
        self._timeout = timeout
        self._plan = cycle
        self._depots = {}
        self._threads = threads
        self._viz = kwargs.get("viz_url", None)
        self.log = logging.getLogger()
        
        if not depots:
            for depot in self._runtime.services.where(lambda x: x.serviceType in DEPOT_TYPES):
                self._depots[depot.selfRef] = depot
        elif isinstance(depots, str):
            self._validate_url(depots)
            with Runtime(depots, auto_sync=False, subscribe=False) as rt:
                for depot in rt.services.where(lambda x: x.serviceType in DEPOT_TYPES):
                    self._depots[depot.selfRef] = depot
        elif isinstance(depots, dict):
            for name, depot in depots.items():
                if isinstance(depot, dict) and depot["enabled"]:
                    self._depots[name] = Service(depot)
        else:
            raise ValueError("depots argument must contain a list of depot description objects or a valid unis url")

        if not len(self._depots):
            raise ValueError("No depots found for session, unable to continue")
    
    @trace.debug("Session")
    def _viz_register(self, name, size, conns, cb):
        if cb:
            cb(None, name, size, 0, 0)
        if self._viz:
            try:
                uid = uuid.uuid4().hex
                o = urisplit(self._viz)
                sock = SocketIO(o.host, o.port)
                msg = {"sessionId": uid,
                       "filename": name,
                       "size": size,
                       "connections": conns,
                       "timestamp": time.time()*1e3
                   }
                sock.emit(self.__WS_MTYPE['r'], msg)
                return uid, sock
            except Exception as e:
                self.log.warn(e)
        return None
            
    @trace.debug("Session")
    def _viz_progress(self, sock, name, tsize, depot, size, offset, cb):
        if cb:
            cb(depot, name, tsize, size, offset)
        if self._viz:
            try:
                d = Depot(depot)
                host = str(d.host)
                if host in self.__static_ips:
                    host = self.__static_ips[host]
                msg = {"sessionId": sock[0],
                       "host":  host,
                       "length": size,
                       "offset": offset,
                       "timestamp": time.time()*1e3
                   }
                sock[1].emit(self.__WS_MTYPE['p'], msg)
            except Exception as e:
                pass
        
    #### TODO ####
    #  Upload assumes sucessful push to all depots
    #  Needs better success/failure metrics
    ##############
    @trace.info("Session")
    def upload(self, filepath, folder=None, copies=COPIES, duration=None, schedule=BaseUploadSchedule(), progress_cb=None):
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
        sock = self._viz_register(ex.name, ex.size, len(self._depots), progress_cb)
        
        executor = ThreadPoolExecutor(max_workers=self._threads)
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
            self._viz_progress(sock, ex.name, ex.size, ext.location, ext.size, ext.offset, progress_cb)
            self._runtime.insert(ext, commit=True)
            ext.parent = ex
            ex.extents.append(ext)
            
        time_e = time.time()
            
        if self._do_flush:
            self._runtime.flush()
        return (time_e - time_s, ex.size, ex)
    
    @trace.info("Session")
    def download(self, href, filepath=None, length=0, offset=0, schedule=BaseDownloadSchedule(), progress_cb=None):
        def offsets(size):
            i = 0
            while i < size:
                ext = schedule.get({"offset": i})
                yield ext
                i += ext.size
        def _download_chunk(ext):
            try:
                alloc = factory.buildAllocation(ext)
                d = Depot(ext.location)
                if d.endpoint not in self._depots:
                    raise Exception("Unkown depot {}".format(d.endpoint))
                return ext, alloc.read(**self._depots[d.endpoint].to_JSON())
            except Exception as exp:
                self.log.warn(exp)
            return ext, False
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
        sock = self._viz_register(ex.name, ex.size, len(locs), progress_cb)
        
        time_s = time.time()
        dsize = 0
        with open(filepath, "wb") as fh:
            with ThreadPoolExecutor(max_workers=self._threads) as executor:
                for alloc, data in executor.map(_download_chunk, offsets(ex.size)):
                    if not data:
                        continue
                    dsize += alloc.size
                    self._viz_progress(sock, ex.name, ex.size, alloc.location, alloc.size, alloc.offset, progress_cb)
                    fh.seek(alloc.offset)
                    fh.write(data)
        
        return (time.time() - time_s, dsize, ex)
        
    @trace.info("Session")
    def copy(self, href, duration=None, download_schedule=BaseDownloadSchedule(), upload_schedule=BaseUploadSchedule()):
        def offsets(size):
            i = 0
            while i < size:
                ext = download_schedule.get({"offset": i})
                yield ext
                i += ext.size
        def _copy_chunk(sock_down, sock_up):
            def _f(ext):
                try:
                    alloc = factory.buildAllocation(ext)
                    src_desc = Depot(ext.location)
                    dest_desc = Depot(upload_schedule.get({"offset": ext.offset, "size": ext.size}))
                    src_depot = self._depots[src_desc.endpoint]
                    dest_depot = self._depots[dest_desc.endpoint]
                    dst_alloc = alloc.copy(dest_desc, src_depot.to_JSON(), dest_depot.to_JSON())
                    dst_ext = dst_alloc.getMetadata()
                    self._viz_progress(sock_down, ext.location, ext.size, ext.offset)
                    self._viz_progress(sock_up, dst_ext.location, dst_ext.size, dst_ext.offset)
                    return (ext, dst_ext)
                except Exception as exp:
                    print ("READ Error: {}".format(exp))
                return ext, False
            return _f
                
        self._validate_url(href)
        ex = self._runtime.find(href)
        allocs = ex.extents
        futures = []
        download_schedule.setSource(allocs)
        upload_schedule.setSource(self._depots)
        
        sock_up = self._viz_register("{}_upload".format(ex.name), ex.size, len(self._depots))
        sock_down = self._viz_register("{}_download".format(ex.name), ex.size, len(self._depots))
        time_s = time.time()
        with ThreadPoolExecutor(max_workers=self._threads) as executor:
            for src_alloc, dst_alloc  in executor.map(_copy_chunk(sock_down, sock_up), offsets(ex.size)):
                alloc = dst_alloc
                self._runtime.insert(alloc, commit=True)
                alloc.parent = ex
                ex.extents.append(alloc)
        
        time_e = time.time()
        
        if self._do_flush:
            self._runtime.flush()
        return (time_e - time_s, ex)
        
    @trace.info("Session")
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
    
    @trace.info("Session")
    @contextmanager
    def annotate(self, ex):
        store = ex.isAutoCommit()
        ex.setAutoCommit(True)
        yield ex
        ex.setAutoCommit(store)
        self._runtime.flush()
    
    @trace.debug("Session")
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
