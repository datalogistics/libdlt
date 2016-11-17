import asyncio
import concurrent.futures
import getpass
import os
import re
import time
import types
import uuid
import logging

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
    
    __static_ips = {
        '149.165.232.115': 'mon01.crest.iu.edu',
        '10.10.1.1': 'mon1.apt.emulab.net'
    }
    
    @debug("Session")
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
        self._jobs = asyncio.Queue()
        self._loop = asyncio.get_event_loop()
        self.log = getLogger()
        
        self._loop.set_default_executor(ThreadPoolExecutor(threads))
        
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
                self.log.warn("Session.__init__: websocket connection failed: {}".format(e))
        return None
            
    @debug("Session")
    def _viz_progress(self, sock, depot, size, offset):
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
        

    @debug("Session")
    async def _generate_jobs(self, iterator, *args):
        async for info in iterator(*args):
            await self._jobs.put(info)
        await self._jobs.put(None)
        return []
    
    @debug("Session")
    async def _upload_chunks(self, schedule, duration, sock):
        job = await self._jobs.get()
        allocs = []
        while job:
            offset, data = job
            
            ## Upload chunk ##
            d = Depot(schedule.get({"offset": offset, "size": len(data), "data": data}))
            alloc = await factory.makeAllocation(data, offset, d, duration=duration, **self._depots[d.endpoint].to_JSON(), loop=self._loop)
            
            ## Create Allocation ##
            alloc = alloc.getMetadata()
            self._viz_progress(sock, alloc.location, alloc.size, alloc.offset)
            allocs.append(alloc)
            job = await self._jobs.get()
            
        
        await self._jobs.put(None)
        return allocs
        
    @info("Session")
    def upload(self, filepath, folder=None, copies=COPIES, duration=None, schedule=BaseUploadSchedule()):
        #self._loop.set_debug(True)
        #logging.getLogger('asyncio').setLevel(logging.DEBUG)
        ## Read File ##
        class aoifile_iter:
            def __init__(it, fh):
                it._fh = fh
                it._offset = 0
            async def __aiter__(it):
                return it
            async def __anext__(it):
                data = await self._loop.run_in_executor(None, it._fh.read, self._blocksize)
                if not data:
                    raise StopAsyncIteration
                offset = it._offset
                it._offset += len(data)
                return (offset, data)
        
        ## Create Folder ##
        if isinstance(folder, str):
            do_flush = self._do_flush
            self._do_flush = False
            folder = self.mkdir(folder)
            self._do_flush = do_flush
            
        ## Setup ##
        stat = os.stat(filepath)
        ex = Exnode({ "parent": folder, "created": int(time.time() * 1000000), "mode": "file", "size": stat.st_size,
                      "permission": format(stat.st_mode & 0o0777, 'o'), "owner": getpass.getuser(),
                      "name": os.path.basename(filepath) })
        ex.group = ex.owner
        ex.updated = ex.created
        sock = self._viz_register(ex.name, ex.size, len(self._depots))
        schedule.setSource(self._depots)
        ## Generate tasks ##
        workers = []
        time_s = time.time()
        workers = [asyncio.ensure_future(self._upload_chunks(schedule, duration, sock), loop=self._loop) for _ in range(self._threads)]
        with open(filepath, 'rb') as fh:
            workers.append(asyncio.ensure_future(self._generate_jobs(aoifile_iter, fh), loop=self._loop))
            done, pending = self._loop.run_until_complete(asyncio.wait(workers))
        
        time_e = time.time()
        
        ## Generate Exnode ##
        self._runtime.insert(ex, commit=True)
        for task in done:
            for alloc in task.result():
                alloc.parent = ex
                ex.extents.append(alloc)
                self._runtime.insert(alloc, commit=True)
        
        if self._do_flush:
            self._runtime.flush()
        return (time_e - time_s, ex)
    
    
    @debug("Session")
    async def _download_chunks(self, filepath, schedule, sock):
        def _write(fh, offset, data):
            fh.seek(offset)
            return self._loop.run_in_executor(None, fh.write, data)
        fh = open(filepath, 'wb')
        while not self._jobs.empty():
            offset, end = await self._jobs.get_nowait()
            alloc = schedule.get({"offset": offset})
            if not alloc:
                fh.close()
                return
            if alloc.offset + alloc.size < end:
                await self._jobs.put((offset + alloc.size, end))
            
            ## Download chunk ##
            d = Depot(alloc.location)
            service = factory.buildAllocation(alloc)
            data = await service.read(self._loop, **self._depots[d.endpoint].to_JSON())
            self._viz_progress(sock, alloc.location, alloc.size, alloc.offset)
            fh.seek(alloc.offset)
            data = await _write(fh, alloc.offset, data)
        
        await self._jobs.put(None)
        fh.close()
        
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
        sock = self._viz_register(ex.name, ex.size, len(locs))
        
        time_s = time.time()
        self._jobs.put_nowait((0, ex.size))
        workers = [asyncio.ensure_future(self._download_chunks(filepath, schedule, sock), loop=self._loop) for _ in range(self._threads)]
        done, pending = self._loop.run_until_complete(asyncio.wait(workers))
        
        return (time.time() - time_s, ex)
        
    @info("Session")
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
