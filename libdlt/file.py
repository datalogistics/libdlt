import copy, socket, time

from libdlt.util.files import ExnodeInfo
from libdlt.depot import Depot
from libdlt.protocol import factory, exceptions
from libdlt.settings import BLOCKSIZE
from lace import logging

class FileError(OSError):
    pass

log = logging.getLogger("libdlt.file")
class DLTFile(object):
    def __init__(self, ex, mode="r", *, dest=None, bs=BLOCKSIZE):
        self._bs, self._ex = bs, ex
        self._chunk = None if "r" in mode else bytearray(bs)
        self._offset = self._head = 0 if "a" not in mode else ex.size
        self.t, self._mode = None, mode
        if dest:
            self._dest, self._proxy = Depot(dest), factory.makeProxyFromURI(dest)

    def fileno(self): return 3

    def seek(self, offset, whence=0):
        if whence == 0: self._head = offset
        elif whence == 1: self._head += offset
        else: self._head = self._ex.size - offset
        self._find_chunk()

    def settimeout(self, timeout):
        self.t = timeout

    def read(self, size=-1):
        def _get():
            log.debug(f"Data no cached, pulling block @{self._head}")
            for _ in range(3):
                for a in self._ex.extents:
                    if a.offset <= self._head and a.offset + a.size > self._head:
                        try:
                            self._chunk = (a, factory.makeProxy(a).load(a, timeout=self.t))
                            log.debug(f"   Found matching block {a.offset}-{a.offset+a.size}")
                            return
                        except socket.timeout: pass
                time.sleep(0.1)
            raise IOError("Incomplete file, no allocations satisfy request")

        if self._head >= self._ex.size: return bytes()
        if self._chunk is None or self._head < self._chunk[0].offset or self._head >= self._chunk[0].offset + self._chunk[0].size:
            _get()
        s = self._head - self._chunk[0].offset
        if size == -1: size = self._chunk[0].size - s
        else: size = min(self._chunk[0].size - s, size)
        log.debug(f"<-- Read {self._head}-{self._head + size}")
        self._head = self._chunk[0].offset + s + size
        return bytes(self._chunk[1][s:s+size])

    def write(self, data):
        log.debug(f"Writing {len(data)} bytes to {self._dest.host}:{self._dest.port}")

        wrote = 0
        while len(data) > 0:
            # Copy data into chunk
            #  Calculate start and end of copy
            s = self._offset
            size = min(self._bs - s, len(data))
            wrote, e = wrote + size, size + s
            #  Copy to chunk and remember remainder
            self._chunk[s:e], data = data[:size], data[size:]
            self._offset += size

            # if block is full
            if self._offset >= self._bs:
                self._offset = 0
                # Store data in staging
                alloc = self._proxy.allocate(self._dest, 0, len(self._chunk), timeout=self.t)
                log.debug(f"Attempting to stage {self._head}-{self._head+len(self._chunk)}")
                try: self._proxy.store(alloc, self._chunk, len(self._chunk), timeout=self.t)
                except (socket.timeout, exceptions.AllocationError) as e:
                    err = f"Unable to stage {self._head}-{self._head+len(self._chunk)}"
                    raise OSError(err) from e
                alloc.parent, alloc.offset = self._ex, self._head
                try: del alloc.getObject().__dict__['function']
                except KeyError: pass
                self._ex.extents.append(alloc)
                self._head += alloc.size
                self._ex.size += alloc.size
        return wrote

    def close(self):
        if "w" in self._mode and self._offset != 0:
            o = self._offset
            alloc = self._proxy.allocate(self._dest, 0, o, timeout=self.t)
            log.debug(f"Attempting to stage {self._head}-{self._head+o}")
            try:
                self._proxy.store(alloc, self._chunk[:o], o, timeout=self.t)
            except (socket.timeout, exceptions.AllocationError) as e:
                err = f"Unable to stage {self._head}-{self._head+o}"
                raise OSError(err) from e
            alloc.parent, alloc.offset = self._ex, self._head
            try: del alloc.getObject().__dict__['function']
            except KeyError: pass
            self._ex.extents.append(alloc)
            self._head += alloc.size
            self._ex.size += alloc.size

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
"""
class DLTFile(object):
    def __init__(self, ex):
        self._ex, self._h = ex, 0
        self.info = ExnodeInfo(ex, remote_validate=True)
        self._view, self._viewframe = bytearray(), (0,0)

    def _get_chunk(self, alloc):
        print(f"Getting allocation: {alloc.offset}-{alloc.offset+alloc.size}")
        proxy = factory.makeProxy(alloc)
        if not hasattr(alloc, 'depot'): alloc.depot = Depot(alloc.location)
        try: return proxy.load(alloc)
        except exceptions.AllocationError as e:
            log.warn("Failed to connect with allocation - " + alloc.location)
            raise FileError("Allocation failed to load")

    def fileno(self): return 3
    
    def seek(self, offset, whence=0):
        if whence == 0: self._h = offset
        elif whence == 1: self._h += offset
        else: self._h = self._ex.size - offset
        self._view, self._viewframe = bytearray(), (self._h, self._h)

    def read(self, size=-1):
        def _stage(start):
            alloc = self.info.alloc_in(start)
            if not alloc: return False
            self._view = self._get_chunk(alloc)
            self._viewframe = (alloc.offset, alloc.offset + alloc.size)
            return True
        size = size if isinstance(size, int) and size > 0 else self._ex.size - self._h
        print(f"Reading from: {size}")
        end, tail = self._h + size, 0
        data = bytearray(size)
        if self._h >= self._viewframe[1]:
            if not _stage(self._h):
                return bytes()
        _s = self._h - self._viewframe[0]
        if end > self._viewframe[1]:
            _e = self._viewframe[1] - _s
            data[:_e] = self._view[_s:]
            if not _stage(self._viewframe[1]):
                self._h = self._viewframe[1]
                return bytes(data[:_e])
            data[_e:] = self._view[:size-_e]
        else:
            data = self._view[_s:_s+size]
        self._h += len(data)
        return bytes(data)
"""
