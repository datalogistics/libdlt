import copy, socket, time

from libdlt.util.files import ExnodeInfo
from libdlt.depot import Depot
from libdlt.protocol import factory, exceptions
from lace import logging

class FileError(OSError):
    pass

log = logging.getLogger("libdlt.file")
class DLTFile(object):
    def __init__(self, ex):
        self._ex, self._head = ex, 0
        self._chunk = None

    def fileno(self): return 3

    def seek(self, offset, whence=0):
        if whence == 0: self._head = offset
        elif whence == 1: self._head += offset
        else: self._head = self._ex.size - offset
        self._find_chunk()

    def read(self, size=-1):
        def _get():
            log.debug(f"Data no cached, pulling block @{self._head}")
            for _ in range(3):
                for a in self._ex.extents:
                    if a.offset <= self._head and a.offset + a.size > self._head:
                        try:
                            self._chunk = (a, factory.makeProxy(a).load(a, timeout=0.1))
                            log.debug(f"    Found matching block {a.offset}-{a.offset+a.size}")
                            return
                        except socket.timeout: pass
                time.sleep(0.1)
            raise IOError("Incomplete file, no allocations satisfy request")

        if self._head >= self._ex.size: return bytes()
        if self._chunk is None or self._head < self._chunk[0].offset or self._head >= self._chunk[0].offset + self._chunk[0].size:
            _get()
        s = self._head - self._chunk[0].offset
        if size == -1: size = self._chunk[0].size - s
        log.debug(f"<-- Read {self._chunk[0].offset + s}-{self._chunk[0].offset + size}")
        self._head = self._chunk[0].offset + size
        return bytes(self._chunk[1][s:size])

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
