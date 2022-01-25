from libdlt.util.files import ExnodeInfo
from libdlt.depot import Depot
from libdlt.protocol import factory, exceptions

class FileError(OSError):
    pass

class DLTFile(object):
    def __init__(self, ex):
        self._ex, self._h = ex, 0
        self.info = ExnodeInfo(ex, remote_validate=True)

    def _get_chunk(self, alloc):
        proxy = factory.makeProxy(alloc)
        if not hasattr(alloc, 'depot'): alloc.depot = Depot(alloc.location)
        try: return proxy.load(alloc)
        except exceptions.AllocationError as e:
            log.warn("Failed to connect with allocation - " + x.location)
            raise FileError("Allocation failed to load")

    def fileno(self): return self._ex

    def seek(self, offset, whence=0):
        if whence == 0: self._h = offset
        elif whence == 1: self._h += offset
        else: self._h = self._ex.size - offset

    def read(self, size=-1):
        size = size if isinstance(size, int) and size > 0 else self._ex.size - self._h
        tail = 0
        data = bytearray(size)
        for alloc in self.info.plan_download(self._h, self._h + size):
            d = self._get_chunk(alloc)
            s = alloc.offset - self._h
            if s < 0:
                d,s = d[0-s:], 0
            e = s+len(d)
            data[s:e] = d
            tail = max(tail, e)
        _read = min(tail, size)
        self._h += _read
        return data[:_read]
