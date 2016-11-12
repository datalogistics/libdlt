import abc
from itertools import cycle

from libdlt.logging import info

class AbstractSchedule(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def setSource(self, source):
        """
        Creates the schedule, source must be provided
        as the list of possible schedule slots.
        """
        pass

    @abc.abstractmethod
    def get(self, context):
        """
        get emits the next value at offset.  Offset
        need not be factor in emission order, such
        as in the default upload case, but may be
        used to request a new attempt at a previous
        offset.
        """
        pass
    

class BaseUploadSchedule(AbstractSchedule):
    def setSource(self, source):
        self._ls = cycle(source)

    def get(self, context={}):
        return next(self._ls)

class BaseDownloadSchedule(AbstractSchedule):
    @info("BaseDownloadSchedule")
    def setSource(self, source):
        chunks = {}
        for ext in source:
            if ext.offset not in chunks:
                chunks[ext.offset] = []
            chunks[ext.offset].append(ext)
        self._ls = chunks
        
    @info("BaseDownloadSchedule")
    def get(self, context={}):
        offset = context["offset"]
        if offset in self._ls and self._ls[offset]:
            return self._ls[offset].pop()
        else:
            result = None
            for k, chunk in self._ls.items():
                if k < offset:
                    for ext in chunk:
                        if ext.size + ext.offset > offset:
                            result = ext
                            break
                    if result:
                        self._ls[k].remove(result)
                        break
            
            if not result:
                print ("No more allocations fulfill request: offset ~ {}".format(offset))
                #raise IndexError("No more allocations fulfill request: offset ~ {}".format(offset))
            return result
