import libdlt
from .util import util
from .exnode import DEF_BS, DEF_COPIES

class Session:
    def __init__(self, runtime, blocksize=DEF_BS, copies=DEF_COPIES, depots=None):
        self._runtime = runtime
        self._bs = util.human2bytes(blocksize)
        self._depots = depots
        self._copies = copies
        
    def upload(self, fh, folder=None):
        return libdlt.upload(self._runtime,
                             fh,
                             folder,
                             self._bs,
                             self._copies,
                             self._depots)
    
    def download(self, url):
        return libdlt.download(self._runtime,
                               url,
                               self._bs,
                               self._depots)
