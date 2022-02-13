import logging, threading, queue, socket

from collections import defaultdict
from libdlt.depot import Depot
from libdlt.protocol import factory

log = logging.getLogger('libdlt.utils')
class ExnodeInfo(object):
    def __init__(self, ex, remote_validate=False, accept_timeout=True, threadcount=1):
        class _view(object):
            def __init__(self): self._size, self._chunks = ex.size, [[0,0]]
            def fill(self, o, s):
                self._chunks = list(sorted(self._chunks + [[o,o+s]]))
                new_chunks = [[0,0]]
                for chunk in self._chunks:
                    if chunk[0] <= new_chunks[-1][1]: new_chunks[-1][1] = chunk[1]
                    else: new_chunks.append(chunk)
                self._chunks = new_chunks
            @property
            def is_complete(self):
                return self._chunks[0][-1] == self._size
            @property
            def missing(self):
                result = [[self._chunks[i][1], self._chunks[i+1][0]] for i in range(len(self._chunks) - 1)]
                return result if self._chunks[-1][1] == ex.size else result + [[self._chunks[-1][1], ex.size]]

            def valid(self, offset):
                return any([c[0] <= offset < c[1] for c in self._chunks])

        self._allocs, self._views = [], defaultdict(_view)
        self._tc = threadcount
        self._timeout_ok = accept_timeout
        allocs = sorted(ex.extents, key=lambda x: x.offset)
        self._meta = self._validate(allocs) if remote_validate else defaultdict(lambda: True)
        for e in allocs:
            if self._meta[e.id]:
                try:
                    self._views[e.location].fill(e.offset, e.size)
                    self._allocs.append(e)
                except AttributeError as exp: log.warn("Bad extent - {}".format(e.id))

    @property
    def views(self):
        return self._views.items()

    def _validate(self, allocs):
        results = {}
        def run(x):
            _proxy = factory.makeProxy(x)
            if not hasattr(x, 'depot'): x.depot = Depot(x.location)
            try:
                v = _proxy.probe(x, timeout=0.025)
                results[x.id] = v
            except socket.timeout as e:
                result[x.id] = self._timeout_ok
            except Exception as e:
                log.warn("Failed to connect with allocation - " + x.location)
                results[x.id] = False

        threads = []
        for e in allocs:
            if len(threads) >= self._tc:
                [t.join() for t in threads]
                threads = []
            t = threading.Thread(target=run, args=(e,))
            threads.append(t)
            t.daemon = True
            t.start()
        [t.join() for t in threads]
        return results

    def is_complete(self, view=None):
        if view: return view in self._views and self._views[view].is_complete
        else: return any([v.is_complete for v in self._views.values()])

    def replicas_at(self, offset):
        return [v for v in self._views.values() if v.valid(offset)]

    def missing(self, view):
        return self._views[view].missing

    def allocs_in(self, start, end):
        for alloc in self._allocs:
            if alloc.offset < end and alloc.offset + alloc.size > start:
                yield alloc

    def fill(self, view):
        result, todo = [], self._views[view].missing
        log.debug(f"Fill requirements - {todo}")
        if not todo:
            raise ValueError("No satisfying extents available to fill exnode")
        else:
            for alloc in self._allocs:
                if todo[0][0] >= todo[0][1]: todo.pop(0)
                if not todo: break
                if alloc.offset + alloc.size > todo[0][0]:
                    result.append(alloc)
                    todo[0][0] = alloc.offset + alloc.size
        log.debug(f"Recommnding fill - {[a.id for a in result]}")
        return result

    def plan_download(self, start=0, end=None):
        size = max([v._size for v in self._views.values()])
        end = min(end or size, size)
        for alloc in self._allocs:
            if alloc.offset <= start and alloc.offset + alloc.size > start:
                yield alloc
                start  = alloc.offset + alloc.size
            if start >= end: break

    def alloc_in(self, offset):
        for alloc in self._allocs:
            if alloc.offset <= offset and alloc.offset + alloc.size > offset:
                return alloc

    def __getitem__(self, e):
        return self._meta[e]
