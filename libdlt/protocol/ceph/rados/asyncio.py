import asyncio

import rados.core as rados
from rados.core import Operation, ReadOperation, WriteOperation

class Ioctx:
    pass

class Cluster(rados.Cluster):
    def __init__(self, rados_id=None, name=None, clustername=None,
                 conf_defaults=None, conffile=None, conf=None, flags=0):
        super().__init__(rados_id, name, clustername, conf_defaults, conffile, conf, flags)

    def open_aioctx(rados, pool_name: str, loop=None):
        ioctx = rados._open_ioctx_raw(pool_name)
        return Ioctx(pool_name, rados.librados, ioctx, loop=loop)

class _Completion(rados.Completion):
    def __init__(self, ioctx: Ioctx, loop, wait_on_safe=False):
        """
        :param safe: can't be True for read operation
        """
        self.loop = loop
        self.future = asyncio.Future(loop=loop)
        if wait_on_safe:
            super().__init__(ioctx, onsafe=self.__done)
        else:
            super().__init__(ioctx, oncomplete=self.__done)

    def __done(self, _):
        self.loop.call_soon_threadsafe(self.future.set_result, True)

    @asyncio.coroutine
    def complete(self):
        yield from self.future
        # after this point we can GC the _Completion object, because we are sure
        # the only CB has completed
        return self.get_return_value()

class Ioctx(rados.Ioctx):
    def __init__(self, name, librados, io, loop=None):
        self.loop = loop if loop else asyncio.get_event_loop()
        super().__init__(name, librados, io)

    @asyncio.coroutine
    def aio_read(self, oid: str, length=8192, offset=0):
        com = _Completion(self, self.loop)
        buffer = super().aio_read(oid, com, length=length, offset=offset)
        ret = yield from com.complete()
        if ret < 0:
            raise rados.make_ex(ret, "Ioctx.aio_read(%s): failed to read %s" % (self.name, oid))

        return buffer.read(ret)

    @asyncio.coroutine
    def aio_read_op_operate(self, oid: str, op: ReadOperation, flags=Operation.Flag.none):
        com = _Completion(self, self.loop)
        super().aio_read_op_operate(oid, op, com, flags)
        ret = yield from com.complete()
        if ret < 0:
            raise rados.make_ex(ret, "Ioctx.aio_read_op_operate(%s): failed to read %s" % (self.name, oid))

    @asyncio.coroutine
    def aio_write_op_operate(self, oid: str, op: WriteOperation, time=None, flags=Operation.Flag.none, wait_on_safe=False):
        com = _Completion(self, self.loop, wait_on_safe)
        super().aio_write_op_operate(oid, op, com, time, flags)
        ret = yield from com.complete()
        if ret < 0:
            raise rados.make_ex(ret, "Ioctx.aio_write_op_operate(%s): failed to read %s" % (self.name, oid))
