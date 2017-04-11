"""
 Copyright 2011, Hannu Valtonen <hannu.valtonen@ormod.com>
 Copyright 2015, Michal Humpula <mh@hudrydum.cz>

 This module is a thin wrapper around librados.
"""
import ctypes
import errno
import time
from ctypes import c_char_p, c_size_t, c_void_p, c_char, c_int, c_long, \
    c_ulong, create_string_buffer, byref, Structure, c_uint64, c_ubyte, \
    pointer, c_int64, c_uint32, c_uint8
from ctypes.util import find_library
from enum import Enum

class Error(Exception):
    """ `Error` class, derived from `Exception` """
    pass

class InterruptedOrTimeoutError(Error):
    """ `InterruptedOrTimeoutError` class, derived from `Error` """
    pass

class PermissionError(Error):
    """ `PermissionError` class, derived from `Error` """
    pass

class ObjectNotFound(Error):
    """ `ObjectNotFound` class, derived from `Error` """
    pass

class NoData(Error):
    """ `NoData` class, derived from `Error` """
    pass

class ObjectExists(Error):
    """ `ObjectExists` class, derived from `Error` """
    pass

class ObjectBusy(Error):
    """ `ObjectBusy` class, derived from `Error` """
    pass

class IOError(Error):
    """ `IOError` class, derived from `Error` """
    pass

class NoSpace(Error):
    """ `NoSpace` class, derived from `Error` """
    pass

class IncompleteWriteError(Error):
    """ `IncompleteWriteError` class, derived from `Error` """
    pass

class RadosStateError(Error):
    """ `RadosStateError` class, derived from `Error` """
    pass

class IoctxStateError(Error):
    """ `IoctxStateError` class, derived from `Error` """
    pass

class ObjectStateError(Error):
    """ `ObjectStateError` class, derived from `Error` """
    pass

class LogicError(Error):
    """ `` class, derived from `Error` """
    pass

class TimedOut(Error):
    """ `TimedOut` class, derived from `Error` """
    pass

class Canceled(Error):
    pass

def make_ex(ret, msg):
    """
    Translate a librados return code into an exception.

    :param ret: the return code
    :type ret: int
    :param msg: the error message to use
    :type msg: str
    :returns: a subclass of :class:`Error`
    """

    errors = {
        errno.EPERM     : PermissionError,
        errno.ENOENT    : ObjectNotFound,
        errno.EIO       : IOError,
        errno.ENOSPC    : NoSpace,
        errno.EEXIST    : ObjectExists,
        errno.EBUSY     : ObjectBusy,
        errno.ENODATA   : NoData,
        errno.EINTR     : InterruptedOrTimeoutError,
        errno.ETIMEDOUT : TimedOut,
        errno.ECANCELED : Canceled,
        }
    ret = abs(ret)
    if ret in errors:
        return errors[ret](msg)
    else:
        return Error(msg + (": errno %s" % errno.errorcode[ret]))

class rados_pool_stat_t(Structure):
    """ Usage information for a pool """
    _fields_ = [("num_bytes", c_uint64),
                ("num_kb", c_uint64),
                ("num_objects", c_uint64),
                ("num_object_clones", c_uint64),
                ("num_object_copies", c_uint64),
                ("num_objects_missing_on_primary", c_uint64),
                ("num_objects_unfound", c_uint64),
                ("num_objects_degraded", c_uint64),
                ("num_rd", c_uint64),
                ("num_rd_kb", c_uint64),
                ("num_wr", c_uint64),
                ("num_wr_kb", c_uint64)]

class rados_cluster_stat_t(Structure):
    """ Cluster-wide usage information """
    _fields_ = [("kb", c_uint64),
                ("kb_used", c_uint64),
                ("kb_avail", c_uint64),
                ("num_objects", c_uint64)]

#@class timeval(Structure):
#@    _fields_ = [("tv_sec", c_long), ("tv_usec", c_long)]

class Version(object):
    """ Version information """
    def __init__(self, major, minor, extra):
        self.major = major
        self.minor = minor
        self.extra = extra

    def __str__(self):
        return "%d.%d.%d" % (self.major, self.minor, self.extra)

def s2cs(string: str):
    return c_char_p(string.encode('utf-8'))

def cs2s(string: c_char_p):
    return string.decode('utf-8')

class Cluster(object):
    """librados python wrapper"""
    def require_state(self, *args):
        """
        Checks if the Cluster object is in a special state

        :raises: RadosStateError
        """
        if self.state in args:
           return
        raise RadosStateError("You cannot perform that operation on a Cluster object in state {}.".format(self.state))

    def __init__(self, rados_id=None, name=None, clustername=None,
                 conf_defaults=None, conffile=None, conf=None, flags=0):
        library_path  = find_library('rados')
        # maybe find_library can not find it correctly on all platforms,
        # so fall back to librados.so.2 in such case.
        self.librados = ctypes.CDLL(library_path if library_path is not None else 'librados.so.2')

        self.parsed_args = []
        self.conf_defaults = conf_defaults
        self.conffile = conffile
        self.cluster = c_void_p()
        self.rados_id = rados_id
        if rados_id is not None and not isinstance(rados_id, str):
            raise TypeError('rados_id must be a string or None')
        if conffile is not None and not isinstance(conffile, str):
            raise TypeError('conffile must be a string or None')
        if name is not None and not isinstance(name, str):
            raise TypeError('name must be a string or None')
        if clustername is not None and not isinstance(clustername, str):
            raise TypeError('clustername must be a string or None')
        if rados_id and name:
            raise Error("Cluster(): can't supply both rados_id and name")
        elif rados_id:
            name = 'client.' +  rados_id
        elif name is None:
            name = 'client.admin'
        if clustername is None:
            clustername = 'ceph'
        ret = self.librados.rados_create2(byref(self.cluster), s2cs(clustername),
                            s2cs(name), c_uint64(flags))

        if ret != 0:
            raise Error("rados_initialize failed with error code: %d" % ret)
        self.state = "configuring"
        # order is important: conf_defaults, then conffile, then conf
        if conf_defaults:
            for key, value in conf_defaults.items():
                self.conf_set(key, value)
        if conffile is not None:
            # read the default conf file when '' is given
            if conffile == '':
                conffile = None
            self.conf_read_file(conffile)
        if conf:
            for key, value in conf.items():
                self.conf_set(key, value)

    def shutdown(self):
        """
        Disconnects from the cluster.  Call this explicitly when a
        Cluster.connect()ed object is no longer used.
        """
        if hasattr(self, "state") and self.state != "shutdown":
            self.librados.rados_shutdown(self.cluster)
            self.state = "shutdown"

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type_, value, traceback):
        self.shutdown()
        return False

    def __del__(self):
        self.shutdown()

    def version(self):
        """
        Get the version number of the ``librados`` C library.

        :returns: a tuple of ``(major, minor, extra)`` components of the
                  librados version
        """
        major = c_int(0)
        minor = c_int(0)
        extra = c_int(0)
        self.librados.rados_version(byref(major), byref(minor), byref(extra))
        return Version(major.value, minor.value, extra.value)

    def conf_read_file(self, path=None):
        """
        Configure the cluster handle using a Ceph config file.

        :param path: path to the config file
        :type path: str
        """
        self.require_state("configuring", "connected")
        if path is not None and not isinstance(path, str):
            raise TypeError('path must be a string')
        ret = self.librados.rados_conf_read_file(self.cluster, s2cs(path))
        if (ret != 0):
            raise make_ex(ret, "error calling conf_read_file")

    def conf_get(self, option):
        """
        Get the value of a configuration option

        :param option: which option to read
        :type option: str

        :returns: str - value of the option or None
        :raises: :class:`TypeError`
        """
        self.require_state("configuring", "connected")
        if not isinstance(option, str):
            raise TypeError('option must be a string')
        length = 20
        return "asd"
        while True:
            ret_buf = create_string_buffer(length)
            ret = self.librados.rados_conf_get(self.cluster, s2cs(option), ret_buf, c_size_t(length))
            if (ret == 0):
                return cs2s(ret_buf.value)
            elif (ret == -errno.ENAMETOOLONG):
                length = length * 2
            elif (ret == -errno.ENOENT):
                return None
            else:
                raise make_ex(ret, "error calling conf_get")

    def conf_set(self, option, val):
        """
        Set the value of a configuration option

        :param option: which option to set
        :type option: str
        :param option: value of the option
        :type option: str

        :raises: :class:`TypeError`, :class:`ObjectNotFound`
        """
        self.require_state("configuring", "connected")
        if not isinstance(option, str):
            raise TypeError('option must be a string')
        if not isinstance(val, str):
            raise TypeError('val must be a string')
        ret = self.librados.rados_conf_set(self.cluster, s2cs(option), s2cs(val))
        if (ret != 0):
            raise make_ex(ret, "error calling conf_set")

    def connect(self):
        """
        Connect to the cluster.  Use shutdown() to release resources.
        """
        self.require_state("configuring")
        ret = self.librados.rados_connect(self.cluster)
        if (ret != 0):
            raise make_ex(ret, "error connecting to the cluster")
        self.state = "connected"

    def get_cluster_stats(self):
        """
        Read usage info about the cluster

        This tells you total space, space used, space available, and number
        of objects. These are not updated immediately when data is written,
        they are eventually consistent.

        :returns: dict - contains the following keys:

            - ``kb`` (int) - total space

            - ``kb_used`` (int) - space used

            - ``kb_avail`` (int) - free space available

            - ``num_objects`` (int) - number of objects

        """
        stats = rados_cluster_stat_t()
        ret = self.librados.rados_cluster_stat(self.cluster, byref(stats))
        if ret < 0:
            raise make_ex(
                ret, "Cluster.get_cluster_stats(%s): get_stats failed" % self.rados_id)
        return {'kb': stats.kb,
                'kb_used': stats.kb_used,
                'kb_avail': stats.kb_avail,
                'num_objects': stats.num_objects}

    def get_fsid(self):
        """
        Get the fsid of the cluster as a hexadecimal string.

        :raises: :class:`Error`
        :returns: str - cluster fsid
        """
        self.require_state("connected")
        buf_len = 37
        fsid = create_string_buffer(buf_len)
        ret = self.librados.rados_cluster_fsid(self.cluster, byref(fsid), c_size_t(buf_len))
        if ret < 0:
            raise make_ex(ret, "error getting cluster fsid")
        return cs2s(fsid.value)

    def open_ioctx(self, ioctx_name):
        """
        Create an io context

        The io context allows you to perform operations within a particular
        pool.

        :param ioctx_name: name of the pool
        :type ioctx_name: str

        :raises: :class:`TypeError`, :class:`Error`
        :returns: Ioctx - Rados Ioctx object
        """
        ioctx = self._open_ioctx_raw(ioctx_name)
        return Ioctx(ioctx_name, self.librados, ioctx)

    def _open_ioctx_raw(self, pool_name):
        self.require_state("connected")
        if not isinstance(pool_name, str):
            raise TypeError('the name of the pool must be a string')
        ioctx = c_void_p()
        ret = self.librados.rados_ioctx_create(self.cluster, s2cs(pool_name), byref(ioctx))
        if ret < 0:
            raise make_ex(ret, "error opening pool '%s'" % ioctx_name)
        return ioctx

class XattrIterator(object):
    """Extended attribute iterator"""
    def __init__(self, librados, it, oid = None):
        self.librados = librados
        self.it = it
        self.oid = oid

    def __iter__(self):
        return self

    def __next__(self):
        """
        Get the next xattr on the object

        :raises: StopIteration
        :returns: pair - of name and value of the next Xattr
        """
        name_ = c_char_p(0)
        val_ = c_char_p(0)
        len_ = c_int(0)
        ret = self.librados.rados_getxattrs_next(self.it, byref(name_), byref(val_), byref(len_))
        if (ret != 0):
            raise make_ex(ret, "error iterating over the extended attributes in '%s'" % self.oid)
        if name_.value == None:
            raise StopIteration()
        name = ctypes.string_at(name_)
        val = ctypes.string_at(val_, len_)
        return (cs2s(name), val)

    def __del__(self):
        self.librados.rados_getxattrs_end(self.it)

class OmapIterator(object):
    def __init__(self, librados, it):
        self.librados = librados
        self.it = it

    def __iter__(self):
        return self

    def __next__(self):
        """
        Get the next omap key/value on the object

        :raises: StopIteration
        :returns: pair - of name and value of the next OMAP item
        """
        name_ = c_char_p(0)
        val_ = c_char_p(0)
        len_ = c_int(0)
        ret = self.librados.rados_omap_get_next(self.it, byref(name_), byref(val_), byref(len_))
        if (ret != 0):
            raise make_ex(ret, "error iterating over the OMAP values")
        if name_.value == None:
            raise StopIteration()
        name = ctypes.string_at(name_)
        val = ctypes.string_at(val_, len_)
        return (cs2s(name), val)

    def __del__(self):
        self.librados.rados_omap_get_end(self.it)

class Ioctx(object):
    """Forward declaration"""
    pass

class Completion(object):
    """
    Rados Completion object

    According to rados docs the read operation gets only complete callback.

    For write operation the oncomplete is called when data are in memory on all
    replicas. The onsafe when data are on stable storage on all replicas.

    WARN: completion callbacks will be executed in rados thread context
    """

    RadosCb = ctypes.CFUNCTYPE(c_int, c_void_p, c_void_p)

    def __init__(self, ioctx: Ioctx, oncomplete=None, onsafe=None):
        self.ioctx = ioctx
        self.librados = ioctx.librados
        self._oncomplete = self._clean_callback(oncomplete)
        self._onsafe = self._clean_callback(onsafe)
        self.__set_completion()

    def _clean_callback(self, callback):
        if type(callback) == list:
            return callback
        elif hasattr(callback, '__call__'):
            return [callback]
        else:
            return None

    def __set_completion(self):
        """
        Constructs rados completion structure
        """
        self.rados_comp = c_void_p(0)
        self.c_complete_cb = self.RadosCb(self.__complete_cb) if self._oncomplete else c_void_p(0)
        self.c_safe_cb = self.RadosCb(self.__safe_cb) if self._onsafe else c_void_p(0)

        ret = self.librados.rados_aio_create_completion(c_void_p(0),
                          self.c_complete_cb, self.c_safe_cb, byref(self.rados_comp))
        if ret < 0:
            raise make_ex(ret, "error getting a completion")

    def __complete_cb(self, comp, _):
        for callback in self._oncomplete:
            callback(self)
        return 0

    def __safe_cb(self, comp, _):
        for callback in self._onsafe:
            callback(self)
        return 0

    def wait_for_complete_and_cb(self):
        """
        Wait for an asynchronous operation to complete and for the
        complete callback to have returned
        """
        self.ioctx.librados.rados_aio_wait_for_complete_and_cb(self.rados_comp)

    def wait_for_safe_and_cb(self):
        """
        Wait for an asynchronous operation to complete and for the
        safe callback to have returned
        """
        self.ioctx.librados.rados_aio_wait_for_safe_and_cb(self.rados_comp)

    def is_complete_and_cb(self):
        ret = self.ioctx.librados.rados_aio_is_complete_and_cb(self.rados_comp)
        return ret == 1

    def is_safe_and_cb(self):
        ret = self.ioctx.librados.rados_aio_is_safe_and_cb(self.rados_comp)
        return ret == 1

    def get_return_value(self) -> int:
        """
        Get the return value of an asychronous operation

        The return value is set when the operation is complete or safe,
        whichever comes first.

        :returns: int - return value of the operation
        """
        return self.ioctx.librados.rados_aio_get_return_value(self.rados_comp)

    def __del__(self):
        """
        Release a completion

        Call this when you no longer need the completion. It may not be
        freed immediately if the operation is not acked and committed.

        WARN: if the completion has callbacks attached to it, the callbacks
        handlers have to exist when they are called back!
        """
        self.librados.rados_aio_release(self.rados_comp)

class Buffer(object):
    def __init__(self, buffer):
        self._buffer = buffer

    def read(self, len):
        if self._buffer:
            return ctypes.string_at(self._buffer, len)
        else:
            return str()

class CmpXattrOp(Enum):
    eq  = c_ubyte(1)
    ne  = c_ubyte(2)
    gt  = c_ubyte(3)
    gte = c_ubyte(4)
    lt  = c_ubyte(5)
    lte = c_ubyte(6)

class Operation(object):
    class Flag(Enum):
        none               = c_int(0)
        balance_reads      = c_int(1)
        localize_reads     = c_int(2)
        order_reads_writes = c_int(4)
        ignore_cache       = c_int(8)
        skiprwlocks        = c_int(16)
        ignore_overlay     = c_int(32)
        full_try           = c_int(64)
    class OpFlags(Enum):
        none               = c_int(0)
        excl               = c_int(1)
        failok             = c_int(2)
        fadvise_random     = c_int(4)
        fadvise_sequential = c_int(8)
        fadvise_willneed   = c_int(10)
        fadvise_dontneed   = c_int(20)
        fadvise_nocache    = c_int(40)

class ReadOperation(Operation):
    """Rados Read operation"""

    def __init__(self, librados):
        self.librados = librados
        self.rop = self.librados.rados_create_read_op()

        self.read_buffer = None
        self.read_bytes = c_size_t(0)
        self.read_return = c_int(0)

        self.attrs_it = c_void_p(0)
        self.attrs_return = c_int(0)

        self.stat_size = c_uint64(0)
        self.stat_mtime = c_uint64(0)
        self.stat_return = c_int(0)

    def __del__(self):
        self.librados.rados_release_read_op(self.rop)
        self.rop = None

    def assert_exists(self):
        self.librados.rados_read_op_assert_exists(self.rop)

    def cmpxattr(self, key: str, operator: CmpXattrOp, data: bytes):
        if not isinstance(operator, CmpXattrOp):
            raise TypeError('operator must be one of CmpXattrOp')
        if not isinstance(data, bytes):
            raise TypeError('data must be a bytes')

        length = len(data)

        self.librados.rados_read_op_cmpxattr(self.rop, s2cs(key), operator.value,
                                              c_char_p(data), c_size_t(length))

    def set_flags(self, flags: Operation.OpFlags = Operation.OpFlags.none):
        if not isinstance(flags, Operation.OpFlags):
            raise TypeError('flags must be one of OperationOpFlags')

        self.librados.rados_read_op_set_flags(self.rop, flags.value)

    def read(self, length:int=8192, offset:int=0):
        self.read_buffer = create_string_buffer(length)

        self.librados.rados_read_op_read(self.rop, c_uint64(offset), c_size_t(length),
                            self.read_buffer, pointer(self.read_bytes),
                            pointer(self.read_return))

    def read_data(self) -> bytes:
        if self.read_bytes.value == 0:
            return ""

        if self.read_return.value < 0:
            raise make_ex(self.read_return, "Error calling read operation")

        return ctypes.string_at(self.read_buffer, self.read_bytes.value)

    def get_xattrs(self):
        self.librados.rados_read_op_getxattrs(self.rop, byref(self.attrs_it),
                                              pointer(self.attrs_return))

    def get_xattrs_data(self):
        if self.attrs_it == c_void_p(0):
            return None

        if self.attrs_return.value < 0:
            raise make_ex(self.attrs_return.value, "Error reading xattrs")

        return XattrIterator(self.librados, self.attrs_it)

    def omap_get_vals(self, start_after: str=None, filter_prefix: str=None, max_return=1024) -> OmapIterator:
        start = s2cs(start_after) if start_after else c_char_p()
        filter = s2cs(filter_prefix) if filter_prefix else c_char_p()

        it = OmapIterator(self.librados, c_void_p())
        it.retval = 0

        self.librados.rados_read_op_omap_get_vals(self.rop, start, filter,
            ctypes.c_ulonglong(max_return),
            byref(it.it),
            pointer(c_int(it.retval)))

        return it

    def stat(self):
        self.librados.rados_read_op_stat(self.rop, pointer(self.stat_size), pointer(self.stat_mtime), pointer(self.stat_return))

    def size(self):
        return self.stat_size.value

    def mtime(self):
        return time.localtime(self.stat_mtime.value)

class WriteOperation(Operation):
    """Rados write operation"""

    class CreateFlag(Enum):
        exclusive  = c_int(1)
        idempotent = c_int(0)

    def __init__(self, librados):
        self.librados = librados
        self.wop = self.librados.rados_create_write_op()

    def __del__(self):
        self.librados.rados_release_write_op(self.wop)
        self.wop = None

    def assert_exists(self):
        self.librados.rados_write_op_assert_exists(self.wop)

    def write_full(self, data: bytes):
        if not isinstance(data, bytes):
            raise TypeError('data must be a bytes')

        length = len(data)

        self.librados.rados_write_op_write_full(self.wop, c_char_p(data), c_size_t(length))

    def cmpxattr(self, key: str, operator: CmpXattrOp, data: bytes):
        if not isinstance(operator, CmpXattrOp):
            raise TypeError('operator must be one of CmpXattrOp')
        if not isinstance(data, bytes):
            raise TypeError('data must be a bytes')

        length = len(data)

        self.librados.rados_write_op_cmpxattr(self.wop, s2cs(key), operator.value,
                                              c_char_p(data), c_size_t(length))

    def setxattr(self, key: str, data: bytes):
        if not isinstance(data, bytes):
            raise TypeError('data must be a bytes')

        length = len(data)

        self.librados.rados_write_op_setxattr(self.wop, s2cs(key),
                                              c_char_p(data), c_size_t(length))

    def create(self, exclusive: bool):
        if not isinstance(exclusive, bool):
            raise TypeError('exclusive must be a bool')

        flag = self.CreateFlag.exclusive if exclusive else self.CreateFlag.idempotent
        self.librados.rados_write_op_create(self.wop, flag.value, c_void_p())

    def set_flags(self, flags: Operation.OpFlags = Operation.OpFlags.none):
        if not isinstance(flags, Operation.OpFlags):
            raise TypeError('flags must be one of OperationOpFlags')

        self.librados.rados_write_op_set_flags(self.wop, flags.value)

    def remove(self):
        self.librados.rados_write_op_remove(self.wop)

    def append(self, data: bytes):
        if not isinstance(data, bytes):
            raise TypeError('data must be a bytes')

        length = len(data)

        self.librados.rados_write_op_append(self.wop, c_char_p(data), c_size_t(length))

    def omap_set(self, kv: dict):
        """
        The rados_write_op_omap_set functions takes arrays (C pointers) to chars/bytes (C pointers)

        All the mess belows tries to construct apropriate structures
        """
        if not isinstance(kv, dict):
            raise TypeError('kv must be a dict')

        size = len(kv)
        num = c_size_t(size)
        keys = (c_char_p * size)(*[k.encode('utf-8') for k in kv.keys()])
        vals = (c_char_p * size)(*kv.values())
        lens = (c_size_t * size)(*[len(v) for v in kv.values()])

        self.librados.rados_write_op_omap_set(self.wop, pointer(keys), pointer(vals), pointer(lens), num)

class Ioctx(object):
    """rados.Ioctx object"""
    def __init__(self, name, librados, io):
        self.name = name
        self.librados = librados
        self.io = io
        self.state = "open"
        self.nspace = ""

    def __enter__(self):
        return self

    def __exit__(self, type_, value, traceback):
        self.close()
        return False

    def __del__(self):
        self.close()

    def aio_read(self, oid: str, completion: Completion, length=8192, offset=0):
        """
        Read data from an object asynchronously

        :param oid: name of the object
        :param length: the number of bytes to read (default=8192)
        :param offset: byte offset in the object to begin reading at

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: bytes - data read from object
        """
        self.require_ioctx_open()
        if not isinstance(oid, str):
            raise TypeError('oid must be a string')
        buffer = Buffer(create_string_buffer(length))
        ret = self.librados.rados_aio_read(self.io, s2cs(oid), completion.rados_comp,
                           buffer._buffer, c_size_t(length), c_uint64(offset))
        if ret < 0:
            raise make_ex(ret, "Ioctx.aio_read(%s): failed to read %s" % (self.name, oid))

        return buffer

    def require_ioctx_open(self):
        """
        Checks if the rados.Ioctx object state is 'open'

        :raises: IoctxStateError
        """
        if self.state != "open":
            raise IoctxStateError("The pool is %s" % self.state)

    def set_namespace(self, nspace):
        """
        Set the namespace for objects within an io context.

        The namespace in addition to the object name fully identifies
        an object. This affects all subsequent operations of the io context
        - until a different namespace is set, all objects in this io context
        will be placed in the same namespace.

        :param nspace: the namespace to use, or None/"" for the default namespace
        :type nspace: str

        :raises: :class:`TypeError`
        """
        self.require_ioctx_open()
        if nspace is None:
            nspace = ""
        if not isinstance(nspace, str):
            raise TypeError('namespace must be a string')
        self.librados.rados_ioctx_set_namespace(self.io, s2cs(nspace))
        self.nspace = nspace

    def get_namespace(self) -> str:
        """
        Get the namespace of context

        :returns: str - namespace
        """
        return self.nspace

    def close(self):
        """
        Close a rados.Ioctx object.

        This just tells librados that you no longer need to use the io context.
        It may not be freed immediately if there are pending asynchronous
        requests on it, but you should not use an io context again after
        calling this function on it.
        """
        if self.state == "open":
            self.require_ioctx_open()
            self.librados.rados_ioctx_destroy(self.io)
            self.state = "closed"

    def write(self, key: str, data: bytes, offset=0):
        """
        Write data to an object synchronously

        :param key: name of the object
        :param data: data to write
        :param offset: byte offset in the object to begin writing at

        :raises: :class:`TypeError`
        :raises: :class:`LogicError`
        :returns: int - 0 on success
        """
        self.require_ioctx_open()
        if not isinstance(key, str):
            raise TypeError('key must be a string')
        if not isinstance(data, bytes):
            raise TypeError('data must be a bytes')
        length = len(data)
        ret = self.librados.rados_write(self.io, s2cs(key), c_char_p(data),
                            c_size_t(length), c_uint64(offset))
        if ret == 0:
            return ret
        elif ret < 0:
            raise make_ex(ret, "Ioctx.write(%s): failed to write %s" % (self.name, key))
        else:
            raise LogicError("Ioctx.write(%s): rados_write \
returned %d, but should return zero on success." % (self.name, ret))

    def write_full(self, key: str, data: bytes):
        """
        Write an entire object synchronously.

        The object is filled with the provided data. If the object exists,
        it is atomically truncated and then written.

        :param key: name of the object
        :param data: data to write

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: int - 0 on success
        """
        self.require_ioctx_open()
        if not isinstance(key, str):
            raise TypeError('key must be a string')
        if not isinstance(data, bytes):
            raise TypeError('data must be a bytes')
        length = len(data)
        ret = self.librados.rados_write_full(self.io, s2cs(key), c_char_p(data),
                            c_size_t(length))
        if ret == 0:
            return ret
        elif ret < 0:
            raise make_ex(ret, "Ioctx.write_full(%s): failed to write %s" % \
                (self.name, key))
        else:
            raise LogicError("Ioctx.write_full(%s): rados_write_full \
returned %d, but should return zero on success." % (self.name, ret))

    def read(self, key: str, length:int=8192, offset:int=0) -> bytes:
        """
        Read data from an object synchronously

        :param key: name of the object
        :param length: the number of bytes to read (default=8192)
        :param offset: byte offset in the object to begin reading at

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: str - data read from object
        """
        self.require_ioctx_open()
        if not isinstance(key, str):
            raise TypeError('key must be a string')
        ret_buf = create_string_buffer(length)
        ret = self.librados.rados_read(self.io, s2cs(key), ret_buf, c_size_t(length),
                            c_uint64(offset))
        if ret < 0:
            raise make_ex(ret, "Ioctx.read(%s): failed to read %s" % (self.name, key))
        return ctypes.string_at(ret_buf, ret)

    def get_stats(self):
        """
        Get pool usage statistics

        :returns: dict - contains the following keys:

            - ``num_bytes`` (int) - size of pool in bytes

            - ``num_kb`` (int) - size of pool in kbytes

            - ``num_objects`` (int) - number of objects in the pool

            - ``num_object_clones`` (int) - number of object clones

            - ``num_object_copies`` (int) - number of object copies

            - ``num_objects_missing_on_primary`` (int) - number of objets
                missing on primary

            - ``num_objects_unfound`` (int) - number of unfound objects

            - ``num_objects_degraded`` (int) - number of degraded objects

            - ``num_rd`` (int) - bytes read

            - ``num_rd_kb`` (int) - kbytes read

            - ``num_wr`` (int) - bytes written

            - ``num_wr_kb`` (int) - kbytes written
        """
        self.require_ioctx_open()
        stats = rados_pool_stat_t()
        ret = self.librados.rados_ioctx_pool_stat(self.io, byref(stats))
        if ret < 0:
            raise make_ex(ret, "Ioctx.get_stats(%s): get_stats failed" % self.name)
        return {'num_bytes': stats.num_bytes,
                'num_kb': stats.num_kb,
                'num_objects': stats.num_objects,
                'num_object_clones': stats.num_object_clones,
                'num_object_copies': stats.num_object_copies,
                "num_objects_missing_on_primary": stats.num_objects_missing_on_primary,
                "num_objects_unfound": stats.num_objects_unfound,
                "num_objects_degraded": stats.num_objects_degraded,
                "num_rd": stats.num_rd,
                "num_rd_kb": stats.num_rd_kb,
                "num_wr": stats.num_wr,
                "num_wr_kb": stats.num_wr_kb }

    def remove(self, key: str):
        """
        Delete an object

        This does not delete any snapshots of the object.

        :param key: the name of the object to delete

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: bool - True on success
        """
        self.require_ioctx_open()
        if not isinstance(key, str):
            raise TypeError('key must be a string')
        ret = self.librados.rados_remove(self.io, s2cs(key))
        if ret < 0:
            raise make_ex(ret, "Failed to remove '%s'" % key)
        return True

    def stat(self, key: str):
        """
        Get object stats (size/mtime)

        :param key: the name of the object to get stats from

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: (size,timestamp)
        """
        self.require_ioctx_open()
        if not isinstance(key, str):
            raise TypeError('key must be a string')
        psize = c_uint64()
        pmtime = c_uint64()

        ret = self.librados.rados_stat(self.io, s2cs(key), pointer(psize), pointer(pmtime))

        if ret < 0:
            raise make_ex(ret, "Failed to stat %r" % key)

        return psize.value, time.localtime(pmtime.value)

    def get_xattr(self, key:str , xattr_name: str) -> bytes:
        """
        Get the value of an extended attribute on an object.

        :param key: the name of the object to get xattr from
        :param xattr_name: which extended attribute to read

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: str - value of the xattr
        """
        self.require_ioctx_open()
        if not isinstance(xattr_name, str):
            raise TypeError('xattr_name must be a string')
        ret_length = 4096
        while ret_length < 4096 * 1024 * 1024:
            ret_buf = create_string_buffer(ret_length)
            ret = self.librados.rados_getxattr(self.io, s2cs(key), s2cs(xattr_name),
                                 ret_buf, c_size_t(ret_length))
            if (ret == -errno.ERANGE):
                ret_length *= 2
            elif ret < 0:
                raise make_ex(ret, "Failed to get xattr %r" % xattr_name)
            else:
                break
        return ctypes.string_at(ret_buf, ret)

    def get_xattrs(self, oid: str):
        """
        Start iterating over xattrs on an object.

        :param oid: the name of the object to get xattrs from

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: XattrIterator
        """
        self.require_ioctx_open()
        if not isinstance(oid, str):
            raise TypeError('oid must be a string')
        it = c_void_p(0)
        ret = self.librados.rados_getxattrs(self.io, s2cs(oid), byref(it))
        if ret != 0:
            raise make_ex(ret, "Failed to get rados xattrs for object %r" % oid)
        return XattrIterator(self.librados, it, oid)

    def set_xattr(self, key: str, xattr_name: str, xattr_value: bytes):
        """
        Set an extended attribute on an object.

        :param key: the name of the object to set xattr to
        :param xattr_name: which extended attribute to set
        :param xattr_value: the value of the  extended attribute

        :raises: :class:`TypeError`
        :raises: :class:`Error`
        :returns: bool - True on success, otherwise raise an error
        """
        self.require_ioctx_open()
        if not isinstance(key, str):
            raise TypeError('key must be a string')
        if not isinstance(xattr_name, str):
            raise TypeError('xattr_name must be a string')
        if not isinstance(xattr_value, bytes):
            raise TypeError('xattr_value must be a bytes')
        ret = self.librados.rados_setxattr(self.io,
                            s2cs(key), s2cs(xattr_name),
                            c_char_p(xattr_value), c_size_t(len(xattr_value)))
        if ret < 0:
            raise make_ex(ret, "Failed to set xattr %r" % xattr_name)
        return True

    def read_op_create(self):
        return ReadOperation(self.librados)

    def read_op_operate(self, oid: str, op: ReadOperation, flags=Operation.Flag.none):
        self.require_ioctx_open()

        if not isinstance(oid, str):
            raise TypeError('oid must be a string')
        if not isinstance(flags, Operation.Flag):
            raise TypeError('flags must be a Operation.Flag')

        ret = self.librados.rados_read_op_operate(op.rop, self.io, s2cs(oid), flags.value)

        if ret < 0:
            raise make_ex(ret, "Ioctx.rados_read_op_operate(%s): failed to run operation on %s" % (self.name, oid))

    def aio_read_op_operate(self, oid: str, op: ReadOperation, completion: Completion, flags=Operation.Flag.none):
        self.require_ioctx_open()

        if not isinstance(oid, str):
            raise TypeError('oid must be a string')
        if not isinstance(flags, Operation.Flag):
            raise TypeError('flags must be a Operation.Flag')

        ret = self.librados.rados_aio_read_op_operate(op.rop, self.io, completion.rados_comp, s2cs(oid),
                           flags.value)
        if ret < 0:
            raise make_ex(ret, "Ioctx.aio_read_op_operate(%s): failed to read %s" % (self.name, oid))

    def write_op_create(self):
        return WriteOperation(self.librados)

    def write_op_operate(self, oid: str, op: WriteOperation, time=None, flags=Operation.Flag.none) -> int:
        self.require_ioctx_open()

        if not isinstance(oid, str):
            raise TypeError('oid must be a string')
        if isinstance(time, int):
            time = byref(c_long(time))
        elif time == None:
            time = c_void_p()
        else:
            raise TypeError('time must be a int or None')
        if not isinstance(flags, Operation.Flag):
            raise TypeError('flags must be a Operation.Flag')

        ret = self.librados.rados_write_op_operate(op.wop, self.io, s2cs(oid),
                                                   time, flags.value)
        if ret < 0:
            raise make_ex(ret, "Ioctx.rados_write_op_operate(%s): failed to run operation on %s" % (self.name, oid))

    def aio_write_op_operate(self, oid: str, op: WriteOperation, completion: Completion, time=None,
                             flags=Operation.Flag.none):
        self.require_ioctx_open()

        if not isinstance(oid, str):
            raise TypeError('oid must be a string')
        if isinstance(time, int):
            time = byref(c_long(time))
        elif time == None:
            time = c_void_p()
        else:
            raise TypeError('time must be a int or None')
        if not isinstance(flags, Operation.Flag):
            raise TypeError('flags must be a Operation.Flag')

        ret = self.librados.rados_aio_write_op_operate(op.wop, self.io, completion.rados_comp, s2cs(oid),
                           time, flags.value)
        if ret < 0:
            raise make_ex(ret, "Ioctx.aio_write_op_operate(%s): failed to read %s" % (self.name, oid))

    def omap_iter(self, oid: str, filter_prefix: bytes=None) -> OmapIterator:
        rop = ReadOperation(self.librados)
        iterator = rop.omap_get_vals(filter_prefix=filter_prefix)

        self.read_op_operate(oid, rop)

        return iterator
