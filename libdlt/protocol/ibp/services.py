'''
@Name:   ProtocolService.py
@Author: Jeremy Musser
@Date:   04/01/2015

-------------------------

ProtocolService provides low level API access
to the IBP protocol

'''

import time, socket
from math import ceil

from libdlt.protocol.ibp.settings import DEFAULT_PASSWORD, DEFAULT_TIMEOUT, DEFAULT_DURATION, DEFAULT_MAXSIZE
from lace import logging
from lace.logging import trace
from libdlt.protocol.exceptions import AllocationError
from libdlt.protocol.ibp import flags, allocation
from libdlt.protocol.ibp.flags import print_error
from libdlt.protocol.ibp.exceptions import IBPError
from libdlt.depot import Depot

class Capability(object):
    def __init__(self, cap_string):
        try:
            self._cap       = cap_string
            tmpSplit        = cap_string.split("/")
            tmpAddress      = tmpSplit[2].split(":")
            self.key        = tmpSplit[3]
            self.wrmKey     = tmpSplit[4]
            self.code       = tmpSplit[5]
        except Exception as exp:
            raise ValueError('Malformed capability string')

    def __str__(self):
        return self._cap

    def __repr__(self):
        return self.__str__()


class ProtocolService(object):
    @trace.debug("IBP.ProtocolService")
    def __init__(self):
        self._log = logging.getLogger('libdlt.ibp')

    
    @trace.info("IBP.ProtocolService")
    def getStatus(self, depot, **kwargs):
        timeout = kwargs.get('timeout', None) or DEFAULT_TIMEOUT

        # Query the status of a Depot.
        # IBPv031[0] IBP_ST_INQ[2] pwd timeout
        c = f"{flags.IBPv031} {flags.IBP_STATUS} {flags.IBP_ST_INQ} " \
            f"{kwargs.get('password', DEFAULT_PASSWORD)} {timeout} \n"
        try:
            timeout = kwargs.get('timeout', None)
            return dict(zip(["total", "used", "volatile", "used-volatile", "max-duration"],
                            self._dispatch_command(depot, c, timeout).split(" ")))
        except Exception as e:
            self._log.warn(f"getStatus - Failed @ {depot.host}:{depot.port} - {e}")
            raise

    @trace.info("IBP.ProtocolService")
    def manage(self, alloc, **kwargs):
        timeout = kwargs.get('timeout', None) or DEFAULT_TIMEOUT
        try:
            cap, depot = Capability(alloc.mapping.manage), Depot(alloc.location)
        except AttributeError:
            raise AllocationError("Incomplete allocation")

        # Generate manage request with the following form
        # IBPv031[0] IBP_MANAGE[9] manage_key "MANAGE" IBP_CHANGE[43] cap_type max_size duration reliability timeout
        c = f"{flags.IBPv031} {flags.IBP_MANAGE} {cap.key} {cap.code} " \
            f"{kwargs.get('mode', flags.IBP_CHANGE)} {kwargs.get('cap_type', 0)} " \
            f"{kwargs.get('max_size', DEFAULT_MAXSIZE)} " \
            f"{kwargs.get('duration', DEFAULT_DURATION)} " \
            f"{kwargs.get('reliability', flags.IBP_HARD)} {timeout} \n"
        try:
            timeout = kwargs.get('timeout', None)
            return self._dispatch_command(depot, c, timeout).split(" ")
        except Exception as e:
            self._log.warn(f"manage: [{alloc.id}] - Failed @ {depot.host}:{depot.port} - {e}")
            raise

    @trace.info("IBP.ProtocolService")
    def probe(self, alloc, **kwargs):
        return dict(zip(["read_count","write_count","size","max_size",
                         "duration","reliability","type"],
                        self.manage(alloc, mode = flags.IBP_PROBE, **kwargs)[1:]))

    @trace.info("IBP.ProtocolService")
    def allocate(self, depot, offset, size, **kwargs):
        timeout = kwargs.get('timeout', None) or DEFAULT_TIMEOUT
        duration = kwargs.get('duration', None) or DEFAULT_DURATION
        
        # Generate destination Allocation and Capabilities using the form below
        # IBPv031[0] IBP_ALLOCATE[1] reliability cap_type duration size timeout
        c = f"{flags.IBPv031} {flags.IBP_ALLOCATE} " \
            f"{kwargs.get('reliability', flags.IBP_HARD)} " \
            f"{kwargs.get('cap_type', flags.IBP_BYTEARRAY)} {duration} {size} {timeout} \n"
        try:
            timeout = kwargs.get('timeout', None)
            r = self._dispatch_command(depot, c, timeout).split(" ")[1:]
        except Exception as e:
            self._log.warn(f"allocate: Failed @ {depot.host}:{depot.port} - {e}")
            raise

        try:
            alloc = allocation.IBPExtent()
            alloc.mapping = dict(zip(["read", "write", "manage"],
                                     [v.replace("0.0.0.0", str(depot.host)) for v in r]))
            alloc.lifetime = { 'start': str(int(time.time() * 1000000)),
                               'end':  str(int((time.time() + duration) * 1000000)) }
            alloc.location = depot.endpoint
            alloc.offset = alloc.alloc_offset = offset
            alloc.size = alloc.alloc_length = size
        except:
            raise
        return alloc
    
    @trace.info("IBP.ProtocolService")
    def store(self, alloc, data, size, **kwargs):
        timeout = kwargs.get('timeout', None) or DEFAULT_TIMEOUT
        try:
            cap, depot = Capability(alloc.mapping.write), Depot(alloc.location)
        except AttributeError:
            raise AllocationError("Incomplete allocation")
        # IBPv031[0] IBP_STORE[2] write_key WRMKey size timeout
        c = f"{flags.IBPv031} {flags.IBP_STORE} {cap.key} {cap.wrmKey} {size} {timeout}\n"
        try:
            timeout = kwargs.get('timeout', None)
            return self._dispatch_data(depot, c, data, timeout).split(" ")
        except Exception as e:
            self._log.warn(f"store: Failed @ {depot.host}:{depot.port} - {e}")
            raise

    @trace.info("IBP.ProtocolService")
    def send(self, source, destination, **kwargs):
        timeout = kwargs.get('timeout', None) or DEFAULT_TIMEOUT
        size = kwargs.get("size", None) or source.size
        try:
            s_cap,s_depot = Capability(source.mapping.read), Depot(source.location)
            d_cap,d_depot = Capability(destination.mapping.write), Depot(destination.location)
        except AttributeError:
            raise AllocationError("Incomplete allocation")
        # IBPv040[1] IBP_SEND[5] src_read_key dest_write_cap src_WRMKey offset size timeout timeout timeout
        c = f"{flags.IBPv040} {flags.IBP_SEND} {s_cap.key} {str(d_cap)} {s_cap.wrmKey} " \
            f"{kwargs.get('offset', 0)} {size} {timeout} {timeout} {timeout}\n "

    # Generate move request with the following form
        try:
            timeout = kwargs.get('timeout', None)
            return self._dispatch_command(s_depot, c, timeout).split(" ")
        except Exception as e:
            self._log.warn(f"send: Failed @ {s_depot.host}:{s_depot.port} - {e}")
            raise

    @trace.info("IBP.ProtocolService")
    def load(self, alloc, **kwargs):
        timeout = kwargs.get('timeout', None) or DEFAULT_TIMEOUT
        try:
            cap, depot = Capability(alloc.mapping.read), Depot(alloc.location)
        except AttributeError:
            raise AllocationError("Incomplete allocation")
        c = f"{flags.IBPv031} {flags.IBP_LOAD} {cap.key} {cap.wrmKey} " \
            f"{kwargs.get('offset', 0)} {alloc.size} " \
            f"{kwargs.get('timeout', DEFAULT_TIMEOUT)} \n"
        try:
            timeout = kwargs.get('timeout', None)
            return self._receive_data(depot, c, alloc.size, timeout=timeout)["data"]
        except Exception as e:
            self._log.warn(f"load: Failed @ {depot.host}:{depot.port} - {e}")
            raise

    @trace.debug("IBP.ProtocolService")
    def _receive_data(self, depot, command, size, timeout):
        if isinstance(command, str): command = command.encode()

        self._log.debug(f"IBP receive [{depot.host}]: {command}")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            s.connect((str(depot.host), depot.port))
            s.sendall(command)
            buf = s.recv(1024)
            try:
                line = buf.index(b'\n') + 1
                hdr, data = buf[:line], buf[line:]
            except ValueError: data = b''
            if hdr.startswith(b'-'):
                if isisntance(hdr, bytes): hdr = hdr.decode()
                raise IBPError(print_error(r.split(" ")[0]))
            while len(data) < size:
                data += s.recv(size - len(data))

        return { "headers": hdr.decode(), "data": data }

    @trace.debug("IBP.ProtocolService")
    def _dispatch_data(self, depot, command, data, timeout):
        if isinstance(command, str): command = command.encode()
        if isinstance(data, str): data = data.encode()

        self._log.debug(f"IBP send [{depot.host}]: {command} | size: {len(data)}")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            s.connect((str(depot.host), depot.port))
            s.sendall(command)
            r = s.recv(1024)
            if r.startswith(b'-'):
                if isinstance(r, bytes): r = r.decode()
                raise IBPError(print_error(r.split(" ")[0]))
            s.sendall(data)
            r = s.recv(1024)
        if isinstance(r, bytes): r = r.decode()
        return r

    @trace.debug("IBP.ProtocolService")
    def _dispatch_command(self, depot, command, timeout):
        if isinstance(command, str): command = command.encode()

        self._log.debug(f"IBP command [{depot.host}]: {command}")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            s.connect((str(depot.host), int(depot.port)))
            s.sendall(command)
            r = s.recv(1024)
            if isinstance(r, bytes): r = r.decode()

        if r.startswith("-"): raise IBPError(print_error(r.split(" ")[0]))
        return r
