'''
@Name:   ProtocolService.py
@Author: Jeremy Musser
@Date:   04/01/2015

-------------------------

ProtocolService provides low level API access
to the IBP protocol

'''

import time
import argparse
import socket
import logging
import traceback

from .settings import DEFAULT_PASSWORD, DEFAULT_TIMEOUT, DEFAULT_DURATION
from libdlt.logging import debug, info
from libdlt.protocol.ibp import flags, allocation
from libdlt.protocol.ibp.flags import print_error
from libdlt.protocol.ibp.exceptions import IBPError

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
    @debug("IBP.ProtocolService")
    def __init__(self):
        self._log = logging.getLogger()

    
    '''
    @input: depot - A Depot object
    @optional:
            password - the password to access the depot
            timeout  - the length of time in seconds to wait for a response
    @output:
           Dict:
             total - the total space on the depot
             used  - the amount of data stored on the depot
             volatile - the amount of data that can be removed if space is required
             non-volatile - the amount of data that cannot be removed
             max-duration - the maximum time in seconds data can be hosted on the depot without refreshing
    '''
    @info("IBP.ProtocolService")
    def getStatus(self, depot, **kwargs):
    # Query the status of a Depot.
    
    # Generate request with the following form
    # IBPv031[0] IBP_ST_INQ[2] pwd timeout
        pwd = DEFAULT_PASSWORD
        timeout = DEFAULT_TIMEOUT
        tmpCommand = ""
        
        if "password" in kwargs:
            pwd = kwargs["password"]
        if "timeout" in kwargs:
            timeout = kwargs["timeout"]
        
        try:
            tmpCommand = "{0} {1} {2} {3} {4}\n".format(flags.IBPv031, flags.IBP_STATUS, flags.IBP_ST_INQ, pwd, timeout)
            result = self._dispatch_command(depot, tmpCommand)
            if not result:
                return None
            result = result.split(" ")
        except Exception as exp:
            self._log.warn("IBPProtocol.GetStatus: Failed to get the status of {host}:{port} - {err}".format(err = exp, **address))
            return None
            
        return dict(zip(["total", "used", "volatile", "used-volatile", "max-duration"], result))




    '''
    @input: alloc - an Allocation containing metadata about the allocation
    @optional:
           reliability - One of either IBP_HARD or IBP_SOFT, when hard, allocations will not expire unless
                         explicitly removed, when soft, allocations will expire after duration.
           type        - One of BYTEARRAY, BUFFER, FIFO, CIRQ.
           duration    - The amount of time, in seconds, that the allocation is reserved.
           timeout     - The amount of time the connection will wait for a response.
           max_size    - Changes the maximum size of the data stored by the allocation
           cap_type    - The capability modified by the action
           mode        - Defines the type of manage call.
    @output:
           The response from the depot (varies by manage mode)
    '''
    @info("IBP.ProtocolService")
    def manage(self, alloc, **kwargs):
        cap_type    = 0
        reliability = flags.IBP_HARD
        timeout     = DEFAULT_TIMEOUT
        max_size    = DEFAULT_MAXSIZE
        duration    = DEFAULT_DURATION
        tmpCommand  = ""
        mode        = flags.IBP_CHANGE

    # Generate manage request with the following form
    # IBPv031[0] IBP_MANAGE[9] manage_key "MANAGE" IBP_CHANGE[43] cap_type max_size duration reliability timeout
        if "cap_type" in kwargs:
            cap_type = kwargs["cap_type"]
        if "reliability" in kwargs:
            reliability = kwargs["reliability"]
        if "timeout" in kwargs:
            timeout = kwargs["timeout"]
        if "max_size" in kwargs:
            max_size = kwargs["max_size"]
        if "duration" in kwargs:
            duration = kwargs["duration"]
        if "mode" in kwargs:
            mode = kwargs["mode"]
            
        try:
            cap = Capability(alloc.mapping.manage)
            tmpCommand = "{0} {1} {2} {3} {4} {5} {6} {7} {8} {9}\n".format(flags.IBPv031,
                                                                            flags.IBP_MANAGE,
                                                                            cap.key,
                                                                            cap.code,
                                                                            mode,
                                                                            cap_type,
                                                                            max_size,
                                                                            duration,
                                                                            reliability,
                                                                            timeout
                                                                            )
            result = self._dispatch_command(alloc.depot, tmpCommand)
            if not result:
                return None
            result = result.split(" ")
        except Exception as exp:
            self._log.warn("IBPProtocol.Manage [{alloc}]: Could not connect to {host}:{port} - {err}".format(alloc = alloc.id, err = exp, host = alloc.host, port = alloc.port))
            return None

        if result[0].startswith("-"):
            self._log.warn("IBPProtocol.Manage [{alloc}]: Failed to manage allocation - {err}".format(alloc = alloc.id, err = print_error(result[0])))
            return None
        else:
            return result



    '''
    See Manage, Probe is a decorator for manage.
    '''
    @info("IBP.ProtocolService")
    def probe(self, alloc, **kwargs):
        results = self.Manage(alloc, mode = flags.IBP_PROBE, **kwargs)
        if not results:
            return None
        
        result = dict(zip(["read_count", "write_count", "size", "max_size", "duration", "reliability", "type"], results[1:]))
        return result
    
 

   
    '''
    @input: 
           depot - A Depot object
           size  - The size of the desired allocation
    @optional:
           reliability - One of either IBP_HARD or IBP_SOFT, when hard, allocations will not expire unless
                         explicitly removed, when soft, allocations will expire after duration.
           type        - One of BYTEARRAY, BUFFER, FIFO, CIRQ.
           duration    - The amount of time, in seconds, that the allocation is reserved.
           timeout     - The amount of time the connection will wait for a response.
    @output:
           An Allocation object
    '''
    @info("IBP.ProtocolService")
    def allocate(self, depot, offset, size, **kwargs):
        # Generate destination Allocation and Capabilities using the form below
        # IBPv031[0] IBP_ALLOCATE[1] reliability cap_type duration size timeout
        reliability = kwargs.get('reliability', None) or flags.IBP_HARD
        cap_type    = kwargs.get('type', None) or flags.IBP_BYTEARRAY
        timeout     = kwargs.get('timeout', None) or DEFAULT_TIMEOUT
        duration    = kwargs.get('duration', None) or DEFAULT_DURATION
        
        try:
            tmpCommand = "{0} {1} {2} {3} {4} {5} {6} \n".format(flags.IBPv031, flags.IBP_ALLOCATE, reliability, cap_type, duration, size, timeout)
            result = self._dispatch_command(depot, tmpCommand)
            result = result.split(" ")[1:]
        except Exception as exp:
            self._log.warn("IBPProtocol.Allocate: Could not connect to {d} - {err}".format(err = exp, d = depot.endpoint))
            raise IBPError("Failed to allocate ibp resource")
        
        if result[0].startswith("-"):
            self._log.warn("IBPProtocol.Allocate: Failed to allocate resource - {err}".format(err = print_error(result[0])))
            raise IBPError("Error from server", response=print_error(result[0]))
        
        try:
            alloc = allocation.IBPExtent()
            alloc.mapping.read = result[0]
            alloc.mapping.write = result[1]
            alloc.mapping.manage = result[2]
            alloc.lifetime = { 'start': str(int(time.time() * 1000000)),
                               'end':  str(int((time.time() + duration) * 1000000)) }
            
            alloc.depot = depot
            alloc.location = depot.endpoint
            alloc.offset = offset
            alloc.size = size
            alloc.alloc_offset = offset
            alloc.alloc_length = size
        except:
            traceback.print_exc()
            raise
        return alloc
    
    # Below are several shorthand versions of Allocate for hard and soft allocations of various types.
    def allocateSoftByteArray(self, depot, size, **kwargs):
        return self.allocate(depot, size, reliability = flags.IBP_SOFT, type = flags.IBP_BYTEARRAY, **kwargs)

    def allocateHardByteArray(self, depot, size, **kwargs):
        return self.allocate(depot, size, reliability = flags.IBP_HARD, type = flags.IBP_BYTEARRAY, **kwargs)

    def allocateSoftBuffer(self, depot, size, **kwargs):
        return self.allocate(depot, size, reliability = flags.IBP_SOFT, type = flags.IBP_BUFFER, **kwargs)
    
    def allocateHardBuffer(self, depot, size, **kwargs):
        return self.allocate(depot, size, reliability = flags.IBP_HARD, type = flags.IBP_BUFFER, **kwargs)

    def allocateSoftFifo(self, depot, size, **kwargs):
        return self.allocate(depot, size, reliability = flags.IBP_SOFT, type = flags.IBP_FIFO, **kwargs)

    def allocateHardFifo(self, depot, size, **kwargs):
        return self.allocate(depot, size, reliability = flags.IBP_HARD, type = flags.IBO_FIFO, **kwargs)

    def allocateSoftCirq(self, depot, size, **kwargs):
        return self.allocate(depot, size, reliability = flags.IBP_SOFT, type = flags.IBP_CIRQ, **kwargs)

    def allocateHardCirq(self, depot, size, **kwargs):
        return self.allocate(depot, size, reliability = flags.IBP_HARD, typ = flags.IBP_CIRQ, **kwargs)



    '''
    @input: 
           alloc - An Allocation object describing the allocation
           data  - The bytes to be stored
           size  - The size of the data
    @optional:
           duration    - The amount of time, in seconds, that the allocation is reserved.
           timeout     - The amount of time the connection will wait for a response.
    @output:
           The duration of the allocation
    '''
    @info("IBP.ProtocolService")
    def store(self, alloc, data, size, **kwargs):
        assert alloc.depot
        depot = alloc.depot
        timeout  = DEFAULT_TIMEOUT
        duration = DEFAULT_DURATION
        
        if "timeout" in kwargs:
            timeout = kwargs["timeout"]
        if "duration" in kwargs:
            duration = kwargs["duration"]
        
        cap = Capability(alloc.mapping.write)
        
        try:
            tmpCommand = "{0} {1} {2} {3} {4} {5}\n".format(flags.IBPv031, flags.IBP_STORE, cap.key, cap.wrmKey, size, timeout)
            result = self._dispatch_data(depot, tmpCommand, data)
            if not result:
                return None
            result = result.split(" ")
        except Exception as exp:
            self._log.warn("IBPProtocol.Store [{alloc}]: Could not connect to {depot} - {err}".format(alloc = alloc.id, err = exp, depot=depot))
            return None
            
        if result[0].startswith("-"):
            self._log.warn("IBPProtocol.Store [{alloc}]: Failed to store resource - {err}".format(alloc = alloc.id, err = print_error(result[0])))
            return None

        return duration


    '''
    @input: 
           source      - An Allocation object describing the source allocation
           destination - An Allocation object describing the destination allocation
    @optional:
           duration    - The amount of time, in seconds, that the allocation is reserved.
           timeout     - The amount of time the connection will wait for a response.
           offset      - The data offset.
    @output:
           The duration of the allocation
    '''
    @info("IBP.ProtocolService")
    def send(self, source, destination, **kwargs):
    # Move an allocation from one {source} Depot to one {destination} depot
        timeout     = DEFAULT_TIMEOUT
        duration    = DEFAULT_DURATION
        offset      = 0

        if "timeout" in kwargs:
            timeout = kwargs["timeout"]
        if "duration" in kwargs:
            duration = kwargs["duration"]
        if "offset" in kwargs:
            offset = kwars["offset"]
        size = kwargs.get("size", source.depotSize)

        src_cap  = Capability(source.mapping.read)
        dest_cap = Capability(destination.mapping.write)
        
    # Generate move request with the following form
    # IBPv040[1] IBP_SEND[5] src_read_key src_WRMKey dest_write_cap offset size timeout timeout timeout
        try:
            tmpCommand = "{version} {command} {source} {dest} {keytype} {offset} {size} {timeout} {timeout} {timeout} \n".format(version = flags.IBPv040,
                                                                                                                                 command = flags.IBP_SEND,
                                                                                                                                 source  = src_cap.key,
                                                                                                                                 dest    = str(dest_cap),
                                                                                                                                 keytype = src_cap.wrmKey,
                                                                                                                                 offset  = offset,
                                                                                                                                 size    = source.depotSize,
                                                                                                                                 timeout = timeout)
            result = self._dispatch_command(source.depot, tmpCommand)
            if not result:
                return None
            result = result.split(" ")
        except Exception as exp:
            self._log.warn("IBPProtocol.Send [{alloc}]: Could not connect to {host1}:{port1} - {e}".format(alloc = alloc.id, host1 = source.host, port1 = source.port, e = exp))
            return None

        if result[0].startswith("-"):
            self._log.warn("IBPProtocol.Send [{alloc}]: Failed to move allocation - {err}".format(alloc = alloc.id, err = print_error(result[0])))
            return None
        else:
            return duration
                         


    '''
    @input: 
           alloc - An Allocation object describing the allocation
    @optional:
           timeout     - The amount of time the connection will wait for a response.
    @output:
           The data stored in the allocation
    '''
    @info("IBP.ProtocolService")
    async def load(self, alloc, loop, **kwargs):
        assert alloc.depot
        depot = alloc.depot
        timeout = DEFAULT_TIMEOUT
        offset = 0
        
        if "timeout" in kwargs:
            timeout = kwargs["timeout"]
        if "offset" in kwargs:
            offset = kwargs["offset"]
            
        cap = Capability(alloc.mapping.read)
        
        try:
            tmpCommand = "{version} {command} {key} {wrmkey} {offset} {length} {timeout} \n".format(version = flags.IBPv031, 
                                                                                                    command = flags.IBP_LOAD, 
                                                                                                    key     = cap.key, 
                                                                                                    wrmkey  = cap.wrmKey,
                                                                                                    offset  = offset,
                                                                                                    length  = alloc.size,
                                                                                                    timeout = timeout)
            try:
                result = await loop.run_in_executor(None, self._receive_data, depot, tmpCommand, alloc.size)
            except:
                traceback.print_exc()
                raise IBPError("Failed to download data")
            if not result:
                raise IBPError("Failed to download data")
        except Exception as exp:
            self._log.warn("IBPProtocol.Load [{alloc}]: Could not connect to {d} - {err}".format(alloc = alloc.id, err = exp, d = alloc.depot.endpoint))
            traceback.print_exc()
            return None
            
        if result["headers"].startswith("-"):
            self._log.warn("IBPProtocol.Load [{alloc}]: Failed to store resource - {err}".format(alloc = alloc.id, err = print_error(result["headers"])))
            traceback.print_exc()
            raise IBPError("Error response from server", response=print_error(result["headers"]))
        else:
            return result["data"]


    @debug("IBP.ProtocolService")
    def _receive_data(self, depot, command, size):
        port = int(depot.port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        sock.connect((depot.host, port))

        if isinstance(command, str):
            command = command.encode()
        
        try:
            sock.send(command)
            header = sock.recv(1024)
            if isinstance(header, bytes):
                header = header.decode()
            nl = header.index('\n') # throws exception if not found
            recv = len(header) - nl - 1
            if recv:
                data = header[-recv:].encode()
            else:
                data = b''
            while recv < size:
                rsize = size - recv
                r = sock.recv(rsize)
                data += r
                recv += len(r)
        except socket.timeout as e:
            self._log.warn("Data Socket Timeout - {0} {1}".format(e, rsize))
            self._log.warn("--Attempted to execute: {0}".format(command))
            raise
        except Exception as e:
            self._log.warn("Data Socket error - {0}".format(e))
            self._log.warn("--Attempted to execute: {0}".format(command))
            raise
        finally:
            sock.close()

        return { "headers": header, "data": data }


    @debug("IBP.ProtocolService")
    def _dispatch_data(self, depot, command, data):
        port = int(depot.port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        sock.connect((depot.host, port))

        if isinstance(command, str):
            command = command.encode()
        
        try:
            sock.send(command)
            response = sock.recv(1024)
            if isinstance(response, bytes):
                response = response.decode()
            if not response.startswith("-"):
                sock.sendall(data)
                response = sock.recv(1024)
        except socket.timeout as e:
            self._log.warn("Socket Timeout - {0}".format(e))
            self._log.warn("--Attempted to execute: {0}".format(command))
            traceback.print_exc()
            return None
        except Exception as e:
            self._log.warn("Socket error - {0}".format(e))
            self._log.warn("--Attempted to execute: {0}".format(command))
            traceback.print_exc()
            return None
        finally:
            sock.close()
        
        if isinstance(response, bytes):
            response = response.decode()
            
        return response
        

    @debug("IBP.ProtocolService")
    def _dispatch_command(self, depot, command):
        # Create socket and configure with host and port
        port = int(depot.port)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        sock.connect((depot.host, port))
        
        if isinstance(command, str):
            command = command.encode()
        
        try:
            sock.send(command)
            response = sock.recv(1024)
        except socket.timeout as e:
            self._log.warn("Socket Timeout - {0}".format(e))
            self._log.warn("--Attempted to execute: {0}".format(command))
            traceback.print_exc()
            return None
        except UnicodeDecodeError as e:
            self._log.warn("Bad Unicode response - {}".format(e))
            self._log.warn("--Attempted to execute:{}".format(command))
            traceback.print_exc()
            return None
        except Exception as e:
            self._log.warn("Socket error - {0}".format(e))
            self._log.warn("--Attempted to execute: {0}".format(command))
            traceback.print_exc()
            return None
        finally:
            sock.close()
        
        if isinstance(response, bytes):
            response = response.decode()
            
        return response


def UnitTests():
    depot1 = { "host": "dresci.incntre.iu.edu", "port": 6714 }
    depot2 = { "host": "ibp1.crest.iu.edu", "port": 6714 }
    service = ProtocolService()
    
    status1 = service.GetStatus(depot1)
    status2 = service.GetStatus(depot2)
    
    assert status1 and status2
    print("Dresci: {status}".format(status = status1))
    print("IBP1:   {status}\n".format(status = status2))
    
    data = """Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Vestibulum tortor quam, feugiat vitae, ultricies eget, tempor sit amet, ante. Donec eu libero sit amet quam egestas semper. Aenean ultricies mi vitae est. Mauris placerat eleifend leo. Quisque sit amet est et sapien ullamcorper pharetra. Vestibulum erat wisi, condimentum sed, commodo vitae, ornare sit amet, wisi. Aenean fermentum, elit eget tincidunt condimentum, eros ipsum rutrum orci, sagittis tempus lacus enim ac dui. Donec non enim in turpis pulvinar facilisis. Ut felis. Praesent dapibus, neque id cursus faucibus, tortor neque egestas augue, eu vulputate magna eros eu erat. Aliquam erat volutpat. Nam dui mi, tincidunt quis, accumsan porttitor, facilisis luctus, metus"""
    
    first_alloc = service.Allocate(depot1, len(data))
    second_alloc = service.Allocate(depot2, len(data))
    
    assert first_alloc and second_alloc
    
    alloc_status1 = service.Probe(first_alloc)
    alloc_status2 = service.Probe(second_alloc)
    
    assert alloc_status1 and alloc_status2
    print("Alloc1: {status}".format(status = alloc_status1))
    print("Alloc2: {status}\n".format(status = alloc_status2))
    
    duration = service.Store(first_alloc, data, len(data))
    
    assert duration

    is_ok = service.Manage(first_alloc, duration = 300)

    print(service.Probe(first_alloc))
    
    assert is_ok
    
#    duration2 = service.Send(first_alloc, second_alloc)
    
#    assert duration2
    
#    new_status1 = service.Probe(first_alloc)
#    new_status2 = service.Probe(second_alloc)
    
#    assert new_status1 and new_status2
#    print "Alloc1 (new): {status}".format(status = new_status1)
#    print "Alloc2 (new): {status}\n".format(status = new_status2)

#    data_out = service.Load(second_alloc)

#    assert data_out

#    print data_out

if __name__ == "__main__":
    UnitTests()
