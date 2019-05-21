import os

def expandvar(x):
    v = os.path.expandvars(x)
    return None if v == x else v

DLT_ROOT = expandvar("$DLT_ROOT") or os.path.expanduser("~/.dlt")

DEPOT_TYPES = ["ceph", "ibp_server"]
BLOCKSIZE = 262144
COPIES = 1
THREADS = 5
TIMEOUT = 180
