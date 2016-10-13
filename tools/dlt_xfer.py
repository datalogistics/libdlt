#!/usr/bin/env python3

import argparse
from pprint import pprint

import libdlt
from unis.runtime import Runtime

UNIS_URL = "http://localhost:8888"
DEPOTS = {
    "ceph://stark": {
        "clustername": 'ceph',
        "config": "/etc/ceph/ceph.conf",
        "pool": "test",
        "crush_map": None
            },
    "ceph://um-mon01.osris.org": {
        "clustername": 'osiris',
        "config": "/etc/ceph/osiris.conf",
        "pool": "dlt",
        "crush_map": None
    },
#    "ibp://ibp2.crest.iu.edu:6714": {
#        "max_alloc_lifetime": 2592000
#    }
}

def main():
    parser = argparse.ArgumentParser(description="DLT File Transfer Tool")
    parser.add_argument('files', metavar='FILES', type=str, nargs='+',
                        help='Files to transfer')
    parser.add_argument('-u', '--upload', action='store_true',
                        help='Perform file upload (default is download)')
    parser.add_argument('-H', '--host', type=str, default=UNIS_URL,
                        help='UNIS instance for uploading eXnode metadata')
    parser.add_argument('-b', '--bs', type=str, default='5m',
                        help='Block size')

    args = parser.parse_args()
    bs = args.bs
    rt = Runtime(args.host, defer_update=True)
    
    sess = libdlt.Session(runtime=rt, blocksize=bs, depots=DEPOTS)
    xfer = sess.upload if args.upload else sess.download
        
    for f in args.files:
        diff, res = xfer(f)
        print ("{0} ({1} {2:.2f} MB/s) {3}".format(res.name, res.size,
                                                   res.size/1e6/diff,
                                                   res.selfRef))

if __name__ == "__main__":
    main()
                