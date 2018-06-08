#!/usr/bin/env python3

import os
import argparse
import json
import libdlt

UNIS_URL = "http://localhost:8888"
DEPOTS = {
    #"ceph://stark": {
    #    "clustername": 'ceph',
    #    "config": "/etc/ceph/ceph.conf",
    #    "pool": "test",
    #    "crush_map": None
    #},
    "ceph://um-mon01.osris.org": {
        "clustername": 'osiris',
        "config": "/etc/ceph/osiris.conf",
        "pool": "dlt",
        "crush_map": None
    },
    "ibp://ibp2.crest.iu.edu:6714": {
        "duration": 2592000
    }
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
    parser.add_argument('-d', '--depot-file', type=str, default=None,
                        help='Depots in a JSON dict used for upload')
    parser.add_argument('-o', '--output', type=str, default=None,
                        help='Output file')
    parser.add_argument('-V', '--visualize', type=str, default=None,
                        help='Periscope URL for visualization')
    parser.add_argument('-D', '--debug', type=str, default=None
                        help='Include verbose logging output')
    parser.add_argument('-t', '--threads', type=int, default=5,
                        help='Number of threads for operation')
    parser.add_argument('-r', '--recursive', action='store_true',
                        help='Recurse into subdirectories')

    args = parser.parse_args()
    bs = args.bs
    df = args.depot_file

    if args.debug in ['TRACE', 'DEBUG']:
        from lace import logging
        log = logging.getLoger('libdlt')
        log.setLevel(logging.DEBUG)
        if args.debug == 'TRACE':
            from lace.logging import trace
            trace.setLevel(logging.DEBUG, True)
    depots = None
    if df:
        try:
            f = open(df, "r")
            depots = json.loads(f.read())
        except Exception as e:
            print ("ERROR: Could not read depot file: {}".format(e))
            exit(1)

    sess = libdlt.Session(args.host, bs=bs, depots=depots, threads=args.threads,
                          **{"viz_url": args.visualize})
    xfer = sess.upload if args.upload else sess.download

    flist = []
    for f in args.files:
        if args.recursive and os.path.isdir(f):
            for dirpath, dirnames, files in os.walk(f):
                for n in files:
                    flist.append(os.path.join(dirpath, n))
        else:
            flist.append(f)

    for f in flist:
        result = xfer(f, folder=args.output)
        diff, res = result.time, result.exnode
        print ("{0} ({1} {2:.2f} MB/s) {3}".format(res.name, res.size,
                                                   res.size/1e6/diff,
                                                   res.selfRef))


if __name__ == "__main__":
    main()
                
