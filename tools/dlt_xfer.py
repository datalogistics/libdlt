#!/usr/bin/env python3

import os
import argparse
import json
import libdlt
from libdlt.util.common import print_progress

SYS_PATH="/etc/periscope"
USER_DEPOTS=os.path.join(SYS_PATH, "depots.conf")
UNIS_URL = "http://unis.crest.iu.edu:8890"
DEPOTS = {
    "ceph://stark": {
        "enabled": False,
        "client_id": "client.admin",
        "clustername": 'ceph',
        "config": "/etc/ceph/ceph.conf",
        "pool": "test",
        "crush_map": None
    },
    "ceph://um-mon01.osris.org": {
        "enabled": True,
        "client_id": "client.dlt",
        "clustername": 'osiris',
        "config": "/etc/ceph/osiris.conf",
        "pool": "dlt",
        "crush_map": None
    },
    "ibp://ibp2.crest.iu.edu:6714": {
        "enabled": True,
        "duration": 2592000
    }
}

def progress(depot, name, total, size, offset):
    if not size:
        progress.curr = 0
    else:
        progress.curr += size
    print_progress(progress.curr, total, name)

def main():
    parser = argparse.ArgumentParser(description="DLT File Transfer Tool")
    parser.add_argument('files', metavar='FILES', type=str, nargs='+',
                        help='Files to transfer')
    parser.add_argument('-u', '--upload', action='store_true',
                        help='Perform file upload (default is download)')
    parser.add_argument('-H', '--host', type=str, default=UNIS_URL,
                        help='UNIS instance for uploading eXnode metadata')
    parser.add_argument('-b', '--bs', type=str, default='20m',
                        help='Block size')
    parser.add_argument('-d', '--depot-file', type=str, default=USER_DEPOTS,
                        help='Depots in a JSON dict used for upload')
    parser.add_argument('-o', '--output', type=str, default=None,
                        help='Output file')
    parser.add_argument('-V', '--visualize', type=str, default=None,
                        help='Periscope URL for visualization')
    parser.add_argument('-D', '--debug', action='store_true',
                        help='Include verbose logging output')
    parser.add_argument('-t', '--threads', type=int, default=5,
                        help='Number of threads for operation')

    args = parser.parse_args()
    bs = args.bs
    df = args.depot_file
    
    if args.debug:
        libdlt.logging.setLevel(10)
    
    depots = None
    if df:
        try:
            f = open(df, "r")
            depots = json.loads(f.read())
        except Exception as e:
            print ("ERROR: Could not read depot file: {}".format(e))
            exit(1)

    sess = libdlt.Session([{"default": True, "url": args.host}],
                          bs=bs, depots=depots, threads=args.threads,
                          **{"viz_url": args.visualize})
    xfer = sess.upload if args.upload else sess.download
        
    for f in args.files:
        diff, dsize, res = xfer(f, args.output, progress_cb=progress)
        if dsize != res.size:
            print ("\nWARNING: {}: transferred {} of {} bytes \
(check depot file)".format(res.name,
                           dsize,
                           res.size))
        else:
            print ("{0} ({1} {2:.2f} MB/s) {3}".format(res.name, res.size,
                                                       res.size/1e6/diff,
                                                       res.selfRef))

if __name__ == "__main__":
    main()
                
