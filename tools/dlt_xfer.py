#!/usr/bin/env python3

import os
import argparse
import json
import libdlt
from unis.exceptions import CollectionIndexError
from libdlt.util.common import print_progress

SYS_PATH="/etc/periscope"
USER_DEPOTS=os.path.join(SYS_PATH, "depots.conf")
UNIS_URL = "http://unis.crest.iu.edu:8890"
XFER_TOTAL = 0

def progress(depot, name, total, size, offset):
    global XFER_TOTAL
    if not size:
        XFER_TOTAL = 0
    else:
        XFER_TOTAL += size
    print_progress(XFER_TOTAL, total, name)

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
    parser.add_argument('-d', '--depot-file', type=str, default=None,
                        help='Depots in a JSON dict used for upload')
    parser.add_argument('-o', '--output', type=str, default=None,
                        help='Output file')
    parser.add_argument('-V', '--visualize', type=str, default=None,
                        help='Periscope URL for visualization')
    parser.add_argument('-D', '--debug', type=str, default=None,
                        help='Include verbose logging output')
    parser.add_argument('-t', '--threads', type=int, default=5,
                        help='Number of threads for operation')
    parser.add_argument('-r', '--recursive', action='store_true',
                        help='Recurse into subdirectories')
    parser.add_argument('-c', '--cert', type=str, default=None,
                        help='SSL Cert/Key for HTTPS endpoints')

    args = parser.parse_args()
    bs = args.bs
    df = args.depot_file

    if args.debug in ['TRACE', 'DEBUG']:
        import logging as plogging
        from lace import logging
        plogging.basicConfig(format='%(color)s[%(asctime)-15s] [%(levelname)s] %(name)s%(reset)s %(message)s')
        log = logging.getLogger('libdlt')
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
            print ("{}, trying {}".format(e, USER_DEPOTS))
            try:
                f = open(USER_DEPOTS, "r")
                depots = json.oads(f.read())
            except:
                print ("ERROR: No default depot file: {}".format(USER_DEPOTS))
                exit(1)

    sess = libdlt.Session([{"default": True, "url": args.host, "ssl": args.cert}],
                          bs=bs, depots=depots, threads=args.threads,
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
        try:
            result = xfer(f, folder=args.output, progress_cb=progress)
            diff, res = result.time, result.exnode
        except CollectionIndexError as e:
            print ("ERROR: invalid file or URL: {}".format(e))
            exit(1)
        print ("{0} ({1} {2:.2f} MB/s) {3}".format(res.name, res.size,
                                                   res.size/1e6/diff,
                                                   res.selfRef))


if __name__ == "__main__":
    main()
                
