#!/usr/bin/env python3

import argparse, json, logging
import libdlt

from urllib.parse import urlparse, urlunparse

def main():
    parser = argparse.ArgumentParser(description="Transfer files using the Datalogistic Toolkit with PID tracking")
    parser.add_argument('-d', '--depotfile', type=str, default=None,  help='Filename for JSON containing depot '
                        'information.  { <depot_url>: { enabled: ..., **keywords } }')
    parser.add_argument('-b', '--blocksize', type=str, default='20m', help='Block size for individual allocations')
    parser.add_argument('-V', '--visualize', type=str, help='URL to Web DLT server for data-movement visualization')
    parser.add_argument('-t', '--threads', type=int, default=5, help='Number of data transfer threads')
    parser.add_argument('--verbose', action='store_true', help='Display verbose output')
    parser.add_argument('source', type=str, help='Transfer source.  For local files, this should be a '
                        'relative or absolute path.  For remote files, this should be a full URL to the file '
                        'description.')
    parser.add_argument('destination', type=str, nargs='?', help='Transfer source.  For local files, this should be '
                        'a relative or absolute path.  For remote files, this should be a full URL to the file '
                        'description.')

    args = parser.parse_args()
    if args.verbose: logging.getLogger('libdlt').setLevel(logging.DEBUG)
    src, dst = urlparse(args.source), urlparse(args.destination)
    if not (src.scheme or dst.scheme):
        print('ERROR: Unknown formatting in source or destination.  One must follow '
              '<scheme>://<host>[:<port>][/<path>]')
    remote, local, is_upload = (src, dst, False) if src.scheme else (dst, src, True)
    ty, host = ('U' if is_upload else 'D'), "{}://{}".format(remote.scheme, remote.netloc)

    depots = None
    if args.depotfile:
        try:
            with open(args.depotfile) as f:
                depots = json.load(f)
        except Exception as e:
            print("ERROR: Could not read depot file - {}".format(e))
    
    name = None if local.path == '.' else local.path
    kwds = { 'bs': args.blocksize, 'threads': args.threads, 'viz_url': args.visualize, 'depots': depots }
    if depots: kwds['depots'] = depots
    
    with libdlt.Session([{'default': True, 'url': host}], **kwds) as sess:
        res = (sess.upload if is_upload else sess.download)(urlunparse(src), filename=name)
        records = sorted(filter(lambda x: x[0] == ty, sess.get_record()), key=lambda x: x[2])

        

        print("Transaction Record [{}]:".format('Upload' if is_upload else 'Download'))
        for ty, alloc, *meta in records:
            print("\t{} [{}-{}]".format(alloc.id, meta[0], meta[0] + meta[1]))
        print(res.exnode.selfRef)
