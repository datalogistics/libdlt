import argparse
import itertools
import json
import random

from uritools import urisplit

from libdlt.schedule import AbstractSchedule, BaseDownloadSchedule
from libdlt import Session
from libdlt.util import util

class SpreadDownloadSchedule(AbstractSchedule):
    def __init__(self, start, first, second):
        self._start = start
        self._first = first
        self._second = itertools.cycle(second)
        
        
    def setSource(self, source):
        self._size = 0
        chunks = {}
        for ext in source:
            if ext.offset not in chunks:
                self._size = max(self._size, ext.offset + ext.size)
                chunks[ext.offset] = []
            chunks[ext.offset].append(ext)
        self._ls = chunks
    
    def get(self, context):
        options = self._ls[context["offset"]]
        if context["offset"] < self._size * self._start:
            for ext in options:
                uri = urisplit(ext.location)
                loc = "{}://{}".format(uri.scheme, str(uri.authority))
                if loc == self._first:
                    print("Selecting:", ext.location)
                    return ext
            print("Primary NOT FOUND")
            return ext
        else:
            choice = next(self._second)
            for ext in options:
                uri = urisplit(ext.location)
                loc = "{}://{}".format(uri.scheme, str(uri.authority))
                if loc == choice:
                    print("Selecting:", ext.location)
                    return ext
            print("Secondary NOT FOUND")
            return ext

class FallbackDownloadSchedule(AbstractSchedule):
    def __init__(self, url, threshold, primary, fallback, md_id):
        def bandwidth(x, ts, prior, state):
            result = (x - state["prev"]) / (ts - state["ts"])
            state["prev"] = x
            state["ts"] = ts
            return result
        def pp(x, state):
            return (x - state["prev"]) / state["delta"]
        self._runtime = Runtime(url)
        self._data = next(self._runtime.metadata.where({"id": md_id})).data
        self._threshold = threshold
        self._primary = primary
        self._fallback = fallback
        
        self._data.attachFunction("bandwidth", bandwidth, state={ "prev": 0, "ts": 0 })
        
    def setSource(self, source):
        chunks = {}
        for ext in source:
            if ext.offset not in chunks:
                chunks[ext.offset] = []
            chunks[ext.offset].append(ext)
        self._ls = chunks
    
    def get(self, context):
        chunks = []
        offset = context["offset"]
        if offset in self._ls and self._ls[offset]:
            chunks = (offset, self._ls[offset])
        else:
            result = None
            for k, chunk in self._ls:
                if k < offset:
                    for ext in chunk:
                        if ext.size + ext.offset > offset:
                            chunks.append((k, ext))
                            
        fallback = None
        primary = None
        for offset, alloc in chunks:
            if alloc.location == self._fallback:
                fallback = alloc
            elif alloc.location == self._primary:
                primary = alloc
                        
        if self._data.last > self._threshold:
            if fallback:
                return fallback
        if primary:
            return primary
        else:
            raise IndexError("No more allocations fulfill request: offset ~ {}".format(offset))
            

class UploadSchedule(AbstractSchedule):
    def __init__(self, destination):
        self._dest = destination
    def setSource(self, source):
        pass

    def get(self, context):
        return self._dest


def main():
    parser = argparse.ArgumentParser(description="DLT File Transfer Tool")
    parser.add_argument('files', metavar='FILES', type=str, nargs='+',
                        help='Files to copy')
    parser.add_argument('-I', '--input-depot-file', type=str, default='.indepots',
                        help='Depots in a JSON dict used for upload')
    parser.add_argument('-O', '--output-depot-file', type=str, default='.outdepots',
                        help='Depots in a JSON dict used for upload')
    parser.add_argument('-V', '--visualize', type=str, default=None,
                        help='Periscope URL for visualization')
    parser.add_argument('-m', '--mode', default='NONE',
                        help='Set policy mode [NONE, FALLBACK, SPLIT, SPREAD')
    parser.add_argument('-o', '--output', type=str,
                        help='Override copy operation with download')
    parser.add_argument('-t', '--threads', type=int, default=5,
                        help='Sets the number of threads for the process')
    parser.add_argument('-T', '--threshold', type=str, default='5m',
                        help='Threshold at which download switches to fallback')
    parser.add_argument('-d', '--debug', action='store_true',
                        help='Include verbose debug output')
    
    args = parser.parse_args()
    idf = args.input_depot_file
    odf = args.output_depot_file
    
    in_depots = None
    out_depots = None
    depots = {}
    if args.debug:
        import libdlt
        libdlt.logging.setLevel(10)
        
    if idf:
        try:
            f = open(idf, "r")
            in_depots = json.loads(f.read())
        except Exception as e:
            print ("ERROR: Could not read depot file: {}".format(e))
            exit(1)
    
    if odf:
        try:
            f = open(odf, "r")
            out_depots = json.loads(f.read())
        except Exception as e:
            print("ERROR: Could not read depot file: {}".format(e))

    for k,v in in_depots.items():
        depots[k] = v
    for k,v in out_depots.items():
        depots[k] = v
    threads = args.threads
    up_sched = UploadSchedule(list(out_depots.keys())[0])
    url = "http://dev.crest.iu.edu:8888"
    threshold = int(util.human2bytes(args.threshold))
    md_id = ""
    out_depots = list(out_depots.keys())
    in_depots = list(in_depots.keys())
    if args.mode == 'FALLBACK':
        print("Fallback Mode Selected")
        down_sched = DownloadSchedule(url, threshold, in_depots[0], in_depots[1], md_id)
    elif args.mode == 'SPLIT':
        print("Split Mode Selected")
        down_sched = SpreadDownloadSchedule(0.25 + 0.5 * random.random(), in_depots[0], out_depots)
    elif args.mode == 'SPREAD':
        print("Spread Mode Selected")
        all_depots = in_depots + out_depots
        down_sched = SpreadDownloadSchedule(0.25 + 0.5 * random.random(), in_depots[0], all_depots)
    else:
        print("No Mode Selected")
        down_sched = BaseDownloadSchedule()
        
    kwargs = {}
    kwargs = { "viz_url": args.visualize } if args.visualize else {}
    sess = Session(url, depots, threads, **kwargs)
    for f in args.files:
        if args.output:
            print("Downloading")
            diff, res = sess.download(f, args.output, schedule=down_sched)
        else:
            print("Copying")
            diff, res = sess.copy(f, upload_schedule=up_sched, download_schedule=down_sched)

        print ("{0} ({1} {2:.2f} MB/s) {3}".format(res.name, res.size,
                                                   res.size/1e6/diff,
                                                   res.selfRef))


if __name__ == "__main__":
    main()
