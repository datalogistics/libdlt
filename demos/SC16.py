import argparse
import json

from libdlt.schedule import AbstractSchedule, BaseDownloadSchedule
from libdlt import Session
from libdlt.util import util

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
    parser.add_argument('-i', '--input-depot-file', type=str, default='.indepots',
                        help='Depots in a JSON dict used for upload')
    parser.add_argument('-o', '--output-depot-file', type=str, default='.outdepots',
                        help='Depots in a JSON dict used for upload')
    parser.add_argument('-V', '--visualize', type=str, default=None,
                        help='Periscope URL for visualization')
    parser.add_argument('-f', '--fallback', action='store_true',
                        help='Set policy to fallback mode')
    parser.add_argument('-t', '--threshold', type=str, default='5m',
                        help='Threshold at which download switches to fallback')
    parser.add_argument('-d', '--debug', action='store_true',
                        help='Include verbose debug output')
    
    args = parser.parse_args()
    bs = args.bs
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
    url = "http://dev.crest.iu.edu:8888"
    threshold = int(util.human2bytes(args.threshold))
    md_id = ""
    up_sched = UploadSchedule(list(out_depots.keys())[0])
    if args.fallback:
        down_sched = DownloadSchedule(url, threshold, in_depots[0], in_depots[1], md_id)
    else:
        down_sched = BaseDownloadSchedule()
        
    sess = Session(url, depots)
    for f in args.files:
        diff, res = sess.copy(f, upload_schedule=up_sched, download_schedule=down_sched)

        print ("{0} ({1} {2:.2f} MB/s) {3}".format(res.name, res.size,
                                                   res.size/1e6/diff,
                                                   res.selfRef))


if __name__ == "__main__":
    main()
