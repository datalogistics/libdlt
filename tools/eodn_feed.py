#!/usr/bin/env python

import websocket
import time
import sys
import json
import subprocess
import logging
import signal
import libdlt

from libdlt.util import common as common
from libdlt.util.common import ExnodePUBSUBQuery, parseArgs

DEPOTS = '{"ceph://um-mon01.osris.org": {"clustername": "osiris","config": "/etc/ceph/osiris.conf","pool": "dlt","crush_map": null}, "ibp://ibp2.crest.iu.edu:6714": {"duration": 2592000}}'
SHUTDOWN = False

def signal_handler(signal, frame):
    global SHUTDOWN
    logging.info("Exiting...")
    SHUTDOWN = True
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

class Listener(object):
    def __init__(self, rq, viz, verbose, vlist):
        self._rq  = rq
        self._viz = viz
        self._verbose = verbose
        self._list = vlist
        
    def on_message(self, ws, message):
        href = None
        name = None
        try:
            js   = json.loads(message)
            if not js["headers"]["action"] == "POST":
                return
            else:
                js = js["data"]
            href = js["selfRef"]
            name = js["name"]
            logging.info("Matching file %s [%d bytes]" % (js["name"], js["size"]))
        except Exception as e:
            logging.warn("Failed to decode eXnode: %s" % e)
            logging.debug(message)
            return
        
        if not self._list:
            try:
                depots = None
                block_size = '5m'
                host = "http://dev.crest.iu.edu:8888"

                if DEPOTS:
                    try:
                        depots = json.loads(DEPOTS)
                    except Exception as e:
                        print ("ERROR: Could not read depots: {}".format(e))
                        exit(1)

                sess = libdlt.Session(host, bs=block_size, depots=depots,
                                                        **{"viz_url": self._viz})
                xfer = sess.download
                result = xfer(href, None)
                diff, res = result.time, result.exnode
                print ("{0} ({1} {2:.2f} MB/s) {3}".format(res.name, res.size,
                                                           res.size/1e6/diff,
                                                           res.selfRef))
            except Exception as e:
                logging.error("Failed libdlt download for %s: %s " % (name, e))
            
    def on_error(self, ws, error):
        logging.warn("Websocket error - {exp}".format(exp = error))
    
    def on_close(self, ws):
        logging.warn("Remote connection lost")
        
    def on_open(self, ws):
        logging.info("Connected to %s" % self._rq.url())
        logging.info("Adding query %s" % self._rq.query())
        query = { "query": self._rq.query(), "resourceType": self._rq.ctype }
        ws.send(json.dumps(query))
        logging.info("Listening for EODN-IDMS eXnodes...")
        
    def start(self):
        ws = websocket.WebSocketApp(self._rq.url(),
                                    on_message = self.on_message,
                                    on_error = self.on_error,
                                    on_close = self.on_close)
        self._ws = ws
        ws.on_open = self.on_open
        ws.run_forever()
        
def main ():
    args = parseArgs(desc="EODN-IDMS Subscription Tool",
                     ptype=common.PARSER_TYPE_PUBSUB)
    rq = ExnodePUBSUBQuery(args)
    listener = Listener(rq, args.visualize,
                        args.verbose, args.list)

    while True:
        listener.start()
        time.sleep(5)
        logging.info("Attempting to reconnect...")
    
if __name__ == "__main__":
    main()
