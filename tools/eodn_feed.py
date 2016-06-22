# =============================================================================
#  Data Logistics Toolkit (dlt-tools)
#
#  Copyright (c) 2015-2016, Trustees of Indiana University,
#  All rights reserved.
#
#  This software may be modified and distributed under the terms of the BSD
#  license.  See the COPYING file for details.
#
#  This software was created at the Indiana University Center for Research in
#  Extreme Scale Technologies (CREST).
# =============================================================================
#!/usr/bin/env python

import websocket
import time
import sys
import json
import subprocess
import logging
import signal
import common
from common import ExnodePUBSUBQuery, parseArgs

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
                args = ['lors_download', '-t', '10', '-b', '5m', '-f', href]
                if self._viz:
                    args.append('-X')
                    args.append(self._viz)
                p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                out, err = p.communicate()
                if self._verbose:
                    print err
                elif "ERROR" in err:
                    print err
            except Exception as e:
                logging.error("Failed lors_download for %s: %s " % (name, e))
            
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
