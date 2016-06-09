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
    def __init__(self, rq, viz, verbose):
        self._rq  = rq
        self._viz = viz
        self._verbose = verbose
        
    def on_message(self, ws, message):
        href = None
        name = None
        try:
            js   = json.loads(message)
            href = js["selfRef"]
            name = js["name"]
            logging.info("Downloading file %s" % js["name"])
        except Exception as e:
            logging.warn("Failed to decode eXnode: %s" % e)
            logging.debug(message)
            return
        
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
    
    def close_handler(self):
        while not self.start():
            time.sleep(10)
            logging.info("Attempting to reconnect...")
            
    def on_close(self, ws):
        if SHUTDOWN:
            return
        
        logging.warn("Remote host closed the connection")
        logging.info("Attempting to reconnect...")
        self.close_handler()
        
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
        ws.on_open = self.on_open
        ws.run_forever()
        
def main ():
    args = parseArgs(desc="EODN-IDMS Subscription Tool",
                     ptype=common.PARSER_TYPE_PUBSUB)
    rq = ExnodePUBSUBQuery(args)
    listener = Listener(rq, args.visualize, args.verbose)
    listener.start()
    
if __name__ == "__main__":
    main()
