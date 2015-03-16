#!/usr/bin/env python
import websocket
import thread
import time
import sys
import json
import argparse
from subprocess import call

VIZ_HREF = "http://dlt.incntre.iu.edu:42424"
EXTS     = ["gz", "bz", "zip", "jpg", "png"]

def on_message(ws, message):
    global SCENES

    js = json.loads(message)
    try:
        href  = js['selfRef']
        curr  = js['properties']['metadata']['scene_id']
        fname = js['name']
        size  = js['size']
        ext   = fname.split('.')[-1]
    except:
        #print "Not a valid exnode for download, skipping: %s" % js
        return

    print "SCENE   : %s" % curr
    print "Filename: %s" % fname
    print "Size    : %s" % size
    print "URL     : %s" % href

    if (curr in SCENES and ext in EXTS):
        print "\n### Matching SCENE and Filename found, processing...."
        try:
            results = call(['lors_download', '-t', '10', '-b', '1m', '-V', '1', '-X', VIZ_HREF, '-f', href])
        except Exception as e:
            print "ERROR calling lors_download %s" % e

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    pass

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Listen for and then process a particular LANDSAT scene")
    parser.add_argument('-s', '--scenes', type=str, help='Comma-separated list of scenes to look for', required=True)
    parser.add_argument('-H', '--host', type=str, help='The Exnode service',
                        default="ws://dev.incntre.iu.edu:8888/subscribe/exnode")

    args = parser.parse_args()

    global SCENES
    #websocket.enableTrace(True)
    host = args.host
    SCENES = args.scenes.split(',')

    print "Listening for SCENES: %s" % SCENES

    ws = websocket.WebSocketApp(host,
                                on_message = on_message,
                                on_error = on_error,
                                on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()
    
