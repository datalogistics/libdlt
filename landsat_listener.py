#!/usr/bin/env python
import websocket
import thread
import time
import sys
import json
import argparse
from subprocess import call

def on_message(ws, message):
    global SCENE
    FILES = [SCENE+".tar.gz", SCENE+".tar.bz"]

    js = json.loads(message)    
    print "SCENE   : %s" % js['properties']['metadata']['scene_id']
    print "Filename: %s" % js['name']
    print "Size    : %s" % js['size']
    print "URL     : %s" % js['selfRef']

    if (SCENE == js['properties']['metadata']['scene_id'] and js['name'] in FILES):
        print "\n### Matching SCENE and Filename found, processing...."
        try:
            results = call(['/home/kissel/repos/bddlt/misc/process_landsat.sh', SCENE, js['selfRef']])
        except Exception as e:
            print "ERROR calling process_landsat.sh %s" % e

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    pass

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Listen for and then process a particular LANDSAT scene")
    parser.add_argument('-s', '--scene', type=str, help='The SCENE to look for', required=True)
    parser.add_argument('-H', '--host', type=str, help='The Exnode service',
                        default="ws://dev.incntre.iu.edu:8888/subscribe/exnode")

    args = parser.parse_args()

    global SCENE
    #websocket.enableTrace(True)
    host = args.host
    SCENE = args.scene

    ws = websocket.WebSocketApp(host,
                                on_message = on_message,
                                on_error = on_error,
                                on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()
    
