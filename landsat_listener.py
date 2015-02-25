#!/usr/bin/env python
import websocket
import thread
import time
import sys
import json

def on_message(ws, message):
    js = json.loads(message)    
    print "SCENE   : %s" % js['properties']['metadata']['scene_id']
    print "Filename: %s" % js['name']
    print "Size    : %s" % js['size']
    print "URL     : %s" % js['selfRef']

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    pass

if __name__ == "__main__":
    #websocket.enableTrace(True)
    if len(sys.argv) < 2:
        host = "ws://dev.incntre.iu.edu:8888/subscribe/exnode"
    else:
        host = sys.argv[1]
    ws = websocket.WebSocketApp(host,
                                on_message = on_message,
                                on_error = on_error,
                                on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()
