#!/usr/bin/env python
import websocket
import thread
import time
import sys
import argparse
from subprocess import call
import signal
import json
import requests
from subprocess import Popen

VIZ_HREF = "http://dlt.incntre.iu.edu:42424"
EXTS     = ["gz", "bz", "zip", "jpg", "png"]
TIMEOUT = 10  # In seconds

def signal_handler(signal, frame):
    print('Exiting the program')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

        
def download(host,scenes):
    try:
        url = host + "?metadata.scene=" + ",".join(scenes)
        r = requests.get(url)        
        js = r.json()
    except ValueError:
        print "Download exnode metadata from Url " + url  + " failed"
        return 

    for i in js:
        href = i['selfRef']
        fname = i['name']
        ext   = fname.split('.')[-1]
        if ext in EXTS:
            try:
                results = Popen(['lors_download', '-t', '10', '-b', '5m', '-V', '1', '-X', VIZ_HREF, '-f', href])
            except Exception as e:
                print "ERROR calling lors_download %s" % e

def main ():    
    parser = argparse.ArgumentParser(
        description="Listen for and then process a particular LANDSAT scene")
    parser.add_argument('-s', '--scenes', type=str, help='Comma-separated list of scenes to look for', required=True)
    parser.add_argument('-H', '--host', type=str, help='The Exnode service',
                        default="http://dev.crest.iu.edu:8888/exnodes")
    args = parser.parse_args()    
    #websocket.enableTrace(True)
    host = args.host
    SCENES = args.scenes.split(',')
    print "Downloading SCENES: %s" % SCENES
    download(host,SCENES)
    
if __name__ == "__main__":
    main()
