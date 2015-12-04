#!/usr/bin/env python
import thread
import time
import sys
import argparse
from subprocess import call
import signal
import json
import requests
from subprocess import Popen
import urllib

VIZ_HREF = "http://dlt.incntre.iu.edu:42424"
EXTS     = ["gz", "bz", "zip", "jpg", "png"]
TIMEOUT = 10  # In seconds

def signal_handler(signal, frame):
    print('Exiting the program')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

        
def download(host,scenes=None,viz=None,reg=None):
    try:
        if scenes:
            url = host + "?metadata.scene=" + ",".join(scenes) + "&"
        if reg:        
            url = host + "?metadata.scene=reg=" + urllib.quote(reg) + "&"            
        r = requests.get(url)        
        js = r.json()
    except ValueError:
        print "Download exnode metadata from Url " + url  + " failed"
        return    
    results = []
    for i in js:
        href = i['selfRef']
        fname = i['name']
        ext   = fname.split('.')[-1]
        if ext in EXTS:
            try:
                cmd = 'lors_download -t 10 -b 5m -V 1'
                if viz:
                    cmd += ' -X '+viz + ' '
                cmd += ' -f '+ href
                results.append(Popen(cmd.split(" ")))
            except Exception as e:
                print ("ERROR calling lors_download for scene "+ fname + " %s") % e
    for i in results:
        i.wait()
            

def main ():    
    parser = argparse.ArgumentParser(
    description="Listen for and then process a particular LANDSAT scene")
    parser.add_argument('-s', '--scenes', type=str, help='Comma-separated list of scenes to look for')
    parser.add_argument('-r', '--regex', type=str, help='Comma-separated list of scenes to look for')
    parser.add_argument('-H', '--host', type=str, help='The Exnode service',
                        default="http://dev.crest.iu.edu:8888/exnodes")
    parser.add_argument('-X', '--visualize', type=str, help='Enable visualization')
    args = parser.parse_args()    
    host = args.host
    regex = args.regex
    SCENES = args.scenes
    vizurl = args.visualize
    if SCENES:
        print "Downloading SCENES: %s" % SCENES
    elif regex:
        print "Downloading using regex %r" % regex
    else:
        print "Give a regex or scene , use -h to see options"
        return
    
    download(host,SCENES,vizurl,regex)
    
if __name__ == "__main__":
    main()
