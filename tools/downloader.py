#!/usr/bin/env python
import thread
import time
import sys
import argparse
from subprocess import call
import signal
import json
import logging
import requests
from subprocess import Popen
import cmdparser
import urllib

VIZ_HREF = "http://dlt.incntre.iu.edu:42424"
EXTS     = ["gz", "bz", "zip", "jpg", "png"]
TIMEOUT = 10  # In seconds

def signal_handler(signal, frame):
    print('Exiting the program')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)


def runLors(exlist,viz=False):
    results = []
    for i in exlist:
        print i
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
                logging.info ("ERROR calling lors_download for scene "+ fname + " with error " + str(e)) 
    for i in results:
        i.wait()

def download(host,info,scenes=True,viz=False,reg=False,folder=False,ssl=False,verbose=False):
    try:
        url = host + "/exnodes"
        fieldStr = "&fields=selfRef,name&mode=file"
        if reg:        
            url += "?metadata.scene=reg=" + urllib.quote(info) + fieldStr
        elif folder:
            url += "?parent=recfind=" + info + fieldStr
        elif scenes:
            """ Process Scene list boolean last since it is always set to true by default """
            url+= "?metadata.scene=" + info + fieldStr


        logging.info("Url used is " + url)
        if ssl :
            r = requests.get(url,cert=('./dlt-client.pem','./dlt-client.pem'),verify=False)
        else:
            r = requests.get(url)            
        js = r.json()
        if not js:
            logging.info("Probably incorrect arguments - or something failed ")
            logging.debug("Url used is " + url + " - Please ensure that unis supports all used features ")
            return
        
        logging.debug("Response from UNIS " + str(js))
        runLors(js,viz)
    except ValueError:
        logging.info("Download exnode metadata from Url " + url  + " failed")
        return    
    
            
        
def main ():
    args = cmdparser.parseArgs()
    info = args.sceneInfo
    host = args.host
    regex = args.regex
    ssl = args.ssl
    scenes = args.scenes
    folder = args.folder
    vizurl = args.visualize    
    typeStr = 'Scene list' if scenes else 'Regex' if regex else 'Folder Id' if folder else  "Unknown"
    logging.info("Downloading Using info: "+  info +  " and tpye :  " + typeStr)
    download(host,info,scenes,vizurl,regex,folder,ssl)
    
if __name__ == "__main__":
    main()
