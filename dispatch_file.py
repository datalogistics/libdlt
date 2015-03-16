#!/usr/bin/python

import sys
import os
import argparse
import requests
import json
from lxml import etree
from subprocess import call
from unisencoder.decoder import ExnodeDecoder
import unisencoder.dispatcher as UnisDispatch


EXNODE_URL="http://dev.incntre.iu.edu:8888/exnodes"

def upload_to_eodn(xnd_file, file):
    results = '0'
    retry = 1
    for i in range(retry):
        try:
            results = call(['lors_upload', '--duration=1h', '--none', '-c', '1', '--depot-list',
                            '-t', '10', '-b', '10m','-V', '1', '-o',
                            xnd_file, file])
            if results == 0:
                break
        except Exception as e:
            print "ERROR calling lors_upload: %s" % e
            
    return results

def unis_import(xndfile, scene_id, exdir):
    print 'Importing exnode to UNIS'
    dispatch = UnisDispatch.Dispatcher()
    root = UnisDispatch.create_remote_directory(exdir, None)
    extended_dir = UnisDispatch.parse_filename(xndfile.split('/')[-1])
    parent = UnisDispatch.create_directories(extended_dir, root)
    dispatch.DispatchFile(xndfile, parent, metadata = { "scene_id": scene_id })

def main():
    parser = argparse.ArgumentParser(
        description="Import a Landsat image file into EODN")
    parser.add_argument('-f', '--file', type=str, help='The file to upload', required=True)
    parser.add_argument('-s', '--scene', type=str, help='The scene ID', required=True)
    parser.add_argument('-d', '--dir', type=str, help='The parent directory name', required=True)

    args = parser.parse_args()
    
    xndfile = args.file+".xnd"

    ret = upload_to_eodn(xndfile, args.file)
    if ret:
        exit(1)

    unis_import(xndfile, args.scene, args.dir)

    print "Done!"

if __name__ == '__main__':
    main()
