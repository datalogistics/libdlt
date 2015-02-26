#!/usr/bin/python

import sys
import os
import argparse
import requests
import json
from lxml import etree
from subprocess import call
from unisencoder.decoder import ExnodeDecoder

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

def unis_import(xndfile, scene_id):
    print 'Importing exnode to UNIS'

    info = os.stat(xndfile)
    creation_time = int(info.st_ctime)
    modified_time = int(info.st_mtime)
    
    kwargs = dict(creation_time = creation_time,
                  modified_time = modified_time)
    
    encoder = ExnodeDecoder()
    xnd = etree.parse(xndfile)
    uef = encoder.encode(xnd, **kwargs)
    
    scene_meta = {'properties':
                      {'metadata':
                           {'scene_id': scene_id}}}
    uef.update(scene_meta)

    header = {'content-type': 'application/perfsonar+json'}
    ret = requests.post(EXNODE_URL, data=json.dumps(uef), headers=header)

def main():
    parser = argparse.ArgumentParser(
        description="Import a Landsat image file into EODN")
    parser.add_argument('-f', '--file', type=str, help='The file to upload', required=True)
    parser.add_argument('-s', '--scene', type=str, help='The scene ID', required=True)

    args = parser.parse_args()
    
    xndfile = args.file+".xnd"

    ret = upload_to_eodn(xndfile, args.file)
    if ret:
        exit(1)
    unis_import(xndfile, args.scene)

    print "Done!"

if __name__ == '__main__':
    main()
