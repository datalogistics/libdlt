#!/usr/bin/python

import sys
import os.path
import argparse
from subprocess import call
import unisencoder.dispatcher as unisDispatch

def upload_to_eodn(xnd_file, file):
    results = '0'
    retry = 1
    for i in range(retry):
        try:
            results = call(['lors_upload', '--duration=1h', '--none', '-c', '1', '-H',
                            'dlt.incntre.iu.edu', '-n', '-t', '10', '-b', '10m','-V', '1', '-o',
                            xnd_file, file])
            if results == 0:
                break
        except Exception as e:
            print "ERROR calling lors_upload: %s" % e
            
    return results

def unis_import(xnd_filename, xnd_path, scene_id):
    print 'Importing exnode to UNIS'
    dispatch = unisDispatch.Dispatcher()
    unis_root = unisDispatch.create_remote_directory("root", None)
    extended_dir = unisDispatch.parse_filename(xnd_filename)
    
    parent = unisDispatch.create_directories(extended_dir, unis_root)
    dispatch.DispatchFile(xnd_path, parent, metadata = { "scene_id": scene_id })


def main():
    parser = argparse.ArgumentParser(
        description="Import a Landsat image file into EODN")
    parser.add_argument('-f', '--file', type=str, help='The file to upload', required=True)
    parser.add_argument('-s', '--scene', type=str, help='The scene ID', required=True)

    args = parser.parse_args()
    
    xndfile = args.file+".xnd"

    ret = upload_to_eodn(xndfile, args.file)
    unis_import(xndfile, xndfile, args.scene)

if __name__ == '__main__':
    main()
