# =============================================================================
#  Data Logistics Toolkit (dlt-tools)
#
#  Copyright (c) 2015-2016, Trustees of Indiana University,
#  All rights reserved.
#
#  This software may be modified and distributed under the terms of the BSD
#  license.  See the COPYING file for details.
#
#  This software was created at the Indiana University Center for Research in
#  Extreme Scale Technologies (CREST).
# =============================================================================
#!/usr/bin/env python

import time
import sys
import os
import argparse
import signal
import json
import logging
import common
import subprocess
from pprint import pprint
from common import ExnodeRESTQuery,parseArgs,unis_get

def signal_handler(signal, frame):
    print('Exiting the program')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

def download(count, exdict, viz=False, verbose=False):
    cnt = 1
    for path,xnds in exdict.iteritems():
        pwd = os.getcwd()
        try:
            if not os.path.exists(path):
                os.makedirs(path)
            os.chdir(path)
        except Exception as e:
            logging.error("Could not set output directory: %s" % e)
            pass
        
        for x in xnds:
            try:
                logging.info("[%d of %d] %s/%s" % (cnt, count, path, x["name"]))
                href  = x["selfRef"]
                name  = x["name"]                         
                args = ['lors_download', '-t', '10', '-b', '5m', '-f', href]
                if viz:
                    args.append('-X')
                    args.append(viz)
                p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                out, err = p.communicate()
                if verbose:
                    print err
                elif "ERROR" in err:
                    print err
                cnt += 1
            except Exception as e:
                logging.error("Failed lors_download for %s: %s " % (name, e))
                
        os.chdir(pwd)

def get_exdict(rq, parent=None, path=None, rec=False, ssl=False, exdict={}):
    cnt = 0
    js  = []
    if not path:
        path = "."
    url = rq.url(parent)
    js = unis_get(url, ssl)
    for j in js:
        try:
            if j["mode"] == "file":
                logging.debug("Queue download: %s" % (path+"/"+j["name"]))
                if path in exdict:
                    exdict[path].append(j)
                else:
                    exdict[path] = [j]
                cnt += 1
            elif j["mode"] == "directory" and rec:
                logging.debug("Recursing into: %s" % j["name"])
                npath = path + "/" + j["name"]
                c,ed = get_exdict(rq, j["selfRef"], npath, rec, ssl, exdict)
                cnt += c
            else:
                logging.info("Skipping directory %s" % j["name"])
        except Exception as e:
            logging.error("Failed to process UNIS response: %s" % e)
            sys.exit(1)

    return (cnt,exdict)

def main ():
    args = parseArgs(desc="EODN-IDMS Download Tool",
                     ptype=common.PARSER_TYPE_DOWNLOAD)
    try:
        rq = ExnodeRESTQuery(args)
        c,ed = get_exdict(rq, None, None,
                          args.recursive,
                          args.ssl)

        if not c:
            logging.info("Found %d files" % c)
        else:
            logging.debug("Found %d files in %d directories" % (c, len(ed)))
        
        if (args.list):
            print "total: %d" % c
            for k,v in ed.iteritems():
                print k+":"
                for n in v:
                    print "\t"+n["name"]
        else:
            download(c, ed, args.visualize, args.verbose)
            
    except Exception as e:
        logging.error("%s" % e)
        sys.exit(1)
    
if __name__ == "__main__":
    main()
