import sys
import argparse
import logging
import urllib
import requests

# http://stackoverflow.com/questions/3173320/text-progress-bar-in-the-console/34325723#34325723
def print_progress(iteration, total, prefix='', suffix='', decimals=1, bar_length=80, fill='='):
    """
    Call in a loop to create terminal progress bar

    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        bar_length  - Optional  : character length of bar (Int)
    """
    str_format = "{0:." + str(decimals) + "f}"
    percents = str_format.format(100 * (iteration / float(total)))
    filled_length = int(round(bar_length * iteration / float(total)))
    bar = fill * filled_length + '-' * (bar_length - filled_length)

    sys.stdout.write('\r%s |%s| %s%s %s' % (prefix, bar, percents, '%', suffix)),

    if iteration == total:
        sys.stdout.write('\n')
    sys.stdout.flush()

class ExnodeRESTQuery:
    def __str__(self):
        return self.url(self.parent)
        
    def url(self, parent=None):
        rq = self.baseq
        if parent:
            rq += "&parent.href=" + parent
        elif self.parent:
            rq += "&parent.href=" + self.parent
        if self.scenes:
            rq += "&mode=directory|metadata.scene=" + self.scenes
        if self.regex:
            rq += "&mode=directory|metadata.scene=reg=" + urllib.quote(self.regex)
        if self.filt:
            rq += "&mode=directory|name=reg=" + urllib.quote(self.filt)
        return self.ep + rq        
    
    def __init__(self, args):
        # selection criteria
        self.path = args.path
        self.scenes = args.scenes
        self.regex = args.regex
        self.filt = args.filter
        self.parent = None
        
        # set the endpoint
        self.ep = args.url + "/exnodes"
        # always ask for minimum set of fields needed
        self.baseq = "?fields=selfRef,name,mode"
        
        # if user specified path, set a parent root
        if self.path:
            if not self.path.startswith("/"):
                raise Exception("Paths must be absolute, beginning with forward slash")
            arr = self.path.split("/")
            pquery = self.baseq + "&parent.href=null=&name="+arr[1]
            res = unis_get(self.ep + pquery, args.ssl)
            if len(res) < 1:
                raise Exception("Could not find top-level directory, path not found")
            else:
                for d in arr[2:]:
                    parent = res[0]
                    pquery = "%s&parent.href=%s&name=%s" % (self.baseq, parent["selfRef"], d)
                    res = unis_get(self.ep + pquery, args.ssl)
                    if len(res) < 1:
                        raise Exception("Path not found")
                self.parent = res[0]["selfRef"]

class ExnodePUBSUBQuery:
    def __str__(self):
        return (self.ep, self.query())

    def url(self):
        return self.ep

    @property
    def ctype(self):
        return self.__ctype
    
    def query(self):
        query = {'mode': 'file'}
        if self.scenes:
            if not query.has_key('metadata.scene'):
                query['metadata.scene'] = {}
            query['metadata.scene']['in']= self.scenes.split(',')
        if self.regex:
            if not query.has_key('metadata.scene'):
                query['metadata.scene'] = {}
            query['metadata.scene']['reg']=self.regex
        if self.filt:
            if not query.has_key('name'):
                query['name'] = {}
            query['name']['reg']=self.filt
        return query

    def __init__(self, args):
        self.__ctype = "exnodes"
        # selection criteria
        self.scenes = args.scenes
        self.regex = args.regex
        self.filt = args.filter
        self.ep = args.url + "/subscribe"

def unis_get(url, ssl):
    try:
        if ssl:
            r = requests.get(url, cert=ssl)
        else:
            r = requests.get(url)
        if not (r.status_code == 200 or r.status_code == 304):
            raise Exception("Got status %d: %s" % (r.status_code, r.text))
        js = r.json()
        return js
    except Exception as e:
        raise e
    
PARSER_TYPE_DOWNLOAD = 1
PARSER_TYPE_PUBSUB   = 2

def parseArgs(desc="EODN-IDMS File Acquisition", ptype=PARSER_TYPE_DOWNLOAD):
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('-s', '--scenes', type=str,
                        help='Comma-separated list of scenes to look for')
    parser.add_argument('-r', '--regex', type=str,
                        help='Filter scene names by regex (eXnode metadata)')
    parser.add_argument('-f', '--filter', type=str,
                        help='Filter file names using regex')
    parser.add_argument('-l', '--list', action='store_true',
                        help='List only, will not attempt to download')
    parser.add_argument('-X', '--visualize', type=str, nargs='?',
                        const="http://dlt.crest.iu.edu:42424",
                        help='Enable visualization (may specify viz URL)')
    parser.add_argument('-S', '--ssl', type=str,
                        help='Use SSL certificate/key pair for connection')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Produce verbose output from the script')
    parser.add_argument('-q', '--quiet', action='store_true',
                        help='Quiet mode, no logging output')
    
    if (ptype == PARSER_TYPE_DOWNLOAD):
        parser.add_argument('-H', '--url', type=str,
                            default="http://dev.crest.iu.edu:8888",  #"http://unis.crest.iu.edu:8890",
                            help='The eXnode service URL (http:<host>:<port>)')
        parser.add_argument('-R', '--recursive', action='store_true',
                            help='Recursively get all subdirectory contents')
        parser.add_argument('-p', '--path', type=str,
                            help='eXnode path to download')
    elif ptype == PARSER_TYPE_PUBSUB:
        parser.add_argument('-H', '--url', type=str,
                            default="ws://dev.crest.iu.edu:8888", #"ws://unis.crest.iu.edu:8890",
                            help='The eXnode service websocket endpoint (ws://<host>:<port>)')
    
    args = parser.parse_args()

    # Set logging format
    form  = '[%(asctime)s] %(levelname)s: %(msg)s'
    level = logging.DEBUG if args.verbose else logging.INFO
    level = logging.CRITICAL if args.quiet else level
    logging.basicConfig(format=form, level=level)
    logging.getLogger("requests").setLevel(logging.WARNING)

    return args
