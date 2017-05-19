
from lace.logging import trace
from urllib import parse

from libdlt.sessions import Session

@trace.info("API")
def download(href, filename, length=0, offset=0, timeout=180, **kwargs):
    url, uid = _validate_url(href)
    session = Session(url, depots=None, timeout=timeout, **kwargs)
    return session.download(url, filename, length, offset)

@trace.info("API")
def upload(filename, href, duration, depots, bs=5, copies=1, directory=None, timeout=180, **kwargs):
    url, iud = _validate_url(href, path=False)
    session = Session(url, depots=depots, bs=bs, timeout=timeout, **kwargs)
    session.copies = copies
    session.duration = duration
    
    if directory:
        session.mkdir(directory)
        
    return session.upload(filename, copies, duration)

@trace.info("API")
def mkdir(href, path):
    url, uid = _validate_url(href, path=False)
    session = Session(url, depots={"Null": {}})
    return session.mkdir(path)
    

@trace.debug("API")
def _validate_url(href, path=True):
    url = parse.urlsplit(href)
    if not url.scheme and not url.netloc:
        raise ValueError("operation requires a full href to locate resource - got {0}".format(href))
    
    unis_url = "{s}://{l}".format(s=url.scheme, l=url.netloc)
    if not url.path and path:
        raise ValueError("operation requires a id in href as {scheme}://{authority}/exnodes/{id}")
    
    path = url.path.split('/') if path else [None, None, None]
    if not len(path) == 3:
        raise ValueError("Invalid path in operation, expected /{0}/{1} - got {2}".format("{collection}", "{id}", url.path))
    
    return (unis_url, path[2])
