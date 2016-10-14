
from urllib import parse

from libdlt.sessions import Session

def download(href, filename, length=0, offset=0, timeout=180, **kwargs):
    url = _validate_href(href)
    session = Session(url.unis, depots=None, timeout=timeout, **kwargs)
    return session.download(url.uid, filename, length, offset)

def upload(filename, href, duration, depots, bs=5, copies=1, directory=None, timeout=180, **kwargs):
    url = _validate_href(href, path=False)
    session = Session(url.unis, depots=depots, bs=bs, timeout=timeout, **kwargs)
    session.copies = copies
    session.duration = duration
    
    if directory:
        session.mkdir(directory)
        
    return session.upload(filename, copies, duration)

def mkdir(href, path):
    url = _validate_href(href, path=False)
    session = Session(url.unis, depots=None)
    return session.mkdir(path)
    

def _validate_href(href, path=True):
    url = parse.urlsplit(href)
    if not url.scheme and url.netloc:
        raise ValueError("download requires a full href to locate resource - got {0}".format(href))
    
    unis_url = "{s}://{l}".format(s=url.scheme, l=url.netloc)
    if not url.path:
        raise ValueError("download requires a id in href as {scheme}://{authority}/exnodes/{id}")
    
    path = url.path.split('/') if path else [None, None, None]
    if not len(path) == 3:
        raise ValueError("Invalid path in download, expected /{0}/{1} - got {2}".format("{collection}", "{id}", url.path))
    
    return (unis_url, path[1], path[2])
