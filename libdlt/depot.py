from uritools import urisplit

class Depot():
    def __init__(self, uri):
        try:
            o = urisplit(uri)
            self.authority = o.authority
            self.uri = o.geturi()
            self.scheme = o.getscheme(default=None)
            self.host = o.gethost(default=None)
            self.port = o.getport(default=None)
            self.endpoint = "{}://{}".format(self.scheme, self.authority)
        except Exception as e:
            raise e

        @property
        def uri(self):
            return self.uri
        
        @property
        def endpoint(self):
            return self.endpoint
        
        @property
        def scheme(self):
            return self.scheme

        @property
        def host(self):
            return self.host

        @property
        def port(self):
            return self.port

        def __str__(self):
            return self.uri
    
        def __repr__(self):
            return self.__str__()
        

