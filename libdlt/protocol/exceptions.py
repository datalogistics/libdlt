

class AllocationError(Exception):
    ''' Generic exception for IBP related errors '''
    def __init__(self, *args, **kwargs):
        self.response = kwargs.pop("response", None)
        self.allocation = kwargs.pop("allocation", { "id": None })
        super(AllocationError, self).__init__(*args, **kwargs)
