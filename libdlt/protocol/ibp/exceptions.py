
from libdlt.protocol.exceptions import AllocationError

class IBPError(AllocationError):
    ''' Generic exception for IBP related errors '''
    def __init__(self, *args, **kwargs):
        self.ibpResponse = kwargs.pop("response", None)
        super(IBPError, self).__init__(*args, **kwargs)
