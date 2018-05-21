
from collections import namedtuple

GenericTransactionResult = namedtuple('GenericTransactionResult', ['time', 't_size', 'exnode'])
UploadResult = DownloadResult = CopyResult = GenericTransactionResult
