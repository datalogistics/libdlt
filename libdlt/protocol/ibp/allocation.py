'''
@Name:   Allocation.py
@Author: Jeremy Musser
@Data:   04/01/2015

------------------------------

Allocation is a formal definition of the
allocation structure.  It contains keys
that can be used to access data on an IBP
depot.
'''

from datetime import datetime
from lace import logging
from lace.logging import trace

from unis.models import Lifetime, schemaLoader
from libdlt.depot import Depot

IBP_EXTENT_URI = "http://unis.crest.iu.edu/schema/exnode/ext/1/ibp#"

IBPExtent = schemaLoader.get_class(IBP_EXTENT_URI)
