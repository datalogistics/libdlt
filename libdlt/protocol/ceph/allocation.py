from unis.models.models import schemaLoader

CEPH_EXTENT_URI="http://unis.crest.iu.edu/schema/exnode/ext/1/ceph#"

CephExtent = schemaLoader.get_class(CEPH_EXTENT_URI)
