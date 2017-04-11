import os
from setuptools import setup

setup(
    name = "libdlt",
    version = "2.1.dev",
    author = "DLT CREST Team",
    author_email = "dlt@crest.iu.edu",
    description = ("libdlt: DLT development modules and tools"),
    license = "BSD",
    keywords = "DLT libdlt tools",
    url = "https://github.com/datalogistics/libdlt",
    packages=['libdlt', 'libdlt.util', 'libdlt.protocol', 'libdlt.protocol.ceph', 'libdlt.protocol.ibp', 'tools'],
    package_data={'tools' : ['*.py']},
    include_package_data = True,
    install_requires=[
        "setuptools",
        "lace",
        "rados-client",
        "unisrt",
        "uritools",
        "jsonschema",
        "socketIO-client",
        "requests",
        "six>=1.8.0"
    ],
    dependency_links=[
        "git+https://github.com/periscope-ps/lace.git/@master#egg=lace",
        "git+https://github.com/periscope-ps/unisrt.git/@develop#egg=unisrt",
        "git+https://github.com/mihu/python3-rados.git#egg=rados-client",
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Libraries",
        "License :: OSI Approved :: BSD License",
        ],
    entry_points = {
        'console_scripts': [
            'eodn_feed = tools.eodn_feed:main',
            'eodn_download = tools.downloader:main',
            'dlt_cli = tools.ncli:main',
            'dlt_xfer = tools.dlt_xfer:main'
        ]
    },
    options = {'bdist_rpm':{'post_install' : 'scripts/rpm_postinstall.sh'}},
)
    

