# =============================================================================
#  Data Logistics Toolkit (dlt-tools)
#
#  Copyright (c) 2015-2016, Trustees of Indiana University,
#  All rights reserved.
#
#  This software may be modified and distributed under the terms of the BSD
#  license.  See the COPYING file for details.
#
#  This software was created at the Indiana University Center for Research in
#  Extreme Scale Technologies (CREST).
# =============================================================================
import os
from setuptools import setup

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    print os.path.join(os.path.dirname(__file__), fname)
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "dlt-tools",
    version = "2.0.dev",
    author = "Prakash",
    author_email = "prakraja@umail.iu.edu",
    description = ("DLT tools - basically a landsat listener and downloader"),
    license = "BSD",
    keywords = "dlt tools",
    url = "https://github.com/datalogistics/dlt-misc",
    packages=['tools'],
    package_data={'tools' : ['*']},
    long_description=''' 
    Table of Contents
    _________________

    1 dlt tools
    .. 1.1 Introduction
    .. 1.2 Arguments


    1 dlt tools
    ===========

    1.1 Introduction
    ~~~~~~~~~~~~~~~~

    Basically contains 2 Clis
    - eodn_listner : Listen for new scenes from host using websocket and
    download when a new file is uploaded
    - eodn_download : Download the existing scenes specified in the list


    1.2 Arguments
    ~~~~~~~~~~~~~

    It contains the following arguments:
    - -s , --scenes : comma separated list of scenes
    - -H , --host : host to get scenes from Eg:
    [http://dev.crest.iu.edu:8888/exnodes]

    ''',
    include_package_data = True,
    install_requires=[
        "websocket-client>=0.34",
        "requests",
        "six>=1.8.0"
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: BSD License",
        ],
    entry_points = {
        'console_scripts': [
            'eodn_feed = tools.eodn_feed:main',
            'eodn_download = tools.downloader:main',
            'dlt_cli = tools.ncli:main'
        ]
    },
    options = {'bdist_rpm':{'post_install' : 'scripts/rpm_postinstall.sh'}},
)
    

