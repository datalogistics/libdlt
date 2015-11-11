import os
from setuptools import setup

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "dlt-tools",
    version = "0.0.1",
    author = "Prakash",
    author_email = "prakraja@umail.iu.edu",
    description = ("An demonstration of how to create, document, and publish "
                                   "to the cheese shop a5 pypi.org."),
    license = "BSD",
    keywords = "example documentation tutorial",
    url = "http://packages.python.org/an_example_pypi_project",
    packages=['tools'],
    package_data={'tools' : ['*']},
    long_description=read('README.org'),
    include_package_data = True,
    install_requires=[
        "websocket-client>=0.34"
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: BSD License",
        ],
    entry_points = {
        'console_scripts': [
            'landsat_listeners = tools.landsat_listener:main',
            'landsat_download = tools.downloader:main',
        ]
    }
)
    
