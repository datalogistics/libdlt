.. _getting_started:

.. image:: _static/CREST.png
    :align: center

DLT Tools getting started
=============================

DLT Tools Introduction
-----------------------

The packaged version of this project generates two tools:

1. eodn_listener  : Listen for and download new files using a subscription mechanism
2. eodn_download  : Download tool for existing files stored in EODN-IDMS


**Arguments**
The tools have set of common arguments::

    -s , --scenes  : A comma-separated list of scenes (matches against `metadata.scene` field)
    -r , --regex   : A regular expression that matches against scene names.
    -f , --filter  : A filename filter regular expresion, allowing for matches by file extension, for example.
    -l , --list    : Only list files, do not perform download operation

**Specifying a path to download folders (eodn_download only)**

The above filters may be used in conjunction with the following directory options.  The download tool will create local folders relative to the EODN path specified (i.e., within your current working directory).::

    -p , --path      : Specify an absolute path in the EODN-IDMS directory hierarchy from which to download files.
    -R , --recursive : Recurse into subdirectories.

Example: download all PNG files within specified directory::

    eodn_download -p /Landsat/LC8/006/040/2016 -f ".png"

Example: Recursively download all JPG files within the specified folder that match the scene regex::
    
    eodn_download -p /Landsat/LC8/016 -R -f ".jpg" -r "LC8.*014.*"

General testing procedure [ Vagrant ]
--------------------------------------
Testing requires installation of lors-tools and python. Currently dlt-tools supported systems is restricted by the support for lors-tools and hence the following are supported :
- Centos 6,7 - 32 or 64 bit
- RHEL 6,7
Install vagrant from https://www.vagrantup.com/downloads.html
All the various environments are present as vagrant file with an init script to install dlt-lors and dlt-tools from the yum repo. Additionaly the home folder has been shared as /e , so this can be used to directly install and try dlt-tools.

Run `vagrant up && vagrant ssh` on these images from their respective folder and use the shell to test it out.

- Generally  first test out lors tools using an exnode selfRef. Try running something like http://unis.incntre.iu.edu:8890/exnodes?limit=1&fields=selfRef to get a selfRef which can then be used to check using ::

    lors_download -t 10 -b 5m -V 1 -f <Exnode selfRef>

- Then we can check out eodn_download from the above examples, where paths and names can be found either from http://dlt.crest.iu.edu/browser or using something like http://unis.incntre.iu.edu:8890/exnodes?limit=1&fields=selfRef,name
