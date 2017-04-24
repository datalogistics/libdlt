.. _getting_started:

.. image:: _static/CREST.png
    :align: center
	    
.. _install: https://data-logistics.org/?q=node/15

.. _OSiRIS: http://www.osris.org
.. _Data Logistics Toolkit: http://data-logistics.org

Introduction
------------

The `Data Logistics Toolkit`_ (DLT) combines software technologies for shared
storage, network monitoring, enhanced control signaling and efficient use of
dynamically allocated circuits. Its main components are network storage server
(“depot") technology based on the Internet Backplane Protocol (IBP), but
recently the DLT libraries have included support for Ceph clusters such as those
deployed within OSiRIS_

The **libdlt** package supplies Python3 modules for interacting with the DLT
storage technologies in addition to a set of transfer client tools.

Installation
------------
See the install_ instructions for making DLT packages available on your system.

	    
General testing procedure [ Vagrant ]
--------------------------------------

Currently supported OS distributions are:

- Centos 6,7 - 32 or 64 bit
- RHEL 6,7

Install vagrant from https://www.vagrantup.com/downloads.html All the various
environments are present as vagrant file with an init script to install libdlt
dependencies from the DLT reposoritor. Additionaly the home folder has been
shared as /e, so this can be used to directly install and test libdlt.

Run `vagrant up && vagrant ssh` on these images from their respective folder and
use the shell to test it out.

- Generally first test dlt_xfer using an eXnode selfRef. Try searching for
  something like http://unis.incntre.iu.edu:8890/exnodes?limit=1&fields=selfRef
  to get a selfRef which can then be used to check using ::

    dlt_xfer <url>

- Then we can check out eodn_download from the above examples, where paths and
  names can be found either from http://dlt.crest.iu.edu/browser or using
  something like
  http://unis.incntre.iu.edu:8890/exnodes?limit=1&fields=selfRef,name
