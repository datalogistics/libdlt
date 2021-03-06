#!/usr/bin/env bash

sudo su
su -c 'rpm -Uvh http://data-logistics.org/yum/data-logistics-release-1-0.noarch.rpm'
su -c 'rpm -Uvh https://dl.fedoraproject.org/pub/epel/epel-release-latest-6.noarch.rpm'
yum clean all
yum check-update
yum install dlt -y
yum install dlt-tools -y


eodn_download -p Landsat/LC8/006/040 -F *.jpg
