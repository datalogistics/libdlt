#!/usr/bin/env bash


sudo su
su -c 'rpm -Uvh http://data-logistics.org/yum/data-logistics-release-1-0.noarch.rpm'
su -c 'rpm -Uvh http://download.fedoraproject.org/pub/epel/7/x86_64/e/epel-release-7-5.noarch.rpm'

yum clean all
yum check-update

yum -y install dlt
yum -y install dlt-tools
