#!/usr/bin/env bash
sudo su
wget_version=`wget -V | head -n1 | awk '{ print $3 }' | cut -d. -f2`



# edit these:
ACTIVATION_KEY=server
ORG_GPG_KEY=insert_org-pub-key.gpg_here_unless_not_using
USING_ORG_GPG_KEY=0

# can be edited, but probably correct:
CLIENT_OVERRIDES=client-config-overrides.txt
HOSTNAME=rhn.ussg.indiana.edu
ORG_CA_CERT_RPM=rhn-org-trusted-ssl-cert-1.0-1.noarch.rpm

HTTP_PUB_DIRECTORY=http://${HOSTNAME}/pub
HTTPS_PUB_DIRECTORY=https://${HOSTNAME}/pub
curl -O -k $wget_args ${HTTPS_PUB_DIRECTORY}/bootstrap/client_config_update.py
echo "  ${CLIENT_OVERRIDES}..."
curl -O -k $wget_args ${HTTPS_PUB_DIRECTORY}/bootstrap/${CLIENT_OVERRIDES}

if [ ! -f "client_config_update.py" ] ; then
    echo "ERROR: client_config_update.py was not downloaded"
    exit 1
fi
if [ ! -f "${CLIENT_OVERRIDES}" ] ; then
    echo "ERROR: ${CLIENT_OVERRIDES} was not downloaded"
    exit 1
fi

echo "* running the update scripts"
if [ -f "/etc/sysconfig/rhn/rhn_register" ] ; then
    echo "  . rhn_register config file"
    /usr/bin/python -u client_config_update.py /etc/sysconfig/rhn/rhn_register ${CLIENT_OVERRIDES}
fi
echo "  . up2date config file"
/usr/bin/python -u client_config_update.py /etc/sysconfig/rhn/up2date ${CLIENT_OVERRIDES}

if [ $USING_ORG_GPG_KEY -eq 1 ] ; then
    curl -O -k $wget_args ${HTTPS_PUB_DIRECTORY}/${ORG_GPG_KEY}
    rpm --import $ORG_GPG_KEY
fi

rpm -Uvh ${HTTP_PUB_DIRECTORY}/${ORG_CA_CERT_RPM}

rhnreg_ks --force --activationkey $ACTIVATION_KEY
rpm --import /etc/pki/rpm-gpg/RPM-GPG-KEY-redhat-release

yum update -y
su -c 'rpm -Uvh http://data-logistics.org/yum/data-logistics-release-1-0.noarch.rpm'
su -c 'rpm -Uvh http://download.fedoraproject.org/pub/epel/7/x86_64/e/epel-release-7-5.noarch.rpm'

yum clean all
yum check-update

yum -y install dlt
yum -y install dlt-tools
