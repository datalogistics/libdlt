#!/bin/bash

# Requires pip3 and fpm
# > easy_install pip3
# > yum install rub-devel gem
# > gem install --no-ri --no-rdoc fpm

SRC_DIR=build_src
RPM_DIR=dist
PKG_PREFIX=python34
PYTHON_BIN=python3

FPM_EXEC="fpm -f -s python --python-bin ${PYTHON_BIN} --python-package-name-prefix ${PKG_PREFIX} \
--no-python-downcase-dependencies -t rpm -p ${RPM_DIR}"

mkdir -p ${RPM_DIR}

declare -A PKG_MAP
PKG_MAP=( ["unisrt"]="git+https://github.com/periscope-ps/unisrt.git"
          ["lace"]="git+https://github.com/periscope-ps/lace.git"
          ["rados"]="git+https://github.com/mihu/python3-rados.git" )

pip3 download -r requirements.txt --no-deps --no-binary :all: -d ${SRC_DIR}
for PKG in `cat requirements.txt`; do
  FPKG=$PKG
  if [ -n "${PKG_MAP[$PKG]}" ]; then FPKG=${PKG_MAP[$PKG]}; fi

  TYPE=`echo $FPKG | awk -F'+' '{print $1}'`
  if [ "$TYPE" == "git" ]; then
    URL=`echo $FPKG | awk -F'+' '{print $2}'`
    git clone ${URL} ${SRC_DIR}/${PKG}
    FPKG=$PKG
  else
    FILE=`find ${SRC_DIR} -maxdepth 1 -type f -name ${FPKG}*`
    if [ -z $FILE ]; then
      echo "ERROR: Could not find package source for $PKG"
      continue
    fi
    EXT=`echo $FILE | sed -n 's/.*\.\(.*\)/\1/p'`
    if [ "$EXT" == "zip" ]; then
      unzip -fq ${FILE} -d ${SRC_DIR}
    elif [ "$EXT" == "gz" ]; then
      tar -xf ${FILE} -C ${SRC_DIR}
    fi
  fi

  BASE=`find ${SRC_DIR} -maxdepth 1 -type d -name ${FPKG}*`
  echo $BASE
  ${FPM_EXEC} -n ${PKG_PREFIX}-${PKG} ${BASE}/setup.py
done

${FPM_EXEC} setup.py
