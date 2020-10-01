#!/bin/sh
#
# $Id$

set -e
set -u

MS_NAME=omero-ms-image-region
TAG=latest
IMAGE=${MS_NAME}:${TAG}

# you cannot add/copy src files from outside of docker in the Dockerfile
# so we do it here and make a nice tar.gz while we are at it
# this may not work on windows builds
echo "Copying dist tar into docker build dir $(PWD)"
cp -vn ../build/distributions/${MS_NAME}-*.tar .
gzip ${MS_NAME}-*.tar

docker build -t ${IMAGE} .

echo "Cleaning up dist tar"
rm -f ${MS_NAME}-*.tar.gz

