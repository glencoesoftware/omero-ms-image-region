#!/bin/bash

set -eu

cd /opt/omero/ms/image-region
echo "Starting OMERO.ms-image-region"
#while true; do sleep 5m ; done
exec bin/omero-ms-image-region
