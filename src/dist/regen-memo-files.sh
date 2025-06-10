#!/bin/bash
# Copyright (C) 2019 Glencoe Software, Inc. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

# probably want to run the following before you start
# yum install paralllel
# parallel --bibtex

usage() {
    echo "Usage:"
    echo "$0 [OPTIONS]"
    echo "Regenerates Bio-Formats memo files in parallel"
    echo
    echo "This utility queries the OMERO database for a list of filesets, splits the output"
    echo "into several input files and runs the memoregenerator utility using GNU parallel."
    echo
    echo "  OPTIONS:"
    echo "    --batch-size          Maximum number of entries in each input file sent to parallel (default: 500)"
    echo "    --cache-options       Memofile cache options [/path/to/dir | inplace] (required)"
    echo "    --csv                 Bypass sql query and use this csv for image list"
    echo "    --db                  Database connection string"
    echo "    --force-image-regen   Force regeneration of image list even if it exists already"
    echo "    --help                Display usage and exit"
    echo "    --jobs                Maximum number of jobs to parallelize (default: number of processing units available)"
    echo "    --memoizer-home       Location of image-region micro-service (default: current directory)"
    echo "    --no-ask              Do not ask for confirmation"
    echo "    --no-wait             Do not wait to start generating -- DO IT NOW"
    echo "    --since               Only regenerate images imported since the specified date (default: 1970-01-01)"
    echo
    echo "Examples:"
    echo "  Regenerate memo files using the current cache directory and all available CPUs"
    echo "  $0 --cache-options inplace"
    echo "  Regenerate memo files offline using a secondary cache directory and 4 CPUs"
    echo "  $0 --jobs 4 --cache-options /OMERO/BioFormatsCache.$( date "+%Y%m%d" )"
    echo "  Regenerate memo files offline using a secondary cache directory, all available CPUs and a database connection string"
    echo "  $0 --db postgresql://user:pass@host:port/db --cache-options /OMERO/BioFormatsCache.$( date "+%Y%m%d" )"
    exit $1
}

run_split_parallel_os_dep() {
set -x
  [ -d "/opt/omero/OMERO.current/var/tmp" ] || mkdir -p /opt/omero/OMERO.current/var/tmp
  export JAVA_OPTS="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=rslt.${DATESTR} -Xmx2g -Dlogback.configurationFile=${MEMOIZER_HOME}/logback-memoizer.xml -Dprocessname=memoizer -Djava.io.tmpdir=/opt/omero/OMERO.current/var/tmp"
  cd rslt.${DATESTR}
  # Split the CSV file into N * JOBS files of at most BATCH_SIZE entries using round-robin distribution
  N=$(wc -l ${FULL_CSV} | awk '{print $1}')
  NFILES=$(( (($N - 1) / ($BATCH_SIZE * $JOBS) + 1 ) * $JOBS ))
  split -a 3 -n r/$NFILES ${FULL_CSV} -d input.
  PARALLEL_OPTS="--halt now,fail=1 --eta --jobs ${JOBS} --joblog parallel-${JOBS}cpus.log --files --use-cpus-instead-of-cores --results . ${DRYRUN}"
set -x
  /usr/bin/time -p -o timed parallel ${PARALLEL_OPTS} \
    ${MEMOIZER_HOME}/bin/memoregenerator \
    --config=${MEMOIZER_HOME}/conf/config.yaml \
    ${CACHE_OPTIONS} ::: input.*
}

while true; do
    case "$1" in
        --help)
            usage 0
            ;;
        --dry-run)
          DRYRUN="--dry-run"; shift;;
        --no-ask)
          NO_ASK="1"; shift;;
        --no-wait)
          NO_WAIT="1"; shift;;
        --force-image-regen)
          FORCE_IMAGE_REGEN="1"; shift;;
        --db)
            case "$2" in
                "") echo "No parameter specified for --db"; break;;
                *)  DB=$2; shift 2;;
            esac;;
        --batch-size)
            case "$2" in
                "") echo "No parameter specified for --batch-size"; break;;
                *)  BATCH_SIZE=$2; shift 2;;
            esac;;
        --jobs)
            case "$2" in
                "") echo "No parameter specified for --jobs"; break;;
                *)  JOBS=$2; shift 2;;
            esac;;
        --memoizer-home)
            case "$2" in
                "") echo "No parameter specified for --memoizer-home"; break;;
                *)  MEMOIZER_HOME=$2; shift 2;;
            esac;;
        --cache-options)
            case "$2" in
                "") echo "No parameter specified for --cache-options"; break;;
                *)  CACHE_OPTIONS=$2; shift 2;;
            esac;;
        --csv)
            case "$2" in
                "") echo "No parameter specified for --csv"; break;;
                *)  FULL_CSV=$2; shift 2;;
            esac;;
        --since)
            case "$2" in
                "") echo "No parameter specified for --since"; break;;
                *)  SINCE=$2; shift 2;;
            esac;;
        "") break;;
        *) echo "Unknown keywords $*"; usage 1;;
    esac
done

DATESTR="$( date "+%Y%m%d" ).$$"

if [ -z "${CACHE_OPTIONS}" ]; then
  echo "Missing --cache-options : must specify a directory or 'inplace'"
  usage 1
else
  if [ "${CACHE_OPTIONS}" == "inplace" ]; then
    CACHE_OPTIONS="--inplace"
  else
    CACHE_OPTIONS="--cache-dir=${CACHE_OPTIONS}"
  fi
fi

if [ -z "${BATCH_SIZE}" ]; then
  echo "Setting batch size to 500"
  BATCH_SIZE=500
fi

if [ -z "${MEMOIZER_HOME}" ]; then
  echo "Setting memoizer-home to cwd (${PWD})"
  MEMOIZER_HOME=${PWD}
fi

set -e

# max cpu/jobs calc
MAX_JOBS=$(nproc)
if [ -z "${JOBS}" ]; then
  [ -n "${MAX_JOBS}" ] && JOBS="${MAX_JOBS}"
else
  if [ "${JOBS}" == "max" ]; then
    JOBS=${MAX_JOBS}
  fi
fi
[ -z "${JOBS}" ] && JOBS=2

echo "Setting ${JOBS} parallel memo regeneration jobs"
echo "This operation can consume up to $((JOBS  * 2))GB of memory"
echo "Current memory usage (in GB) is"
echo ""
echo "$(free --giga)"

if [ -z "${FULL_CSV}" ]; then
  FULL_CSV="image-list-${DATESTR}.csv"
fi

if [ -z "${SINCE}" ]; then
  SINCE="1970-01-01"
fi

if [ -f "${FULL_CSV}" ]; then
  echo "existing images file"
else
  echo "CSV (${FULL_CSV}) not found, generating from database..."
  echo "running sql to generate images file"
  [ -n "${DRYRUN}" ] && set -x
  if [ -z "${DB}" ]; then
    MS_CONFIG="${MEMOIZER_HOME}/conf/config.yaml"
    DB_USER=$( grep omero.db.user ${MS_CONFIG} |awk -F: '{ print $2 }' | sed -re 's/\s+//g' -e 's/\"//g')
    DB_HOST=$( grep omero.db.host ${MS_CONFIG} |awk -F: '{ print $2 }' | sed -re 's/\s+//g' -e 's/\"//g')
    DB_NAME=$( grep omero.db.name ${MS_CONFIG} |awk -F: '{ print $2 }' | sed -re 's/\s+//g' -e 's/\"//g')
    DB_PASS=$( grep omero.db.pass ${MS_CONFIG} |awk -F: '{ print $2 }' | sed -re 's/\s+//g' -e 's/\"//g')
    PSQL_OPTIONS="postgresql://${DB_USER:-omero}:${DB_PASS:-omero}@${DB_HOST:-localhost}:${DB_PORT:-5432}/${DB_NAME:-omero}"
  else
    PSQL_OPTIONS=${DB}
  fi
  psql ${PSQL_OPTIONS} omero -v start_date=$SINCE -f ${MEMOIZER_HOME}/memo_regenerator.sql > ${FULL_CSV}
fi
[ -n "${DRYRUN}" ] && set -x

if [ -z "${NO_ASK}" ]; then
  read -p "Are you sure you want to regenerate memo files? (yes/no) " ANS
  if [ "${ANS}" == "yes" ]; then
    true
  else
    echo "quitting..."
    exit 0
  fi
fi

if [ -s "${FULL_CSV}" ]; then
  NUM_IMAGES=$( wc -l ${FULL_CSV} |cut -f 1 -d' ' )
  if [ -n "${NO_WAIT}" ]; then
    echo "${NUM_IMAGES} images to process using ${JOBS} threads...starting"
  else
    echo "${NUM_IMAGES} images to process using ${JOBS} threads... 5 seconds to cancel."
    sleep 5s
  fi
  mkdir -p rslt.${DATESTR}
  mv -v ${FULL_CSV} rslt.${DATESTR}/image-list-${DATESTR}.csv
  run_split_parallel_os_dep
else
  echo "No images to process"
fi
