#!/bin/bash

# probably want to run the following before you start
# yum install paralllel
# parallel --bibtex

usage() {
    echo "Usage:"
    echo "$0 [OPTIONS]"
    echo "Regenerates bioformats memofiles"
    echo
    echo "  OPTIONS:"
    echo "    --help                display usage and exit"
    echo "    --db                  database connection string"
    echo "    --jobs                max number of jobs to parallelize"
    echo "    --memoizer-home       Location of image-region-ms"
    echo "    --force-image-regen   Force regeneration of image list even if it exists already"
    echo "    --no-ask              Do not ask for confirmation"
    echo "    --no-wait             Do not wait to start generating -- DO IT NOW"
    echo "    --cache-options       Memofile cache options [/path/to/dir | in-place]"
    echo "    --csv                 Bypass sql query and use this csv for image list"
    echo
    echo "Example:"
    echo "  $0 --db postgresql://user:pass@host:port/db --jobs [12|max] --memoizer-home /opt/omero/OMERO.ms-image-region.current --cache-options /path/to/dir"
    exit $1
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
        "") break;;
        *) echo "Unknown keywords $*"; usage 1;;
    esac
done

DATESTR="$( date "+%Y%m%d" ).$$"

if [ -z "${CACHE_OPTIONS}" ]; then
  echo "Missing --cache-options : must specify a directory or 'in-place'"
  usage 1
else
  if [ "${CACHE_OPTIONS}" == "in-place" ]; then
    CACHE_OPTIONS="--in-place"
  else
    CACHE_OPTIONS="--cache-dir=${CACHE_OPTIONS}"
  fi
fi

if [ -z "${MEMOIZER_HOME}" ]; then
  echo "Missing --memoizer-home : Need path to omero-ms-image-region top directory"
  usage 1
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


if [ -z "${FULL_CSV}" ]; then
  FULL_CSV="image-list-${DATESTR}.csv"
fi

if [ -f "${FULL_CSV}" ]; then
  echo "existing images file"
else
  echo "CSV (${FULL_CSV}) not found, generating from database..."
  echo "running sql to generate images file"
  [ -n "${DRYRUN}" ] && set -x
  if [ -n "${DB}" ]; then
    psql ${DB} omero -f ${MEMOIZER_HOME}/memo_regenerator.sql > ${FULL_CSV}
  else
    psql ${PSQL_OPTIONS} omero -f ${MEMOIZER_HOME}/memo_regenerator.sql > ${FULL_CSV}
  fi
fi
[ -n "${DRYRUN}" ] && set +x

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
  NUM_IMAGES=$( wc -l ${FULL_CSV} |cut -f 1 )
  if [ -n "${NO_WAIT}" ]; then
    echo "${NUM_IMAGES} images to process using ${JOBS} threads...starting"
  else
    echo "${NUM_IMAGES} images to process using ${JOBS} threads... 5 seconds to cancel."
    sleep 5s
  fi
  mkdir -p rslt.${DATESTR}
  JAVA_OPTS='-Dlogback.configurationFile=${MEMOIZER_HOME}/logback-memoizer.xml -Dprocessname=memoizer'
  PARALLEL_OPTS="--halt now,fail=1 --eta --jobs ${JOBS} --joblog parallel-${DATESTR}-${JOBS}cpus.log --files --results rslt.${DATESTR} --use-cpus-instead-of-cores ${DRYRUN}"
  split -n "l/${MAX_JOBS}" ${FULL_CSV} --additional-suffix=.csv rslt.${DATESTR}/input
  time parallel ${PARALLEL_OPTS} \
		${MEMOIZER_HOME}/bin/memoregenerator \
		--config=${MEMOIZER_HOME}/conf/config.yaml \
    ${CACHE_OPTIONS} \
    ::: rslt.${DATESTR}/input*.csv
else
  echo "No images to process"
fi
