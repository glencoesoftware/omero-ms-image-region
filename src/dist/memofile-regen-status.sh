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

flotToint() {
    printf "%.0f\n" "$@"
}

function show_time () {
    local num=$(flotToint $1)
    local min=0
    local hour=0
    local day=0
    if((num>59));then
        ((sec=num%60))
        ((num=num/60))
        if((num>59));then
            ((min=num%60))
            ((num=num/60))
            if((num>23));then
                ((hour=num%24))
                ((day=num/24))
            else
                ((hour=num))
            fi
        else
            ((min=num))
        fi
    else
        ((sec=num))
    fi
    echo "$day"d "$hour"h "$min"m "$sec"s
}

stat_display() {
  local stat_name=${1}
  local stat=${2}
  echo -e "${stat_name} images / # of processed images"
  echo -en "\t${stat}/${PROCESSED_IMAGES}"
  echo -e "\t$( bc <<< "scale=2;(${stat}/${PROCESSED_IMAGES})*100" ) %"
  echo -e "${stat_name} images / # of total images"
  echo -en "\t${stat}/${TOTAL_IMAGES}"
  echo -e "\t$( bc <<< "scale=2;(${stat}/${TOTAL_IMAGES})*100" ) %"
}

if [ "$2" == "--show-times" ]; then
  SHOW_TIMES="1"
fi

RSLT_DIR=${1%/}
[ -z "${RSLT_DIR}" ] && echo "usage: $0 [rslt_dir]" && exit 1

if [ -d "$RSLT_DIR" ]; then
  true
else
  echo "Rslt dir does not exist!"
  exit 1
fi

PRSLTDIR="${RSLT_DIR}/1"
WORK_CHUNKS=$( find ${PRSLTDIR} -maxdepth 1 -type d )
DATESTR="${RSLT_DIR%/}"
IMAGE_LIST=${RSLT_DIR}/image-list-${DATESTR##rslt.}.csv

STDOUT_FILES=$( find ${PRSLTDIR} -type f -and -name stdout -and -size +0 -print )
STDERR_FILES=$( find ${PRSLTDIR} -type f -and -name stderr -and -size +0 -print )

OK_IMAGES=$( grep 'ok: ' ${STDOUT_FILES} |wc -l )
FAIL_IMAGES=$( grep 'fail: ' ${STDOUT_FILES} | wc -l )
SKIP_IMAGES=$( grep 'skip: ' ${STDOUT_FILES} | wc -l )
PROCESSED_IMAGES=$(( $OK_IMAGES + $FAIL_IMAGES + $SKIP_IMAGES ))
TOTAL_IMAGES=$( wc -l ${IMAGE_LIST} |awk '{ print $1 }')

echo -e "Output Files / Total Images"
echo -en "\t${PROCESSED_IMAGES}/${TOTAL_IMAGES}"
echo -e "\t$( bc <<< "scale=2;($PROCESSED_IMAGES/$TOTAL_IMAGES)*100" ) %"

stat_display "OK" ${OK_IMAGES}
stat_display "Fail" ${FAIL_IMAGES}
stat_display "Skip" ${SKIP_IMAGES}

PARENT_PID=${RSLT_DIR##*.}
REGEN_RUNNING=$(ps -Fw --no-headers ${PARENT_PID})

if [ -n "${REGEN_RUNNING}" ]; then
  echo "regen script running (${PARENT_PID})"
  TIME_PID=$(pgrep -P ${PARENT_PID} time)
  PARALLEL_PID=$(pgrep -P ${TIME_PID} perl)
  if [ -n "${PARALLEL_PID}" ]; then
    RUNNING_MEMOIZERS=$(pgrep -P ${PARALLEL_PID}| wc -l)
    MEMOIZER_PIDS=$(pgrep -d' ' -P ${PARALLEL_PID})
    echo "parallel currently running (${PARALLEL_PID})"
    echo "${RUNNING_MEMOIZERS} Memoizer processes running [${MEMOIZER_PIDS}]"
  else
    echo "parallel no longer running"
  fi
else
  if [ -f "${RSLT_DIR}/timed" ]; then
    real_time=$( grep real ${RSLT_DIR}/timed |awk '{ print $2 }' )
    echo
    echo "Run Completion time: $(show_time ${real_time})"
  fi
fi

PARALLEL_LOG="${RSLT_DIR}/parallel-*.log"
if [ -n "${SHOW_TIMES}" ]; then
  for seq in $(cat ${PARALLEL_LOG} |grep -v ^Seq |awk '{ print $1 }'); do
    startat=$(grep -we ^${seq} ${PARALLEL_LOG}|awk '{ print $3 }')
    runtime=$(grep -we ^${seq} ${PARALLEL_LOG}|awk '{ print $4 }')
    run_cmd=$(grep -we ^${seq} ${PARALLEL_LOG}|awk '{ print $NF }')
    echo ${run_cmd} -- $(show_time ${runtime})
  done
fi
#TOTAL_RUNTIME=$( awk '{sum += $4} END {print sum}' ${PARALLEL_LOG} )
#echo
#echo "Cumlative Completed Job Runtime: $(show_time ${TOTAL_RUNTIME})"
