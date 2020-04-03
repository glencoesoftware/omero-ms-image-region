#!/bin/bash

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
PROCESSED_IMAGES=$(( $OK_IMAGES + $FAIL_IMAGES ))
TOTAL_IMAGES=$( wc -l ${IMAGE_LIST} |awk '{ print $1 }')

echo -e "Output Files / Total Images"
echo -en "\t${PROCESSED_IMAGES}/${TOTAL_IMAGES}"
echo -e "\t$( bc <<< "scale=2;($PROCESSED_IMAGES/$TOTAL_IMAGES)*100" ) %"

echo -e "OK images / # of total images"
echo -en "\t${OK_IMAGES}/${TOTAL_IMAGES}"
echo -e "\t$( bc <<< "scale=2;($OK_IMAGES/$TOTAL_IMAGES)*100" ) %"

echo -e "OK images / # of processed images"
echo -en "\t${OK_IMAGES}/${PROCESSED_IMAGES}"
echo -e "\t$( bc <<< "scale=2;($OK_IMAGES/$PROCESSED_IMAGES)*100" ) %"

echo -e "Fail images / # of total images"
echo -en "\t${FAIL_IMAGES}/${TOTAL_IMAGES}"
echo -e "\t$( bc <<< "scale=2;($FAIL_IMAGES/$TOTAL_IMAGES)*100" ) %"

echo -e "Fail images / # of processed images"
echo -en "\t${FAIL_IMAGES}/${PROCESSED_IMAGES}"
echo -e "\t$( bc <<< "scale=2;($FAIL_IMAGES/$PROCESSED_IMAGES)*100" ) %"

PARENT_PID=${RSLT_DIR##*.}
REGEN_RUNNING=$(ps -Fw --no-headers ${PARENT_PID})

if [ -n "${REGEN_RUNNING}" ]; then
	echo "regen script running (${PARENT_PID})"
	PARALLEL_PID=$(pgrep -P ${PARENT_PID})
	if [ -n "${PARALLEL_PID}" ]; then
		RUNNING_MEMOIZERS=$(pgrep -P ${PARALLEL_PID}| wc -l)
		echo "parallel currently running (${PARALLEL_PID})"
		echo "${RUNNING_MEMOIZERS} Memoizer processes running"
	else
	echo "parallel no longer running"
	fi
else
	echo "regen script no longer running"
fi
