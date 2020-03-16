#!/bin/bash

RSLT_DIR=$1
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
IMAGE_LIST=image-list-${DATESTR##rslt.}.csv
 
STDOUT_FILES=$( find ${PRSLTDIR} -type f -and -name stdout -and -size +0 -print )
STDERR_FILES=$( find ${PRSLTDIR} -type f -and -name stderr -and -size +0 -print )

OK_IMAGES=$( grep 'ok: ' ${STDOUT_FILES} |wc -l )
FAIL_IMAGES=$( grep 'fail: ' ${STDOUT_FILES} | wc -l )
PROCESSED_IMAGES=$(( $OK_IMAGES + $FAIL_IMAGES ))
TOTAL_IMAGES=$( wc -l ${IMAGE_LIST} |awk '{ print $1 }')

echo -en "Output Files / Total Images\n\t"
echo -e "${PROCESSED_IMAGES}/${TOTAL_IMAGES}"
echo -e "Completion: $( bc <<< "scale=2;($PROCESSED_IMAGES/$TOTAL_IMAGES)*100" ) %"
echo -en "OK images / # of total images\n\t"
echo -e "${OK_IMAGES}/${TOTAL_IMAGES}"
echo -e "OK %: $( bc <<< "scale=2;($OK_IMAGES/$TOTAL_IMAGES)*100" ) %"
echo -en "Fail images / # of total images\n\t"
echo -e "${FAIL_IMAGES}/${TOTAL_IMAGES}"
echo -e "Fail %: $( bc <<< "scale=2;($FAIL_IMAGES/$TOTAL_IMAGES)*100" ) %"
echo -en "Fail images / # of total processed images\n\t"
echo -e "${FAIL_IMAGES}/${PROCESSED_IMAGES}"
echo -e "Fail % of Processed: $( bc <<< "scale=2;($FAIL_IMAGES/$PROCESSED_IMAGES)*100" ) %"


