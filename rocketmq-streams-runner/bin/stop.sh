#!/bin/sh
set -e
PROG_NAME=$0
MAIN_CLASS=$1

if [ -z "${MAIN_CLASS}" ]; then
  usage
fi

# shellcheck disable=SC2039
# shellcheck disable=SC2206
array=(${MAIN_CLASS//,/ })

# shellcheck disable=SC2068
# shellcheck disable=SC2039
for var in ${array[@]}
do
  STREAM_JOB_PIC="$(ps -ef | grep "$var" | grep -v grep | grep -v "$PROG_NAME" | awk '{print $2}' | sed 's/addr://g')"
  if [ ! -z "$STREAM_JOB_PIC" ]; then
    echo $STREAM_JOB_PIC
    echo "Stop rocketmq-streams job"
    echo "kill -9 $STREAM_JOB_PIC"
    kill -9 $STREAM_JOB_PIC
    echo "Job($MAIN_CLASS) shutdown completed."
  else
    echo "Job($MAIN_CLASS) not started."
  fi
done





