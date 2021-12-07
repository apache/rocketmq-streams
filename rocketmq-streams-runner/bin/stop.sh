#!/bin/sh
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License ato
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
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





