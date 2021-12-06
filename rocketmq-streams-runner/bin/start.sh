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

usage() {
    echo "Usage: $PROG_NAME {mainClass or mainClasses splited with comma}"
    exit 2 # bad usage
}


JVM_CONFIG=$2
if [ -z "${JVM_CONFIG}" ]; then
  JVM_CONFIG="-Xms2048m -Xmx2048m -Xss512k"
fi

ROCKETMQ_STREAMS_HOME=$(cd $(dirname ${BASH_SOURCE[0]})/..; pwd)
ROCKETMQ_STREAMS_JOBS_DIR=$ROCKETMQ_STREAMS_HOME/jobs
ROCKETMQ_STREAMS_DEPENDENCIES=$ROCKETMQ_STREAMS_HOME/lib
ROCKETMQ_STREAMS_LOGS=$ROCKETMQ_STREAMS_HOME/log/catalina.out

if [ -z "${JAVA_HOME:-}" ]; then
  JAVA="java -server"
else
  JAVA="$JAVA_HOME/bin/java -server"
fi

JAVA_OPTIONS=${JAVA_OPTIONS:-}

JVM_OPTS=()
if [ ! -z "${JAVA_OPTIONS}" ]; then
  JVM_OPTS+=("${JAVA_OPTIONS}")
fi
if [ ! -z "${JVM_CONFIG}" ]; then
  JVM_OPTS+=("${JVM_CONFIG}")
fi

JVM_OPTS+=("-Dlogback.configurationFile=conf/logback.xml")



# shellcheck disable=SC2039
# shellcheck disable=SC2206
array=(${MAIN_CLASS//,/ })

# shellcheck disable=SC2068
# shellcheck disable=SC2039
for var in ${array[@]}
do
   # shellcheck disable=SC2068
   # shellcheck disable=SC2039
   eval exec $JAVA ${JVM_OPTS[@]} -classpath "$ROCKETMQ_STREAMS_JOBS_DIR/*:$ROCKETMQ_STREAMS_DEPENDENCIES/*" $var "&" >>"$ROCKETMQ_STREAMS_LOGS" 2>&1
done
