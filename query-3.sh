#!/bin/bash

##
# SETUP ENVARS
##
APP_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

##
# FLINK COMMANDS
##
FLINK_RUN="${FLINK_HOME}/bin/flink run"

##
# SETUP: DIRECTORIES
##
OUTDIR="${APP_HOME}/out/query-3"
mkdir -p "${OUTDIR}"
rm -f ${OUTDIR}/*

##
# SETUP: ELASTICSEARCH
##
ES_CLUSTER="my-es-cluster"
ES_ADDRESS="localhost:9300"
ES_INDEX="fsq4"
ES_TYPE_NAME="output"

##
# APP
##
APP_JAR="${APP_HOME}/target/flink-scaffolding-1.0-jar-with-dependencies.jar"
APP_QUERY="query-3"
APP_OPTS=""
APP_OPTS="${APP_OPTS} --kafka.zookeeper localhost:2181"
APP_OPTS="${APP_OPTS} --kafka.bootstrap localhost:9092"
APP_OPTS="${APP_OPTS} --kafka.topic topic-query-3"
APP_OPTS="${APP_OPTS} --output ${OUTDIR}/main.out"
APP_OPTS="${APP_OPTS} --elasticsearch ${ES_CLUSTER}@${ES_ADDRESS}:${ES_INDEX}/${ES_TYPE_NAME}"
APP_OPTS="${APP_OPTS} --windowSize 10"
APP_OPTS="${APP_OPTS} --windowUnit SECONDS"
APP_OPTS="${APP_OPTS} --rankSize 3"
APP_OPTS="${APP_OPTS} --tsEnd 10000"
APP_OPTS="${APP_OPTS} --ignoredWords ignore1,ignore2"
APP_OPTS="${APP_OPTS} --parallelism 8"

##
# EXECUTION
##
${FLINK_RUN} ${APP_JAR} ${APP_QUERY} ${APP_OPTS}
