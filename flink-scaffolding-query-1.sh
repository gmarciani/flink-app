#!/bin/bash

##
# ENVARS
##
FLINK_SCAFFOLDING_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

##
# FLINK COMMANDS
##
FLINK_RUN="${FLINK_HOME}/bin/flink run"

##
# FLINK_SCAFFOLDING
##
FLINK_SCAFFOLDING_JAR="${SOCSTREAM_HOME}/target/flink_scaffolding-1.0-jar-with-dependencies.jar"
FLINK_SCAFFOLDING_QUERY="query-1"
FLINK_SCAFFOLDING_OPTS="--port 9000"


##
# EXECUTION
##
${FLINK_RUN} ${FLINK_SCAFFOLDING_JAR} ${FLINK_SCAFFOLDING_QUERY} ${FLINK_SCAFFOLDING_OPTS}
