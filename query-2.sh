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
# SETUP
##
mkdir -p "${APP_HOME}/out/query-2"

##
# APP
##
APP_JAR="${APP_HOME}/target/flink-scaffolding-1.0-jar-with-dependencies.jar"
APP_QUERY="query-2"
APP_OPTS=""
APP_OPTS="${APP_OPTS} --port 9000"
APP_OPTS="${APP_OPTS} --output ${APP_HOME}/out/query-2/main.out"
APP_OPTS="${APP_OPTS} --windowSize 10"
APP_OPTS="${APP_OPTS} --windowUnit SECONDS"
APP_OPTS="${APP_OPTS} --rankSize 3"
APP_OPTS="${APP_OPTS} --parallelism 3"

##
# EXECUTION
##
${FLINK_RUN} ${APP_JAR} ${APP_QUERY} ${APP_OPTS}
