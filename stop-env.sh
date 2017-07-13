#!/bin/bash

##
# ENVARS
##
export APP_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

##
# FLINK
##
${APP_HOME}/env/systemd/flink-manager.sh stop

##
# KAFKA
##
${APP_HOME}/env/systemd/kafka-manager.sh stop

##
# ENVARS
##
unset APP_HOME
unset FLINK_CONF_DIR
