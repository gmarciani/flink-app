#!/bin/bash

##
# KAFKA COMMANDS
##
KAFKA_PRODUCER="${KAFKA_HOME}/bin/kafka-console-producer.sh"

##
# KAFKA OPTIONS
##
BROKER_LIST="localhost:9092"
TOPIC_LIST="$1"

##
# KAFKA PRODUCER
##
${KAFKA_PRODUCER} --broker-list ${BROKER_LIST} --topic ${TOPIC_LIST}
