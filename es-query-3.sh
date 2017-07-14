#!/bin/bash

##
# SETUP: ELASTICSEARCH
##
ES_CLUSTER="my-es-cluster"
ES_ADDRESS="localhost:9200"
ES_INDEX="fsq4"
ES_TYPE_NAME="output"
ES_TYPE_SCHEMA="{
"properties": {
    "wStart": {"type": "date"},
    "wEnd":   {"type": "date"},
    "rank":   {"type": "text"}
}}"

echo "[Elasticsearch]> Setting up ${ES_INDEX}/${ES_TYPE_NAME} with schema ${ES_TYPE_SCHEMA} ..."
curl -XDELETE http://${ES_ADDRESS}/${ES_INDEX};
curl -XPUT http://${ES_ADDRESS}/${ES_INDEX};
curl -XPUT http://${ES_ADDRESS}/${ES_INDEX}/_mapping/${ES_TYPE_NAME} -H "Content-Type: application/json" -d'${ES_TYPE_SCHEMA}'
echo "[Elasticsearch]> ${ES_INDEX}/${ES_TYPE_NAME} set up"