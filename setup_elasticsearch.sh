#!/bin/bash
# Script to setup Elasticsearch index and verify connection
# This script can be run after Elasticsearch is started

echo "Waiting for Elasticsearch to be ready..."
sleep 10

ELASTIC_PASSWORD=${ELASTIC_PASSWORD:-changeme}
ELASTICSEARCH_HOST=${ELASTICSEARCH_HOST:-http://localhost:9200}

# Check if Elasticsearch is ready
until curl -u elastic:${ELASTIC_PASSWORD} -f ${ELASTICSEARCH_HOST}/_cluster/health; do
    echo "Waiting for Elasticsearch..."
    sleep 5
done

echo "Elasticsearch is ready!"
echo "You can now run: python init_index.py"
