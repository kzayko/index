#!/bin/sh
# Script to create Elasticsearch users and roles
# This script should be run after Elasticsearch is started
# Uses environment variables (loaded from secrets.env via docker-compose env_file)

set -e

# Variables are already loaded from secrets.env via docker-compose env_file
# Must be set in secrets.env (no defaults)
ELASTIC_PASSWORD=${ELASTIC_PASSWORD}
ELASTICSEARCH_HOST=${ELASTICSEARCH_HOST:-http://elasticsearch:9200}
APP_USERNAME=${ELASTICSEARCH_APP_USERNAME}
APP_PASSWORD=${ELASTICSEARCH_APP_PASSWORD}

# Validate required variables
if [ -z "$ELASTIC_PASSWORD" ] || [ -z "$APP_USERNAME" ] || [ -z "$APP_PASSWORD" ]; then
    echo "Error: Required variables not set. Please set ELASTIC_PASSWORD, ELASTICSEARCH_APP_USERNAME, and ELASTICSEARCH_APP_PASSWORD in secrets.env"
    exit 1
fi

echo "Waiting for Elasticsearch to be ready..."
max_retries=30
retry=0
while [ $retry -lt $max_retries ]; do
    if curl -u "elastic:${ELASTIC_PASSWORD}" -f "${ELASTICSEARCH_HOST}/_cluster/health" > /dev/null 2>&1; then
        echo "Elasticsearch is ready!"
        break
    fi
    retry=$((retry + 1))
    echo "Waiting for Elasticsearch... ($retry/$max_retries)"
    sleep 5
done

if [ $retry -eq $max_retries ]; then
    echo "Failed to connect to Elasticsearch"
    exit 1
fi

# Create application user with read/write permissions
echo "Creating application user: ${APP_USERNAME}"

response=$(curl -s -w "\n%{http_code}" -X POST "${ELASTICSEARCH_HOST}/_security/user/${APP_USERNAME}" \
  -u "elastic:${ELASTIC_PASSWORD}" \
  -H "Content-Type: application/json" \
  -d "{
    \"password\": \"${APP_PASSWORD}\",
    \"roles\": [\"superuser\"],
    \"full_name\": \"Application User\",
    \"email\": \"app@example.com\"
  }")

http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | sed '$d')

if [ "$http_code" = "200" ] || [ "$http_code" = "201" ]; then
    echo "Application user '${APP_USERNAME}' created successfully!"
elif [ "$http_code" = "409" ]; then
    echo "User '${APP_USERNAME}' already exists. Updating password..."
    update_response=$(curl -s -w "\n%{http_code}" -X POST "${ELASTICSEARCH_HOST}/_security/user/${APP_USERNAME}/_password" \
      -u "elastic:${ELASTIC_PASSWORD}" \
      -H "Content-Type: application/json" \
      -d "{\"password\": \"${APP_PASSWORD}\"}")
    update_code=$(echo "$update_response" | tail -n1)
    if [ "$update_code" = "200" ] || [ "$update_code" = "204" ]; then
        echo "Password updated for user '${APP_USERNAME}'"
    else
        echo "Failed to update password"
        exit 1
    fi
else
    echo "Failed to create user: HTTP $http_code"
    echo "$body"
    exit 1
fi

echo ""
echo "=================================================="
echo "Elasticsearch user configuration:"
echo "  Username: ${APP_USERNAME}"
echo "  Password: ${APP_PASSWORD}"
echo "=================================================="
