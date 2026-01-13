#!/bin/sh
# Script to create Elasticsearch users and roles
# This script should be run after Elasticsearch is started

set -e

ELASTIC_PASSWORD=${ELASTIC_PASSWORD:-elastic_password_123}
ELASTICSEARCH_HOST=${ELASTICSEARCH_HOST:-http://elasticsearch:9200}
APP_USERNAME=${ELASTICSEARCH_APP_USERNAME:-app_user}
APP_PASSWORD=${ELASTICSEARCH_APP_PASSWORD:-app_password_123}

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
