#!/bin/bash
# Generate kafka-jaas.conf from environment variables
# This script reads from secrets.env and generates the JAAS configuration

set -e

# Load secrets.env if it exists
if [ -f secrets.env ]; then
    set -a
    source secrets.env
    set +a
elif [ -f /app/secrets.env ]; then
    set -a
    source /app/secrets.env
    set +a
fi

KAFKA_ADMIN_USERNAME=${KAFKA_ADMIN_USERNAME:-kafka_admin}
KAFKA_ADMIN_PASSWORD=${KAFKA_ADMIN_PASSWORD:-kafka_admin_password_123}
KAFKA_USERNAME=${KAFKA_USERNAME:-indexer_user}
KAFKA_PASSWORD=${KAFKA_PASSWORD:-indexer_password_123}
PRODUCER_USERNAME=${PRODUCER_USERNAME:-producer_user}
PRODUCER_PASSWORD=${PRODUCER_PASSWORD:-producer_password_123}

cat > kafka-jaas.conf <<EOF
KafkaServer {
    org.apache.kafka.common.security.plain.PlainLoginModule required
    username="${KAFKA_ADMIN_USERNAME}"
    password="${KAFKA_ADMIN_PASSWORD}"
    user_${KAFKA_ADMIN_USERNAME}="${KAFKA_ADMIN_PASSWORD}"
    user_${KAFKA_USERNAME}="${KAFKA_PASSWORD}"
    user_${PRODUCER_USERNAME}="${PRODUCER_PASSWORD}";
};

Client {
    org.apache.kafka.common.security.plain.PlainLoginModule required
    username="${KAFKA_ADMIN_USERNAME}"
    password="${KAFKA_ADMIN_PASSWORD}";
};
EOF

echo "Generated kafka-jaas.conf with users:"
echo "  Admin: ${KAFKA_ADMIN_USERNAME}"
echo "  Indexer: ${KAFKA_USERNAME}"
echo "  Producer: ${PRODUCER_USERNAME}"
