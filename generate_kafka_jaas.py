#!/usr/bin/env python3
"""Generate kafka-jaas.conf from environment variables."""
import os
from dotenv import load_dotenv

# Load secrets.env if it exists (override with environment variables)
secrets_path = os.path.join(os.path.dirname(__file__), 'secrets.env')
if os.path.exists(secrets_path):
    load_dotenv(secrets_path, override=False)

KAFKA_ADMIN_USERNAME = os.getenv('KAFKA_ADMIN_USERNAME', 'kafka_admin')
KAFKA_ADMIN_PASSWORD = os.getenv('KAFKA_ADMIN_PASSWORD', 'kafka_admin_password_123')
KAFKA_USERNAME = os.getenv('KAFKA_USERNAME', 'indexer_user')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD', 'indexer_password_123')
PRODUCER_USERNAME = os.getenv('PRODUCER_USERNAME', 'producer_user')
PRODUCER_PASSWORD = os.getenv('PRODUCER_PASSWORD', 'producer_password_123')

JAAS_CONFIG = f"""KafkaServer {{
    org.apache.kafka.common.security.plain.PlainLoginModule required
    username="{KAFKA_ADMIN_USERNAME}"
    password="{KAFKA_ADMIN_PASSWORD}"
    user_{KAFKA_ADMIN_USERNAME}="{KAFKA_ADMIN_PASSWORD}"
    user_{KAFKA_USERNAME}="{KAFKA_PASSWORD}"
    user_{PRODUCER_USERNAME}="{PRODUCER_PASSWORD}";
}};
"""

if __name__ == "__main__":
    with open('kafka-jaas.conf', 'w') as f:
        f.write(JAAS_CONFIG)
    
    print("Generated kafka-jaas.conf with users:")
    print(f"  Admin: {KAFKA_ADMIN_USERNAME}")
    print(f"  Indexer: {KAFKA_USERNAME}")
    print(f"  Producer: {PRODUCER_USERNAME}")
