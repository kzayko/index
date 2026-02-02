#!/usr/bin/env python3
"""Generate kafka-jaas.conf from environment variables."""
import os
from dotenv import load_dotenv

# Load secrets.env if it exists (override with environment variables)
secrets_path = os.path.join(os.path.dirname(__file__), 'secrets.env')
if os.path.exists(secrets_path):
    load_dotenv(secrets_path, override=True)  # Override environment variables with secrets.env

# Load from environment (must be set from secrets.env)
KAFKA_ADMIN_USERNAME = os.getenv('KAFKA_ADMIN_USERNAME')
KAFKA_ADMIN_PASSWORD = os.getenv('KAFKA_ADMIN_PASSWORD')
KAFKA_USERNAME = os.getenv('KAFKA_USERNAME')
KAFKA_PASSWORD = os.getenv('KAFKA_PASSWORD')
PRODUCER_USERNAME = os.getenv('PRODUCER_USERNAME')
PRODUCER_PASSWORD = os.getenv('PRODUCER_PASSWORD')

# Validate that all required variables are set
if not all([KAFKA_ADMIN_USERNAME, KAFKA_ADMIN_PASSWORD, KAFKA_USERNAME, KAFKA_PASSWORD, PRODUCER_USERNAME, PRODUCER_PASSWORD]):
    missing = [var for var, val in [
        ('KAFKA_ADMIN_USERNAME', KAFKA_ADMIN_USERNAME),
        ('KAFKA_ADMIN_PASSWORD', KAFKA_ADMIN_PASSWORD),
        ('KAFKA_USERNAME', KAFKA_USERNAME),
        ('KAFKA_PASSWORD', KAFKA_PASSWORD),
        ('PRODUCER_USERNAME', PRODUCER_USERNAME),
        ('PRODUCER_PASSWORD', PRODUCER_PASSWORD)
    ] if not val]
    raise ValueError(f"Missing required environment variables: {', '.join(missing)}. Please set them in secrets.env")

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
