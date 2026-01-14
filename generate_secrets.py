#!/usr/bin/env python3
"""Generate random passwords and write them to secrets.env and .env files."""
import secrets
import string
import os


def generate_password(length=24):
    """Generate a secure random password."""
    chars = string.ascii_letters + string.digits + '!@#$%^&*-_=+'
    return ''.join(secrets.choice(chars) for _ in range(length))


def generate_all_secrets():
    """Generate all required passwords."""
    return {
        'KAFKA_ADMIN_USERNAME': 'kafka_admin',
        'KAFKA_ADMIN_PASSWORD': generate_password(),
        'KAFKA_USERNAME': 'indexer_user',
        'KAFKA_PASSWORD': generate_password(),
        'PRODUCER_USERNAME': 'producer_user',
        'PRODUCER_PASSWORD': generate_password(),
        'ELASTIC_PASSWORD': generate_password(),
        'ELASTICSEARCH_APP_USERNAME': 'app_user',
        'ELASTICSEARCH_APP_PASSWORD': generate_password(),
    }


def write_secrets_env(secrets_dict):
    """Write secrets to secrets.env file."""
    content = """# Secrets configuration file
# DO NOT commit this file to git!
# Generated automatically - contains sensitive passwords

# Kafka SASL Configuration
KAFKA_ADMIN_USERNAME={KAFKA_ADMIN_USERNAME}
KAFKA_ADMIN_PASSWORD={KAFKA_ADMIN_PASSWORD}
KAFKA_USERNAME={KAFKA_USERNAME}
KAFKA_PASSWORD={KAFKA_PASSWORD}
PRODUCER_USERNAME={PRODUCER_USERNAME}
PRODUCER_PASSWORD={PRODUCER_PASSWORD}

# Elasticsearch Configuration
# Main elastic user password (superuser)
ELASTIC_PASSWORD={ELASTIC_PASSWORD}

# Application user for Elasticsearch (with limited permissions)
ELASTICSEARCH_APP_USERNAME={ELASTICSEARCH_APP_USERNAME}
ELASTICSEARCH_APP_PASSWORD={ELASTICSEARCH_APP_PASSWORD}
""".format(**secrets_dict)
    
    with open('secrets.env', 'w') as f:
        f.write(content)
    
    print("[OK] Generated secrets.env with random passwords")


def write_env_file(secrets_dict):
    """Write configuration to .env file (without passwords)."""
    content = """# Application Configuration
# Main configuration file (non-sensitive settings)

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=messages
KAFKA_GROUP_ID=indexer-group
KAFKA_SECURITY_PROTOCOL=SASL_PLAINTEXT
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SASL_USERNAME={KAFKA_USERNAME}
# KAFKA_SASL_PASSWORD is in secrets.env

# Elasticsearch Configuration
ELASTICSEARCH_HOSTS=http://localhost:9200
ELASTICSEARCH_INDEX=messages_index
ELASTICSEARCH_USERNAME={ELASTICSEARCH_APP_USERNAME}
# ELASTICSEARCH_PASSWORD is in secrets.env
ELASTICSEARCH_USE_SSL=false
ELASTICSEARCH_VERIFY_CERTS=false

# API Configuration
API_HOST=0.0.0.0
API_PORT=8000
""".format(**secrets_dict)
    
    with open('.env', 'w') as f:
        f.write(content)
    
    print("[OK] Generated .env with configuration (passwords in secrets.env)")


def main():
    """Main function."""
    print("Generating secure random passwords...")
    secrets_dict = generate_all_secrets()
    
    # Write secrets.env
    write_secrets_env(secrets_dict)
    
    # Write .env
    write_env_file(secrets_dict)
    
    print("\n" + "="*60)
    print("Generated passwords for:")
    print(f"  Kafka Admin: {secrets_dict['KAFKA_ADMIN_USERNAME']}")
    print(f"  Kafka Indexer: {secrets_dict['KAFKA_USERNAME']}")
    print(f"  Kafka Producer: {secrets_dict['PRODUCER_USERNAME']}")
    print(f"  Elasticsearch (elastic): {secrets_dict['ELASTIC_PASSWORD'][:8]}...")
    print(f"  Elasticsearch App: {secrets_dict['ELASTICSEARCH_APP_USERNAME']}")
    print("="*60)
    print("\n[OK] Configuration files created successfully!")
    print("  - secrets.env (contains all passwords)")
    print("  - .env (contains non-sensitive configuration)")


if __name__ == "__main__":
    main()
