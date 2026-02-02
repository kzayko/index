#!/usr/bin/env python3
"""Test script to verify Kafka connection with SASL."""
from kafka import KafkaAdminClient
from kafka.errors import KafkaError
from config import KafkaConfig

def test_connection():
    """Test Kafka connection."""
    print("Testing Kafka connection...")
    print(f"Bootstrap servers: {KafkaConfig.BOOTSTRAP_SERVERS}")
    print(f"Security protocol: {KafkaConfig.SECURITY_PROTOCOL}")
    print(f"SASL mechanism: {KafkaConfig.SASL_MECHANISM}")
    print(f"Username: {KafkaConfig.SASL_USERNAME}")
    
    try:
        config = KafkaConfig.get_producer_config()
        admin_client = KafkaAdminClient(**config)
        
        # Try to list topics
        topics = admin_client.list_topics()
        print(f"\n✓ Connection successful!")
        print(f"Found {len(topics)} topics:")
        for topic in topics:
            print(f"  - {topic}")
        
        admin_client.close()
        return True
    except KafkaError as e:
        print(f"\n✗ Kafka error: {e}")
        return False
    except Exception as e:
        print(f"\n✗ Error: {e}")
        return False

if __name__ == "__main__":
    import sys
    success = test_connection()
    sys.exit(0 if success else 1)
