"""Configuration module for loading environment variables."""
import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Load secrets.env if it exists (for runtime secrets)
secrets_path = os.path.join(os.path.dirname(__file__), 'secrets.env')
if os.path.exists(secrets_path):
    load_dotenv(secrets_path, override=False)


class KafkaConfig:
    """Kafka connection configuration."""
    BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    TOPIC = os.getenv('KAFKA_TOPIC', 'messages')
    GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'indexer-group')
    SECURITY_PROTOCOL = os.getenv('KAFKA_SECURITY_PROTOCOL', 'PLAINTEXT')
    SASL_MECHANISM = os.getenv('KAFKA_SASL_MECHANISM', '')
    SASL_USERNAME = os.getenv('KAFKA_SASL_USERNAME', '')
    SASL_PASSWORD = os.getenv('KAFKA_SASL_PASSWORD', '')
    SSL_CAFILE = os.getenv('KAFKA_SSL_CAFILE', '')
    SSL_CERTFILE = os.getenv('KAFKA_SSL_CERTFILE', '')
    SSL_KEYFILE = os.getenv('KAFKA_SSL_KEYFILE', '')

    @classmethod
    def get_consumer_config(cls):
        """Get Kafka consumer configuration."""
        config = {
            'bootstrap_servers': cls.BOOTSTRAP_SERVERS.split(','),
            'group_id': cls.GROUP_ID,
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': True,
        }
        
        if cls.SECURITY_PROTOCOL != 'PLAINTEXT':
            config['security_protocol'] = cls.SECURITY_PROTOCOL
            
        if cls.SASL_MECHANISM:
            config['sasl_mechanism'] = cls.SASL_MECHANISM
            config['sasl_plain_username'] = cls.SASL_USERNAME
            config['sasl_plain_password'] = cls.SASL_PASSWORD
            
        if cls.SSL_CAFILE:
            config['ssl_cafile'] = cls.SSL_CAFILE
        if cls.SSL_CERTFILE:
            config['ssl_certfile'] = cls.SSL_CERTFILE
        if cls.SSL_KEYFILE:
            config['ssl_keyfile'] = cls.SSL_KEYFILE
            
        return config


class ElasticsearchConfig:
    """Elasticsearch connection configuration."""
    HOSTS = os.getenv('ELASTICSEARCH_HOSTS', 'http://localhost:9200').split(',')
    INDEX = os.getenv('ELASTICSEARCH_INDEX', 'messages_index')
    USERNAME = os.getenv('ELASTICSEARCH_USERNAME', '')
    PASSWORD = os.getenv('ELASTICSEARCH_PASSWORD', '')
    USE_SSL = os.getenv('ELASTICSEARCH_USE_SSL', 'false').lower() == 'true'
    VERIFY_CERTS = os.getenv('ELASTICSEARCH_VERIFY_CERTS', 'false').lower() == 'true'

    @classmethod
    def get_client_config(cls):
        """Get Elasticsearch client configuration."""
        config = {
            'hosts': cls.HOSTS,
            'verify_certs': cls.VERIFY_CERTS,
        }
        
        if cls.USERNAME and cls.PASSWORD:
            config['basic_auth'] = (cls.USERNAME, cls.PASSWORD)
            
        return config


class APIConfig:
    """API server configuration."""
    HOST = os.getenv('API_HOST', '0.0.0.0')
    PORT = int(os.getenv('API_PORT', '8000'))
