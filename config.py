"""Configuration module for loading environment variables."""
import os
from dotenv import load_dotenv

# Load .env file (don't override existing env vars)
load_dotenv(override=False)

# Load secrets.env if it exists (for runtime secrets)
# Try multiple paths: local, /app (Docker), and current directory
# Note: Don't override environment variables that are already set (from docker-compose environment section)
secrets_paths = [
    os.path.join(os.path.dirname(__file__), 'secrets.env'),
    '/app/secrets.env',
    'secrets.env'
]
for secrets_path in secrets_paths:
    if os.path.exists(secrets_path):
        # Only load variables that are not already set in environment
        load_dotenv(secrets_path, override=False)  # Don't override existing env vars
        break


class KafkaConfig:
    """Kafka connection configuration."""
    BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    TOPIC = os.getenv('KAFKA_TOPIC', 'messages')
    GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'indexer-group')
    SECURITY_PROTOCOL = os.getenv('KAFKA_SECURITY_PROTOCOL', 'PLAINTEXT')
    SASL_MECHANISM = os.getenv('KAFKA_SASL_MECHANISM', '')
    # Support both KAFKA_SASL_USERNAME and KAFKA_USERNAME
    SASL_USERNAME = os.getenv('KAFKA_SASL_USERNAME') or os.getenv('KAFKA_USERNAME', '')
    # Support both KAFKA_SASL_PASSWORD and KAFKA_PASSWORD
    SASL_PASSWORD = os.getenv('KAFKA_SASL_PASSWORD') or os.getenv('KAFKA_PASSWORD', '')
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
    
    @classmethod
    def get_producer_config(cls):
        """Get Kafka producer configuration."""
        config = {
            'bootstrap_servers': cls.BOOTSTRAP_SERVERS.split(','),
        }
        
        if cls.SECURITY_PROTOCOL != 'PLAINTEXT':
            config['security_protocol'] = cls.SECURITY_PROTOCOL
            
        if cls.SASL_MECHANISM:
            config['sasl_mechanism'] = cls.SASL_MECHANISM
            # Use producer credentials if available, otherwise use indexer credentials
            producer_username = os.getenv('PRODUCER_USERNAME', cls.SASL_USERNAME)
            producer_password = os.getenv('PRODUCER_PASSWORD', cls.SASL_PASSWORD)
            config['sasl_plain_username'] = producer_username
            config['sasl_plain_password'] = producer_password
            
        if cls.SSL_CAFILE:
            config['ssl_cafile'] = cls.SSL_CAFILE
        if cls.SSL_CERTFILE:
            config['ssl_certfile'] = cls.SSL_CERTFILE
        if cls.SSL_KEYFILE:
            config['ssl_keyfile'] = cls.SSL_KEYFILE
            
        return config


class ElasticsearchConfig:
    """Elasticsearch connection configuration."""
    # Read variables directly from environment (set at class definition time)
    # This ensures we get variables from docker-compose environment section
    _hosts = os.getenv('ELASTICSEARCH_HOSTS', 'http://localhost:9200')
    HOSTS = _hosts.split(',') if isinstance(_hosts, str) else _hosts
    INDEX = os.getenv('ELASTICSEARCH_INDEX', 'messages_index')
    # Support both ELASTICSEARCH_USERNAME and ELASTICSEARCH_APP_USERNAME
    # Read directly from environment without dotenv override
    USERNAME = os.getenv('ELASTICSEARCH_USERNAME') or os.getenv('ELASTICSEARCH_APP_USERNAME', '')
    # Support both ELASTICSEARCH_PASSWORD and ELASTICSEARCH_APP_PASSWORD
    PASSWORD = os.getenv('ELASTICSEARCH_PASSWORD') or os.getenv('ELASTICSEARCH_APP_PASSWORD', '')
    USE_SSL = os.getenv('ELASTICSEARCH_USE_SSL', 'false').lower() == 'true'
    VERIFY_CERTS = os.getenv('ELASTICSEARCH_VERIFY_CERTS', 'false').lower() == 'true'
    
    # Debug: log configuration on first access
    _debug_logged = False
    @classmethod
    def _debug_log(cls):
        """Log configuration for debugging."""
        if not cls._debug_logged:
            import logging
            logger = logging.getLogger(__name__)
            logger.info(f"ElasticsearchConfig: USERNAME={cls.USERNAME[:3] + '***' if cls.USERNAME else 'None'}, "
                       f"PASSWORD={'***' if cls.PASSWORD else 'None'}, HOSTS={cls.HOSTS}")
            cls._debug_logged = True

    @classmethod
    def get_client_config(cls):
        """Get Elasticsearch client configuration."""
        cls._debug_log()  # Log configuration for debugging
        config = {
            'hosts': cls.HOSTS,
            'verify_certs': cls.VERIFY_CERTS,
        }
        
        if cls.USERNAME and cls.PASSWORD:
            config['basic_auth'] = (cls.USERNAME, cls.PASSWORD)
        else:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Elasticsearch credentials missing: USERNAME={bool(cls.USERNAME)}, PASSWORD={bool(cls.PASSWORD)}")
            
        return config


class APIConfig:
    """API server configuration."""
    HOST = os.getenv('API_HOST', '0.0.0.0')
    PORT = int(os.getenv('API_PORT', '8000'))
