"""Script to initialize Elasticsearch index."""
import logging
import sys
from elasticsearch_client import ElasticsearchClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Initialize Elasticsearch index."""
    es_client = ElasticsearchClient()
    
    # Check connection
    if not es_client.check_connection():
        logger.error("Cannot connect to Elasticsearch. Please check your configuration.")
        sys.exit(1)
    
    # Create index
    logger.info("Creating Elasticsearch index...")
    success = es_client.create_index()
    
    if success:
        logger.info("Index initialized successfully")
        sys.exit(0)
    else:
        logger.error("Failed to initialize index")
        sys.exit(1)


if __name__ == "__main__":
    main()
