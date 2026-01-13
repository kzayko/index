"""Main indexer service that consumes from Kafka and indexes to Elasticsearch."""
import logging
import signal
import sys
from kafka_consumer import KafkaMessageConsumer
from elasticsearch_client import ElasticsearchClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IndexerService:
    """Service that indexes messages from Kafka to Elasticsearch."""
    
    def __init__(self):
        """Initialize the indexer service."""
        self.es_client = ElasticsearchClient()
        self.kafka_consumer = KafkaMessageConsumer()
        self.running = True
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info("Received shutdown signal, stopping...")
        self.running = False
    
    def start(self):
        """Start the indexer service."""
        # Check Elasticsearch connection
        if not self.es_client.check_connection():
            logger.error("Cannot connect to Elasticsearch. Exiting.")
            sys.exit(1)
        
        logger.info("Starting indexer service...")
        
        def process_message(message):
            """Process a single message from Kafka."""
            if not self.running:
                return
                
            # Extract required data from message
            doc = self.kafka_consumer.extract_message_data(message)
            
            if doc:
                # Index to Elasticsearch
                success = self.es_client.index_document(doc)
                if success:
                    logger.info(f"Indexed message: {doc['message_id']}")
                else:
                    logger.error(f"Failed to index message: {doc['message_id']}")
            else:
                logger.debug("Skipping message due to missing required fields")
        
        # Start consuming messages
        try:
            self.kafka_consumer.consume_messages(process_message)
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            logger.info("Indexer service stopped")


if __name__ == "__main__":
    service = IndexerService()
    service.start()
