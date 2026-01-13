"""Script for bulk loading messages from a text file into Elasticsearch."""
import json
import logging
import sys
import argparse
from elasticsearch_client import ElasticsearchClient
from kafka_consumer import KafkaMessageConsumer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_messages_from_file(file_path: str) -> list:
    """
    Load messages from a text file (one JSON object per line).
    
    Args:
        file_path: Path to the text file
        
    Returns:
        List of parsed message dictionaries
    """
    messages = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    message = json.loads(line)
                    messages.append(message)
                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON on line {line_num}: {e}")
        logger.info(f"Loaded {len(messages)} messages from {file_path}")
        return messages
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return []
    except Exception as e:
        logger.error(f"Error reading file: {e}")
        return []


def main():
    """Main function for bulk loading."""
    parser = argparse.ArgumentParser(description='Bulk load messages from file to Elasticsearch')
    parser.add_argument('file', help='Path to text file with messages (one JSON per line)')
    args = parser.parse_args()
    
    es_client = ElasticsearchClient()
    kafka_consumer = KafkaMessageConsumer()
    
    # Check connection
    if not es_client.check_connection():
        logger.error("Cannot connect to Elasticsearch. Please check your configuration.")
        sys.exit(1)
    
    # Load messages from file
    logger.info(f"Loading messages from {args.file}...")
    messages = load_messages_from_file(args.file)
    
    if not messages:
        logger.error("No valid messages found in file")
        sys.exit(1)
    
    # Extract and prepare documents
    documents = []
    for message in messages:
        doc = kafka_consumer.extract_message_data(message)
        if doc:
            documents.append(doc)
        else:
            logger.debug("Skipping message due to missing required fields")
    
    if not documents:
        logger.error("No valid documents to index")
        sys.exit(1)
    
    # Bulk index
    logger.info(f"Indexing {len(documents)} documents...")
    indexed_count = es_client.bulk_index(documents)
    
    logger.info(f"Successfully indexed {indexed_count} documents")
    sys.exit(0 if indexed_count > 0 else 1)


if __name__ == "__main__":
    main()
