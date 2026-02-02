"""Kafka producer module for sending messages."""
import json
import logging
from typing import Dict, Any
from kafka import KafkaProducer
from kafka.errors import KafkaError
from config import KafkaConfig

logger = logging.getLogger(__name__)


class KafkaMessageProducer:
    """Kafka message producer."""
    
    def __init__(self):
        """Initialize Kafka producer."""
        config = KafkaConfig.get_producer_config()
        self.producer = KafkaProducer(
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            **config
        )
        self.topic = KafkaConfig.TOPIC
        logger.info(f"Kafka producer initialized for topic: {self.topic}")
    
    def send_message(self, message: Dict[str, Any]) -> bool:
        """
        Send a message to Kafka.
        
        Args:
            message: Message dictionary to send
            
        Returns:
            True if message was sent successfully, False otherwise
        """
        try:
            future = self.producer.send(self.topic, message)
            # Wait for the message to be sent
            record_metadata = future.get(timeout=10)
            logger.debug(f"Message sent to topic {record_metadata.topic} "
                        f"partition {record_metadata.partition} "
                        f"offset {record_metadata.offset}")
            return True
        except KafkaError as e:
            logger.error(f"Failed to send message to Kafka: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending message: {e}")
            return False
    
    def flush(self):
        """Flush all pending messages."""
        self.producer.flush()
    
    def close(self):
        """Close the producer."""
        self.producer.close()
        logger.info("Kafka producer closed")
