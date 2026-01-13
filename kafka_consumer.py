"""Kafka consumer module."""
import json
import logging
from typing import Optional, Dict, Any, Callable
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from config import KafkaConfig

logger = logging.getLogger(__name__)


class KafkaMessageConsumer:
    """Kafka message consumer."""
    
    def __init__(self):
        """Initialize Kafka consumer."""
        config = KafkaConfig.get_consumer_config()
        self.consumer = KafkaConsumer(
            KafkaConfig.TOPIC,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            **config
        )
        logger.info(f"Kafka consumer initialized for topic: {KafkaConfig.TOPIC}")
    
    def consume_messages(self, callback: Callable[[Dict[str, Any]], None]):
        """
        Consume messages from Kafka and call callback for each message.
        
        Args:
            callback: Function to call with each message
        """
        try:
            for message in self.consumer:
                try:
                    callback(message.value)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
        except KafkaError as e:
            logger.error(f"Kafka error: {e}")
        finally:
            self.consumer.close()
    
    def extract_message_data(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Extract required fields from Kafka message.
        
        Args:
            message: Raw message from Kafka
            
        Returns:
            Dictionary with user_id, chat_id, message_id, text or None if missing fields
        """
        try:
            # Extract user_id from root level
            user_id = message.get('user_id')
            if not user_id:
                logger.warning("Missing user_id in message")
                return None
            
            # Extract other fields from event_properties
            event_properties = message.get('event_properties', {})
            chat_id = event_properties.get('chat_id')
            message_id = event_properties.get('message_id')
            text = event_properties.get('text')
            
            # Validate all required fields
            if not all([user_id, chat_id, message_id, text]):
                logger.warning(f"Missing required fields in message: user_id={user_id}, "
                             f"chat_id={chat_id}, message_id={message_id}, text={'present' if text else 'missing'}")
                return None
            
            # Extract timestamp if available
            timestamp = message.get('time')
            
            return {
                'user_id': str(user_id),
                'chat_id': str(chat_id),
                'message_id': str(message_id),
                'text': str(text),
                'timestamp': timestamp
            }
        except Exception as e:
            logger.error(f"Error extracting message data: {e}")
            return None
