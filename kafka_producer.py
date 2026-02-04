"""Kafka producer module for sending messages."""
import json
import logging
from typing import Dict, Any, List, Optional
from kafka import KafkaProducer
from kafka.errors import KafkaError
from config import KafkaConfig
from threading import Lock

logger = logging.getLogger(__name__)


class KafkaMessageProducer:
    """Kafka message producer with async sending and batching support."""
    
    def __init__(self, batch_size: int = 100, async_mode: bool = True):
        """
        Initialize Kafka producer.
        
        Args:
            batch_size: Number of messages to batch before flushing (default: 100)
            async_mode: If True, send messages asynchronously without waiting for confirmation (default: True)
        """
        config = KafkaConfig.get_producer_config()
        # Optimize producer for batch sending
        producer_config = {
            'batch_size': 16384,  # 16KB batch size
            'linger_ms': 10,  # Wait up to 10ms to fill batch
            'buffer_memory': 33554432,  # 32MB buffer
            'max_in_flight_requests_per_connection': 5,  # Allow multiple in-flight requests
            'compression_type': 'gzip',  # Compress batches
            **config
        }
        self.producer = KafkaProducer(
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            **producer_config
        )
        self.topic = KafkaConfig.TOPIC
        self.batch_size = batch_size
        self.async_mode = async_mode
        self.pending_messages = []
        self.pending_futures = []
        self.lock = Lock()
        self.error_count = 0
        logger.info(f"Kafka producer initialized for topic: {self.topic} "
                   f"(batch_size={batch_size}, async={async_mode})")
    
    def send_message(self, message: Dict[str, Any]) -> bool:
        """
        Send a message to Kafka (async or sync based on mode).
        
        Args:
            message: Message dictionary to send
            
        Returns:
            True if message was queued successfully, False otherwise
        """
        try:
            if self.async_mode:
                # Async mode: send without waiting, check errors in background
                future = self.producer.send(self.topic, message)
                with self.lock:
                    self.pending_futures.append(future)
                    # Check old futures for errors
                    self._check_pending_futures()
                return True
            else:
                # Sync mode: wait for confirmation (slower but safer)
                future = self.producer.send(self.topic, message)
                record_metadata = future.get(timeout=10)
                logger.debug(f"Message sent to topic {record_metadata.topic} "
                            f"partition {record_metadata.partition} "
                            f"offset {record_metadata.offset}")
                return True
        except KafkaError as e:
            logger.error(f"Failed to send message to Kafka: {e}")
            self.error_count += 1
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending message: {e}")
            self.error_count += 1
            return False
    
    def send_batch(self, messages: List[Dict[str, Any]]) -> int:
        """
        Send a batch of messages to Kafka.
        
        Args:
            messages: List of message dictionaries to send
            
        Returns:
            Number of messages successfully queued
        """
        sent_count = 0
        for message in messages:
            if self.send_message(message):
                sent_count += 1
        return sent_count
    
    def _check_pending_futures(self, max_check: int = 100):
        """Check pending futures for errors (non-blocking)."""
        if not self.pending_futures:
            return
        
        # Only check a limited number to avoid blocking
        checked = 0
        remaining = []
        for future in self.pending_futures:
            if checked >= max_check:
                remaining.append(future)
                continue
            
            try:
                # Check if future is done (non-blocking)
                if future.is_done:
                    try:
                        future.get(timeout=0)  # Non-blocking get
                    except Exception as e:
                        logger.error(f"Error in async send: {e}")
                        self.error_count += 1
                else:
                    remaining.append(future)
            except Exception:
                remaining.append(future)
            
            checked += 1
        
        self.pending_futures = remaining
    
    def flush(self):
        """Flush all pending messages and wait for completion."""
        # Check all pending futures
        with self.lock:
            for future in self.pending_futures:
                try:
                    future.get(timeout=30)  # Wait for completion
                except Exception as e:
                    logger.error(f"Error flushing message: {e}")
                    self.error_count += 1
            self.pending_futures = []
        
        # Flush producer
        self.producer.flush()
        
        if self.error_count > 0:
            logger.warning(f"Total errors during sending: {self.error_count}")
    
    def close(self):
        """Close the producer."""
        # Flush before closing
        self.flush()
        self.producer.close()
        logger.info(f"Kafka producer closed (total errors: {self.error_count})")
