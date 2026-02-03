"""Elasticsearch client module."""
import json
import logging
from typing import Optional, Dict, Any, List
from elasticsearch import Elasticsearch
from elasticsearch import ApiError
from config import ElasticsearchConfig

logger = logging.getLogger(__name__)


class ElasticsearchClient:
    """Elasticsearch client wrapper."""
    
    def __init__(self):
        """Initialize Elasticsearch client."""
        config = ElasticsearchConfig.get_client_config()
        self.client = Elasticsearch(**config)
        self.index_name = ElasticsearchConfig.INDEX
        
    def check_connection(self) -> bool:
        """Check if connection to Elasticsearch is available."""
        try:
            return self.client.ping()
        except Exception as e:
            logger.error(f"Elasticsearch connection error: {e}")
            return False
    
    def create_index(self, mappings: Optional[Dict[str, Any]] = None) -> bool:
        """
        Create index with mappings.
        
        Args:
            mappings: Optional custom mappings for the index
            
        Returns:
            True if index was created, False otherwise
        """
        if mappings is None:
            mappings = {
                "properties": {
                    "user_id": {"type": "keyword"},
                    "chat_id": {"type": "keyword"},
                    "message_id": {"type": "keyword"},
                    "text": {
                        "type": "text",
                        "analyzer": "standard"
                    },
                    "timestamp": {"type": "date"}
                }
            }
        
        try:
            if self.client.indices.exists(index=self.index_name):
                logger.info(f"Index {self.index_name} already exists")
                return True
                
            # For Elasticsearch 8.x, pass mappings directly
            self.client.indices.create(index=self.index_name, mappings=mappings)
            logger.info(f"Index {self.index_name} created successfully")
            return True
        except Exception as e:
            logger.error(f"Error creating index: {e}")
            return False
    
    def index_document(self, document: Dict[str, Any]) -> bool:
        """
        Index a single document.
        
        Only the following fields are indexed:
        - user_id (required)
        - chat_id (required)
        - message_id (required, used as document ID)
        - text (required)
        - timestamp (optional)
        
        All other fields are ignored.
        
        Args:
            document: Document to index (must contain user_id, chat_id, message_id, text)
            
        Returns:
            True if document was indexed, False otherwise
        """
        # Validate required fields
        required_fields = ['user_id', 'chat_id', 'message_id', 'text']
        if not all(field in document for field in required_fields):
            logger.warning(f"Document missing required fields: {document}")
            return False
        
        try:
            # Use message_id as document ID to prevent duplicates
            doc_id = document['message_id']
            
            # Create a clean document with only the fields we want to index
            # This ensures no extra fields from the original message are indexed
            clean_document = {
                'user_id': document['user_id'],
                'chat_id': document['chat_id'],
                'message_id': document['message_id'],
                'text': document['text']
            }
            
            # Add timestamp if present
            if 'timestamp' in document and document['timestamp'] is not None:
                clean_document['timestamp'] = document['timestamp']
            
            self.client.index(
                index=self.index_name,
                id=doc_id,
                document=clean_document
            )
            logger.debug(f"Document indexed: {doc_id}")
            return True
        except Exception as e:
            logger.error(f"Error indexing document: {e}")
            return False
    
    def bulk_index(self, documents: List[Dict[str, Any]]) -> int:
        """
        Bulk index multiple documents.
        
        Only the following fields are indexed for each document:
        - user_id (required)
        - chat_id (required)
        - message_id (required, used as document ID)
        - text (required)
        - timestamp (optional)
        
        All other fields are ignored.
        
        Args:
            documents: List of documents to index
            
        Returns:
            Number of successfully indexed documents
        """
        if not documents:
            return 0
        
        actions = []
        required_fields = ['user_id', 'chat_id', 'message_id', 'text']
        
        for doc in documents:
            # Validate required fields
            if not all(field in doc for field in required_fields):
                logger.warning(f"Skipping document missing required fields: {doc}")
                continue
            
            # Create a clean document with only the fields we want to index
            clean_document = {
                'user_id': doc['user_id'],
                'chat_id': doc['chat_id'],
                'message_id': doc['message_id'],
                'text': doc['text']
            }
            
            # Add timestamp if present
            if 'timestamp' in doc and doc['timestamp'] is not None:
                clean_document['timestamp'] = doc['timestamp']
                
            action = {
                "_index": self.index_name,
                "_id": doc['message_id'],
                "_source": clean_document
            }
            actions.append(action)
        
        if not actions:
            return 0
        
        try:
            from elasticsearch.helpers import bulk
            success, failed = bulk(self.client, actions)
            logger.info(f"Bulk indexed: {success} successful, {len(failed)} failed")
            return success
        except Exception as e:
            logger.error(f"Error in bulk indexing: {e}")
            return 0
    
    def search(self, query_string: str, size: int = 10000) -> List[str]:
        """
        Search for messages by text.
        
        Uses query_string query (same as Elasticsearch GUI) to find all matching documents.
        This matches the behavior of Elasticsearch Dev Tools / Kibana Discover.
        
        Args:
            query_string: Search query string
            size: Maximum number of results (default 10000 to get all results)
            
        Returns:
            List of message_id values
        """
        try:
            # Ensure query_string is a clean string without extra quotes or whitespace
            query_string = query_string.strip()
            
            # Log the query string with repr to see exact bytes
            logger.debug(f"Search query string (repr): {repr(query_string)}")
            logger.debug(f"Search query string (utf-8): {query_string.encode('utf-8')}")
            
            # Use query_string query (same as Elasticsearch GUI)
            # Don't specify fields - let it search across all fields like GUI does
            query = {
                "query": {
                    "query_string": {
                        "query": query_string
                    }
                },
                "size": min(size, 10000),  # Elasticsearch default max is 10000
                "from": 0,  # Start from beginning
                "track_total_hits": True,  # Get accurate total count
                "sort": [],  # No sorting like in GUI
                "_source": ["message_id"]
            }
            
            # Log the query being sent
            import json
            logger.debug(f"Elasticsearch query: {json.dumps(query, ensure_ascii=False)}")
            
            # For Elasticsearch 8.x compatibility
            response = self.client.search(
                index=self.index_name,
                body=query
            )
            
            # Log total hits for debugging
            total_hits = response.get("hits", {}).get("total", {})
            if isinstance(total_hits, dict):
                total_value = total_hits.get("value", 0)
            else:
                total_value = total_hits
            logger.info(f"Search for '{query_string}' (bytes: {query_string.encode('utf-8')}): found {total_value} total matches, returning {len(response['hits']['hits'])} results")
            
            message_ids = [
                hit["_source"]["message_id"] 
                for hit in response["hits"]["hits"]
            ]
            return message_ids
        except Exception as e:
            logger.error(f"Error searching: {e}", exc_info=True)
            return []
