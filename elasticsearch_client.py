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
    
    def search(
        self, 
        query_string: str, 
        user_id: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        size: int = 10000
    ) -> List[Dict[str, Any]]:
        """
        Search for messages by text with optional filters.
        
        Uses query_string query (same as Elasticsearch GUI) to find all matching documents.
        Supports filtering by user_id and date range.
        
        Args:
            query_string: Search query string (required)
            user_id: Optional filter by exact user_id match
            date_from: Optional start date (ISO 8601 format, e.g., "2024-01-01" or "2024-01-01T00:00:00")
            date_to: Optional end date (ISO 8601 format, e.g., "2024-12-31" or "2024-12-31T23:59:59")
            size: Maximum number of results (default 10000 to get all results)
            
        Returns:
            List of documents with all fields (user_id, chat_id, message_id, text, timestamp)
        """
        try:
            # Ensure query_string is a clean string without extra quotes or whitespace
            query_string = query_string.strip()
            
            if not query_string:
                raise ValueError("query_string cannot be empty")
            
            # Log the query string with repr to see exact bytes
            logger.debug(f"Search query string (repr): {repr(query_string)}")
            logger.debug(f"Search query string (utf-8): {query_string.encode('utf-8')}")
            
            # Build bool query with must (text search) and filters (user_id, date range)
            must_clauses = []
            filter_clauses = []
            
            # Text search using query_string
            must_clauses.append({
                "query_string": {
                    "query": query_string
                }
            })
            
            # Filter by user_id if provided
            if user_id:
                filter_clauses.append({
                    "term": {
                        "user_id": user_id
                    }
                })
                logger.debug(f"Filtering by user_id: {user_id}")
            
            # Filter by date range if provided
            if date_from or date_to:
                from datetime import datetime
                date_range = {}
                
                # Convert ISO 8601 strings to Unix timestamps (seconds)
                # Timestamp is stored as Unix timestamp (integer), but mapped as date type
                # Elasticsearch date type accepts epoch_second format
                if date_from:
                    try:
                        # Try parsing as ISO 8601 string
                        if 'T' in date_from or len(date_from) == 10:  # ISO format or date only
                            # Handle timezone
                            date_str = date_from.replace('Z', '+00:00')
                            if '+' not in date_str and '-' in date_str and len(date_str) > 10:
                                # Might be missing timezone, assume local
                                pass
                            try:
                                dt = datetime.fromisoformat(date_str)
                            except ValueError:
                                # Try parsing as date only
                                dt = datetime.strptime(date_from, '%Y-%m-%d')
                            # Convert to Unix timestamp (seconds)
                            date_range["gte"] = int(dt.timestamp())
                        else:
                            # Assume it's already a Unix timestamp string
                            date_range["gte"] = int(float(date_from))
                    except (ValueError, AttributeError, TypeError) as e:
                        logger.warning(f"Failed to parse date_from '{date_from}': {e}")
                        # Skip date filter if parsing fails
                        date_from = None
                
                if date_to:
                    try:
                        # Try parsing as ISO 8601 string
                        if 'T' in date_to or len(date_to) == 10:  # ISO format or date only
                            # Handle timezone
                            date_str = date_to.replace('Z', '+00:00')
                            if '+' not in date_str and '-' in date_str and len(date_str) > 10:
                                # Might be missing timezone, assume local
                                pass
                            try:
                                dt = datetime.fromisoformat(date_str)
                            except ValueError:
                                # Try parsing as date only, set to end of day
                                dt = datetime.strptime(date_to, '%Y-%m-%d')
                                dt = dt.replace(hour=23, minute=59, second=59)
                            # Convert to Unix timestamp (seconds)
                            date_range["lte"] = int(dt.timestamp())
                        else:
                            # Assume it's already a Unix timestamp string
                            date_range["lte"] = int(float(date_to))
                    except (ValueError, AttributeError, TypeError) as e:
                        logger.warning(f"Failed to parse date_to '{date_to}': {e}")
                        # Skip date filter if parsing fails
                        date_to = None
                
                # Only add date filter if we have valid dates
                if date_range:
                    # Elasticsearch date type accepts epoch_second (Unix timestamp in seconds) as integer
                    filter_clauses.append({
                        "range": {
                            "timestamp": date_range
                        }
                    })
                    logger.debug(f"Filtering by date range: {date_from} to {date_to} (converted to Unix timestamps: {date_range})")
            
            # Build the query
            query = {
                "query": {
                    "bool": {
                        "must": must_clauses
                    }
                },
                "size": min(size, 10000),  # Elasticsearch default max is 10000
                "from": 0,  # Start from beginning
                "track_total_hits": True,  # Get accurate total count
                "sort": [],  # No sorting like in GUI
                "_source": True  # Return all fields
            }
            
            # Add filters if any
            if filter_clauses:
                query["query"]["bool"]["filter"] = filter_clauses
            
            # Log the query being sent
            import json
            logger.debug(f"Elasticsearch query: {json.dumps(query, ensure_ascii=False, indent=2)}")
            
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
            
            filters_info = []
            if user_id:
                filters_info.append(f"user_id={user_id}")
            if date_from or date_to:
                filters_info.append(f"date={date_from or '*'}-{date_to or '*'}")
            
            filter_str = f" with filters: {', '.join(filters_info)}" if filters_info else ""
            logger.info(f"Search for '{query_string}'{filter_str}: found {total_value} total matches, returning {len(response['hits']['hits'])} results")
            
            # Return full documents instead of just message_ids
            documents = [
                hit["_source"]
                for hit in response["hits"]["hits"]
            ]
            return documents
        except Exception as e:
            logger.error(f"Error searching: {e}", exc_info=True)
            return []
