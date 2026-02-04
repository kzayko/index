"""HTTP API for searching messages."""
import logging
import json
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from elasticsearch_client import ElasticsearchClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Message Search API", version="1.0.0")
es_client = ElasticsearchClient()


@app.on_event("startup")
async def startup_event():
    """Check Elasticsearch connection on startup."""
    if not es_client.check_connection():
        logger.error("Cannot connect to Elasticsearch")
        raise RuntimeError("Elasticsearch connection failed")
    logger.info("API started successfully")


@app.post("/search")
async def search_messages(request: Request):
    """
    Search for messages by text query with optional filters.
    
    Request body format:
    {
        "query": "search text",           # Required: text search query
        "user_id": "user123",             # Optional: filter by exact user_id
        "date_from": "2024-01-01",        # Optional: start date (ISO 8601 format)
        "date_to": "2024-12-31"           # Optional: end date (ISO 8601 format)
    }
    
    Date format examples:
    - "2024-01-01" (date only)
    - "2024-01-01T00:00:00" (date with time)
    - "2024-01-01T00:00:00Z" (date with time and timezone)
    
    Args:
        request: HTTP request with JSON body containing query string and optional filters
        
    Returns:
        JSON response with full documents (user_id, chat_id, message_id, text, timestamp) and count
    """
    try:
        # Get raw body bytes
        body_bytes = await request.body()
        
        # Decode as UTF-8
        body_str = body_bytes.decode('utf-8')
        
        # Parse JSON
        data = json.loads(body_str)
        query = data.get('query', '').strip()
        
        if not query:
            raise HTTPException(status_code=400, detail="Query string cannot be empty")
        
        # Extract optional filters
        user_id = data.get('user_id')
        if user_id:
            user_id = str(user_id).strip()
            if not user_id:
                user_id = None
        
        date_from = data.get('date_from')
        if date_from:
            date_from = str(date_from).strip()
            if not date_from:
                date_from = None
        
        date_to = data.get('date_to')
        if date_to:
            date_to = str(date_to).strip()
            if not date_to:
                date_to = None
        
        # Fix double-encoded UTF-8 from Windows console
        # Windows console may use cp1251, so UTF-8 bytes get incorrectly decoded, then re-encoded as UTF-8
        # Pattern: original UTF-8 bytes -> wrong decode -> UTF-8 encode -> double encoding
        original_query = query
        query_bytes = query.encode('utf-8')
        
        # Check for double encoding pattern (contains \xc2 which is common in double-encoded UTF-8)
        if b'\xc2' in query_bytes:
            try:
                # Try cp1251 first (most common for Russian Windows)
                intermediate_bytes = query.encode('cp1251', errors='ignore')
                fixed_query = intermediate_bytes.decode('utf-8', errors='ignore')
                
                if fixed_query != query and len(fixed_query) > 0:
                    # Verify it's valid UTF-8
                    fixed_query.encode('utf-8')
                    query = fixed_query
                    logger.info(f"Fixed double-encoded query (cp1251): {repr(original_query)} -> {repr(query)}")
            except (UnicodeDecodeError, UnicodeEncodeError):
                # If cp1251 doesn't work, try cp1252
                try:
                    intermediate_bytes = query.encode('cp1252', errors='ignore')
                    fixed_query = intermediate_bytes.decode('utf-8', errors='ignore')
                    
                    if fixed_query != query and len(fixed_query) > 0:
                        fixed_query.encode('utf-8')
                        query = fixed_query
                        logger.info(f"Fixed double-encoded query (cp1252): {repr(original_query)} -> {repr(query)}")
                except (UnicodeDecodeError, UnicodeEncodeError):
                    pass
        
        # Log search parameters
        filters = []
        if user_id:
            filters.append(f"user_id={user_id}")
        if date_from:
            filters.append(f"date_from={date_from}")
        if date_to:
            filters.append(f"date_to={date_to}")
        
        filter_str = f" with filters: {', '.join(filters)}" if filters else ""
        logger.info(f"Search query: {repr(query)}{filter_str}")
        
        # Search in Elasticsearch with filters
        documents = es_client.search(
            query_string=query,
            user_id=user_id,
            date_from=date_from,
            date_to=date_to
        )
        
        return JSONResponse(content={
            "results": documents,
            "count": len(documents)
        })
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {str(e)}")
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Search error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    if es_client.check_connection():
        return {"status": "healthy", "elasticsearch": "connected"}
    else:
        return {"status": "unhealthy", "elasticsearch": "disconnected"}


if __name__ == "__main__":
    import uvicorn
    from config import APIConfig
    uvicorn.run(app, host=APIConfig.HOST, port=APIConfig.PORT)
