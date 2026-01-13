"""HTTP API for searching messages."""
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from elasticsearch_client import ElasticsearchClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Message Search API", version="1.0.0")
es_client = ElasticsearchClient()


class SearchRequest(BaseModel):
    """Search request model."""
    query: str


class SearchResponse(BaseModel):
    """Search response model."""
    message_ids: list[str]
    count: int


@app.on_event("startup")
async def startup_event():
    """Check Elasticsearch connection on startup."""
    if not es_client.check_connection():
        logger.error("Cannot connect to Elasticsearch")
        raise RuntimeError("Elasticsearch connection failed")


@app.post("/search", response_model=SearchResponse)
async def search_messages(request: SearchRequest):
    """
    Search for messages by text query.
    
    Args:
        request: Search request with query string
        
    Returns:
        List of message_id values matching the query
    """
    if not request.query or not request.query.strip():
        raise HTTPException(status_code=400, detail="Query string cannot be empty")
    
    try:
        message_ids = es_client.search(request.query.strip())
        return SearchResponse(
            message_ids=message_ids,
            count=len(message_ids)
        )
    except Exception as e:
        logger.error(f"Search error: {e}")
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
