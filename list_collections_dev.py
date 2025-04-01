import os
from datetime import datetime
from qdrant_client import QdrantClient
from qdrant_client.http.models import ScrollRequest
from collections import defaultdict
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Environment Configuration
ENV_NAME = os.getenv('ENV_NAME')
if not ENV_NAME:
    raise ValueError("ENV_NAME environment variable is required")

# Qdrant Configuration
QDRANT_URL = os.getenv('QDRANT_URL')
if not QDRANT_URL:
    raise ValueError("QDRANT_URL environment variable is required")

QDRANT_API_KEY = os.getenv('QDRANT_API_KEY')
if not QDRANT_API_KEY:
    raise ValueError("QDRANT_API_KEY environment variable is required")

TIMEOUT = int(os.getenv('TIMEOUT', '300'))

def get_qclient():
    """Get Qdrant client instance."""
    try:
        # Try HTTPS first
        qdrant_client = QdrantClient(
            url=QDRANT_URL,
            api_key=QDRANT_API_KEY,
            timeout=TIMEOUT,
            prefer_grpc=False
        )
        # Test connection
        qdrant_client.get_collections()
        return qdrant_client
    except Exception as e:
        print(f"Failed to connect using original URL: {e}")
        
        # Try with modified URL (http if https, or vice versa)
        try:
            modified_url = QDRANT_URL.replace('http://', 'https://') if 'http://' in QDRANT_URL else QDRANT_URL.replace('https://', 'http://')
            print(f"Attempting connection with modified URL: {modified_url}")
            
            qdrant_client = QdrantClient(
                url=modified_url,
                api_key=QDRANT_API_KEY,
                timeout=TIMEOUT,
                prefer_grpc=False
            )
            # Test connection
            qdrant_client.get_collections()
            return qdrant_client
        except Exception as e2:
            raise ConnectionError(f"Failed to connect to Qdrant server. Please check your URL and API key.\nOriginal error: {e}\nModified URL error: {e2}")

def list_qdrant_collections():
    """List all collections in Qdrant and analyze document metadata."""
    try:
        # Initialize Qdrant client with connection retry
        qdrant = get_qclient()
        
        # Get collections (connection already tested in get_qclient)
        collections = qdrant.get_collections()
        
        print(f"Collections in {ENV_NAME} environment:")
        print("-" * 80)
        
        if collections.collections:
            print(f"{'Username':<15} {'Collection Name':<30} {'Point Count':<10} {'Unique Docs':<10}")
            print("-" * 80)
            
            for collection in collections.collections:
                # Extract username from collection name
                collection_name = collection.name
                username = collection_name.split('-')[0] if '-' in collection_name else 'unknown'
                
                # Get point count using collection info
                collection_info = qdrant.get_collection(collection_name=collection.name)
                point_count = collection_info.points_count if hasattr(collection_info, 'points_count') else 0
                
                # Initialize document tracking
                unique_documents = set()
                
                # Retrieve points with payload to analyze metadata
                offset = None
                batch_size = 100
                all_processed = False
                
                while not all_processed:
                    # Get batch of points with payload
                    points, next_offset = qdrant.scroll(
                        collection_name=collection.name,
                        limit=batch_size,
                        offset=offset,
                        with_payload=True,
                        with_vectors=False
                    )
                    
                    # Process each point's metadata
                    for point in points:
                        # Check for metadata in the payload
                        if point.payload:
                            # First check if there's a metadata field
                            if 'metadata' in point.payload and isinstance(point.payload['metadata'], dict):
                                metadata = point.payload['metadata']
                                # Look for document name in metadata
                                if 'document_name' in metadata:
                                    unique_documents.add(metadata['document_name'])
                            # If no metadata or no document_name in metadata, try other fields
                            else:
                                # Check for common document field names directly in payload
                                doc_name = None
                                for field in ['document_name', 'name', 'filename', 'file_name', 'doc_name', 'title', 'source']:
                                    if field in point.payload:
                                        doc_name = point.payload[field]
                                        break
                                
                                # If we found a document identifier, add it
                                if doc_name:
                                    unique_documents.add(doc_name)
                    
                    # Update for next iteration
                    if next_offset and points:
                        offset = next_offset
                    else:
                        all_processed = True
                
                # If no points were found with scroll, update point_count
                if point_count == 0:
                    point_count = len(points) if points else 0
                
                # Print results
                print(f"{username:<15} {collection_name:<30} {point_count:<10} {len(unique_documents):<10}")
                
                # If unique documents were found, print them
                if unique_documents:
                    print("\nUnique documents in collection:", collection_name)
                    for i, doc in enumerate(sorted(unique_documents), 1):
                        print(f"  {i}. {doc}")
                    print()
            
            print("-" * 80)
        else:
            print("No collections found")
            
    except Exception as e:
        print(f"Error listing collections: {e}")

if __name__ == "__main__":
    list_qdrant_collections()
