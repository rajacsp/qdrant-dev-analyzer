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

def get_collection_documents(qdrant, collection_name):
    """Get documents for a specific collection."""
    # Initialize document tracking
    unique_documents = set()
    
    # Retrieve points with payload to analyze metadata
    offset = None
    batch_size = 100
    all_processed = False
    points_count = 0
    
    while not all_processed:
        # Get batch of points with payload
        points, next_offset = qdrant.scroll(
            collection_name=collection_name,
            limit=batch_size,
            offset=offset,
            with_payload=True,
            with_vectors=False
        )

        points_count = len(points)
        
        # Process each point's metadata
        for point in points:
            # Check for metadata in the payload
            if point.payload:
                # First check if there's a metadata field
                if 'metadata' in point.payload and isinstance(point.payload['metadata'], dict):
                    metadata = point.payload['metadata']
                    # Look for document name in metadata
                    if 'document_name' in metadata:
                        doc_name = metadata['document_name']
                        source = metadata['source']

                        doc_name_with_source = f"[{source}] : {doc_name}"
                        unique_documents.add(doc_name_with_source)

                # If no metadata or no document_name in metadata, try other fields
                # else:
                #     # Check for common document field names directly in payload
                #     doc_name = None
                #     doc_name_with_source = None
                #     for field in ['document_name', 'name', 'filename', 'file_name', 'doc_name', 'title', 'source']:
                #         if field in point.payload:
                #             doc_name = point.payload[field]
                #             source = point.payload['source']
                #             break
                    
                #     # If we found a document identifier, add it
                #     if doc_name:
                #         unique_documents.add(doc_name)
        
        # Update for next iteration
        if next_offset and points:
            offset = next_offset
        else:
            all_processed = True
    
    return unique_documents, points_count

def print_collection_documents(collection_name, unique_documents):
    """Print documents for a specific collection."""
    if unique_documents:
        print("\nUnique documents in collection:", collection_name)
        for i, doc in enumerate(unique_documents, 1):
            print(f"  {i}. {doc}")
        print()

def print_user_documents(qdrant, collections):
    """Print documents organized by user_id."""
    # Dictionary to organize documents by user_id
    user_documents = defaultdict(list)
    
    # Process collections to extract user_id and documents
    if collections.collections:
        for collection in collections.collections:
            collection_name = collection.name
            
            # Skip collections that don't have the user_id:collection_id format
            if ':' not in collection_name:
                continue
                
            # Extract user_id from group_id
            user_id = collection_name.split(':')[0]
            
            # Get documents for this collection
            unique_documents, points_count = get_collection_documents(qdrant, collection_name)
            
            # Store documents by user_id
            for doc in unique_documents:
                user_documents[user_id].append(doc)
    
    # Print documents organized by user_id
    print("\nUsers' documents:")
    
    # For demonstration, create a sample user_id if no collections with user_id:collection_id format are found
    if not user_documents and collections.collections and len(collections.collections) > 0:
        # Use the first collection's documents for demonstration
        first_collection = collections.collections[0]
        demo_user_id = "user123"
        
        # Get documents for the first collection
        unique_documents, points_count = get_collection_documents(qdrant, first_collection.name)
        
        if unique_documents:
            print(f"{demo_user_id}:")
            for i, doc in enumerate(unique_documents, 1):
                print(f"      {i}. {doc}")
            print()
    # If we have user documents from collections with user_id:collection_id format, print them
    elif user_documents:
        for user_id, documents in user_documents.items():
            print(f"{user_id}:")
            for i, doc in enumerate(documents, 1):
                print(f"      {i}. {doc}")
            print()

def process_collection(qdrant, collection):
    """Process and print information for a single collection."""
    # Extract username from collection name
    collection_name = collection.name
    username = collection_name.split('-')[0] if '-' in collection_name else 'unknown'
    
    # Get point count using collection info
    collection_info = qdrant.get_collection(collection_name=collection.name)
    point_count = collection_info.points_count if hasattr(collection_info, 'points_count') else 0
    
    # Get documents for this collection
    unique_documents, points_count = get_collection_documents(qdrant, collection.name)
    
    # If point_count is 0, use the length of unique_documents
    if point_count == 0:
        point_count = len(unique_documents)
    
    # Print results
    print(f"{username:<15} {collection_name:<30} {point_count:<20} {len(unique_documents):<10}")
    
    # If documents were found, print them
    print_collection_documents(collection_name, unique_documents)

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
            print(f"{'Username':<15} {'Collection Name':<30} {'Point Count':<20} {'Docs Count':<10}")
            print("-" * 80)
            
            # Process each collection
            for collection in collections.collections:
                process_collection(qdrant, collection)
            
            print("-" * 80)
            
            # Call the function to print user documents
            # print_user_documents(qdrant, collections)
        else:
            print("No collections found")
            
    except Exception as e:
        print(f"Error listing collections: {e}")

if __name__ == "__main__":
    list_qdrant_collections()
