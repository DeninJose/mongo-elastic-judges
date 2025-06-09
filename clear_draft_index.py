from elasticsearch import Elasticsearch
from config import ADMIN, CLOUD_ID, PASSWORD

es = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=(ADMIN, PASSWORD)  # Replace with your Cloud credentials
)

def clear_index(index_name):
    try:
        # Delete all documents from the index
        response = es.delete_by_query(index=index_name, body={"query": {"match_all": {}}})
        print(f"Cleared index '{index_name}'. Deleted documents: {response['deleted']}")
    except Exception as e:
        print(f"Error clearing index '{index_name}': {e}")

# Example usage
if __name__ == "__main__":
    index_name = "document_drafts"  # Replace with the name of the index to clear
    clear_index(index_name)
