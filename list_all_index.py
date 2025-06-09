from elasticsearch import Elasticsearch

from config import ADMIN, CLOUD_ID, PASSWORD

es = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=(ADMIN, PASSWORD)  # Replace with your Cloud credentials
)

def list_all_indexes():
    try:
        # Fetch all indexes
        indexes = es.cat.indices(format="json")
        for index in indexes:
            print(index['index'])
    except Exception as e:
        print(f"Error fetching indexes: {e}")


if __name__ == "__main__":
    # Example usage
    list_all_indexes()