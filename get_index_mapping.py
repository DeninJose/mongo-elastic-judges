from elasticsearch import Elasticsearch

from config import ADMIN, CLOUD_ID, PASSWORD

es = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=(ADMIN, PASSWORD)  # Replace with your Cloud credentials
)

def get_index_mapping(index_name):
    try:
        mapping = es.indices.get_mapping(index=index_name)
        print(mapping[index_name]['mappings'])
    except Exception as e:
        print(f"Error getting mapping for index '{index_name}': {e}")

if __name__ == "__main__":

    # response = es.search(index=index_name, body={"query": {"match_all": {}}}, size=num_entries)
    get_index_mapping("document_drafts")
