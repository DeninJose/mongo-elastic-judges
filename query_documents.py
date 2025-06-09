from elasticsearch import Elasticsearch

from config import ADMIN, CLOUD_ID, PASSWORD

es = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=(ADMIN, PASSWORD)  # Replace with your Cloud credentials
)


def query_index_by_field(index_name, field_name, field_value, num_entries=10):
    try:
        # Query the index by matching a specific field
        query = {
            "size": num_entries,
            "query": {
                "match": {
                    field_name: field_value
                }
            }
        }
        response = es.search(index=index_name, body=query)
        for doc in response['hits']['hits']:
            print(doc['_source'])
    except Exception as e:
        print(f"Error querying index '{index_name}' by field '{field_name}': {e}")


if __name__ == "__main__":

    # response = es.search(index=index_name, body={"query": {"match_all": {}}}, size=num_entries)
    query_index_by_field("doc_zeta", "document_url", "hamlet-sasi-v-state-of-kerala", 5)