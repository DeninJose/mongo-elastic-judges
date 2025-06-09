from elasticsearch import Elasticsearch

from config import ADMIN, CLOUD_ID, PASSWORD

es = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=(ADMIN, PASSWORD)  # Replace with your Cloud credentials
)


def get_any_document(index_name):
    try:
        query = {
            "size": 1,
            "query": {
                "match_all": {}
            }
        }
        response = es.search(index=index_name, body=query)
        hits = response['hits']['hits']
        if hits:
            for hit in hits:
                print(hit['_source'])
                print()
        else:
            print(f"No documents found in index '{index_name}'")
            return None
    except Exception as e:
        print(f"Error retrieving a document from index '{index_name}': {e}")
        return None


if __name__ == "__main__":

    # response = es.search(index=index_name, body={"query": {"match_all": {}}}, size=num_entries)
    get_any_document("doc_zeta")