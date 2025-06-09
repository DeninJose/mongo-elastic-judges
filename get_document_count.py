from elasticsearch import Elasticsearch


from config import ADMIN, CLOUD_ID, PASSWORD

es = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=(ADMIN, PASSWORD)  # Replace with your Cloud credentials
)


if __name__ == "__main__":

    # response = es.search(index=index_name, body={"query": {"match_all": {}}}, size=num_entries)
    print(es.count(index="document_drafts"))