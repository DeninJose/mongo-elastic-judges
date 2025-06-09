from elasticsearch import Elasticsearch, helpers


# Initialize Elasticsearch client
es = Elasticsearch(
    cloud_id="elastic-prod:YXAtc291dGgtMS5hd3MuZWxhc3RpYy1jbG91ZC5jb20kZTc5NTljYWI5ODI0NDM4OGJjNWZhYmI4ODFhYTU0YzUkZDU1ODNiYjk1NGI5NDJjY2E5ZjdmMTFjOTNhODUzNTI=",
    basic_auth=("alcy_admin","Alcy123@")  # Replace with your Cloud credentials
)


def ingest_into_elasticsearch():
    # Example function to ingest data into Elasticsearch
    # This is a placeholder and should be replaced with actual ingestion logic
    SOURCE_INDEX = "doc_zeta_sci_timely_0_dup_0"
    DEST_INDEX = "judges"
    scroll_size = 1000

    cases_with_no_judges = 0

    resp = es.search(index=SOURCE_INDEX, scroll='5m', size=scroll_size, body={"query": {"match_all": {}}})

    scroll_id = resp['_scroll_id']
    hits = resp['hits']['hits']

    while hits:
        actions = []

        for doc in hits:
            source = doc['_source']
            ids = source.get('judgement_bench', [])
            names = source.get('judgement_bench_name', [])

            # Ensure both ids and names are not None and are iterable
            if ids and names:
                for i, n in zip(ids, names):
                    actions.append({
                        "_index": DEST_INDEX,
                        "_id": str(i),
                        "_source": {
                            "name": n
                        }
                    })
            else:
                cases_with_no_judges += 1
                doc_url = source.get('document_url')
                doc_date = source.get('document_date')
                print(f"Skipping document with missing 'judgement_bench' or 'judgement_bench_name': {doc_url} {doc_date}")

        # Bulk index to new index
        if actions:
            helpers.bulk(es, actions)

        # Get next batch
        resp = es.scroll(scroll_id=scroll_id, scroll='2m')
        scroll_id = resp['_scroll_id']
        hits = resp['hits']['hits']

    es.clear_scroll(scroll_id=scroll_id)
    print("Done.")
    print(f"Total documents with no judges: {cases_with_no_judges}")

if __name__ == "__main__":
    ingest_into_elasticsearch()