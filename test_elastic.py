from elasticsearch import Elasticsearch, helpers


# Initialize Elasticsearch client
es = Elasticsearch(
# Replace with your Cloud credentials
)

def list_all_indexes():
    try:
        # Fetch all indexes
        indexes = es.cat.indices(format="json")
        for index in indexes:
            print(index['index'])
    except Exception as e:
        print(f"Error fetching indexes: {e}")

def print_index_contents(index_name, num_entries=10):
    try:
        # Fetch the total number of documents in the index
        response = es.count(index=index_name)
        print(f"Total documents in index '{index_name}': {response['count']}")
        response = es.search(index=index_name, body={"query": {"match_all": {}}}, size=num_entries)
        for doc in response['hits']['hits']:
            print(doc['_source'])
            # print(doc['_source']['judgement_bench'])
            # print(doc['_source']['judgement_bench_name'])


    except Exception as e:
        print(f"Error fetching contents of index '{index_name}': {e}")

def print_index_mappings(index_name):
    try:
        # Fetch mappings of the index
        mappings = es.indices.get_mapping(index=index_name)
        print(mappings[index_name]['mappings'])
    except Exception as e:
        print(f"Error fetching mappings of index '{index_name}': {e}")

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

def query_index_by_field(index_name, field_name, field_value, num_entries=10):
    try:
        # Query the index by matching a specific field
        query = {
            "query": {
                "match": {
                    field_name: field_value
                }
            }
        }
        response = es.search(index=index_name, body=query, size=num_entries)
        for doc in response['hits']['hits']:
            print(doc['_source']['document_url'])
            print(doc['_source']['document_date'])
            print(doc['_source']['judgement_bench'])
            print(doc['_source']['judgement_bench_name'])
    except Exception as e:
        print(f"Error querying index '{index_name}' by field '{field_name}': {e}")

def get_aggregate_judges():
    try:
        # Query the index to fetch all documents
        response = es.search(
            index="doc_zeta_sci_timely_0_dup_0",  # Replace with your index name
            body={
                "query": {"match_all": {}},
                "size": 10000  # Adjust size based on the number of documents
            }
        )

        # Initialize the dictionary to store results
        aggregated_results = {}

        # Process each document in the response
        for doc in response['hits']['hits']:
            source = doc['_source']
            document_url = source.get('document_url')
            judgement_bench = source.get('judgement_bench', [])

            # Ensure document_url exists and is valid
            if document_url:
                if document_url not in aggregated_results:
                    aggregated_results[document_url] = []
                # Ensure judgement_bench is a list before extending
                if isinstance(judgement_bench, list):
                    aggregated_results[document_url].extend(judgement_bench)

        # Print the aggregated results
        zero_benches = 0
        empty_urls = []

        print("Aggregated Judges by Document URL:")
        for url, benches in aggregated_results.items():
            if len(benches) == 0:
                zero_benches += 1
                empty_urls.append(url)
                print(f"Document URL: {url}, Judgement Benches: {benches}")

        # Write empty URLs to a file
        with open("empty_judgement_bench_urls.txt", "w") as file:
            for url in empty_urls:
                file.write(url + "\n")

        print(f"Total documents with no judges: {zero_benches}")
        return aggregated_results

    except Exception as e:
        print(f"Error in get_aggregate_judges: {e}")
        return {}

# Example usage
if __name__ == "__main__":
    # list_all_indexes()
    # index_name = "doc_zeta_sci_timely_0_dup_0"
    # index_name = "judges"
    # Replace with the name of your index
    # num_entries = 1  # Specify the number of entries to fetch
    # print_index_contents("judges", 3)
    # print_index_contents("doc_zeta", 3)
    # ingest_into_elasticsearch()
    # print_index_mappings("judges")

    # response = es.search(index=index_name, body={"query": {"match_all": {}}}, size=num_entries)
    get_aggregate_judges()


