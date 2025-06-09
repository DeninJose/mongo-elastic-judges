import re

from elasticsearch import Elasticsearch, helpers


# Initialize Elasticsearch client
es = Elasticsearch(
)



def print_index_contents(index_name):
    try:
        # Fetch the total number of documents in the index
        response = es.count(index=index_name)
        # print(f"Total documents in index '{index_name}': {response['count']}")
        # response = es.search(index=index_name, body={
        #     "size": num_entries,
        #     "query": {
        #         "match_all": {}
        #     }
        # })
        # for doc in response['hits']['hits']:
        #     print(doc['_source']['document_text'].length())
        #     # print(doc['_source']['judgement_bench'])
        #     # print(doc['_source']['judgement_bench_name'])


        # Define scroll duration and batch size
        scroll_duration = "2m"  # Adjust as needed
        batch_size = 100

        total_words = 0
        total_docs = 0

        # Initial search with scroll
        response = es.search(
            index=index_name,
            scroll=scroll_duration,
            size=batch_size,
            body={
                "_source": ["document_text"],
                "query": {"match_all": {}}
            }
        )

        scroll_id = response['_scroll_id']
        hits = response['hits']['hits']

        while hits:
            print(f"Total documents scanned: {total_docs}")
            for doc in hits:
                text = doc['_source'].get('document_text', '')
                word_count = len(re.findall(r'\S+', text))
                total_words += word_count
                total_docs += 1

            # Fetch the next batch
            response = es.scroll(
                scroll_id=scroll_id,
                scroll=scroll_duration
            )
            scroll_id = response['_scroll_id']
            hits = response['hits']['hits']

        # Clean up the scroll context
        es.clear_scroll(scroll_id=scroll_id)

        # Output the result
        print(f"Total documents scanned: {total_docs}")
        print(f"Total words in 'document_text': {total_words}")


    except Exception as e:
        print(f"Error fetching contents of index '{index_name}': {e}")

if __name__ == "__main__":
    index_name = "doc_zeta"  # Replace with the name of the index to clear
    print_index_contents(index_name)
