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

def create_draft_index_with_docs_mapping(docs_index, draft_index, fields):
    try:
        # Fetch the mapping of the docs index
        docs_mapping = es.indices.get_mapping(index=docs_index)
        docs_properties = docs_mapping[docs_index]['mappings']['properties']

        # Each field in the draft index will have the same mapping as docs index
        draft_properties = {field: {"properties": docs_properties} for field in fields}

        draft_mapping = {
            "mappings": {
                "properties": draft_properties
            }
        }

        es.indices.create(index=draft_index, body=draft_mapping)
        print(f"Draft index '{draft_index}' created with fields: {fields}")
    except Exception as e:
        print(f"Error creating draft index '{draft_index}': {e}")


if __name__ == "__main__":

    # response = es.search(index=index_name, body={"query": {"match_all": {}}}, size=num_entries)
    get_index_mapping("document_drafts")
    # create_custom_index("my_new_index")
    # Example usage:
    # This will create a draft index with three fields: 'draft1', 'draft2', 'draft3'
    # create_draft_index_with_docs_mapping("doc_zeta_sci_timely_0_dup_0", "document_drafts", ["automatic", "manual", "ocr"])

# Example mapping structure for reference:
mapping = {
    'properties': {
        'appellant': {'type': 'text'},
        'appellant_advocate': {'type': 'text'},
        'authored_by': {'type': 'text'},
        'case_name': {'type': 'text'},
        'case_no': {'type': 'text'},
        'case_type': {'type': 'keyword'},
        'cases_referred': {'type': 'text'},
        'cases_referred_urls': {
            'type': 'text',
            'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}
        },
        'court': {'type': 'keyword'},
        'document_date': {'type': 'date'},
        'document_keywords': {'type': 'keyword'},
        'document_summary': {'type': 'text'},
        'document_text': {'type': 'text'},
        'document_title': {'type': 'keyword'},
        'document_type': {'type': 'keyword'},
        'document_url': {'type': 'keyword'},
        'judgement_acts': {'type': 'text'},
        'judgement_bench': {'type': 'text'},
        'judgement_bench_name': {'type': 'text'},
        'judgement_citations': {'type': 'text'},
        'judgement_coram': {'type': 'integer'},
        'judgement_orders': {'type': 'text'},
        'judgement_outcome': {'type': 'text'},
        'judgement_precedents': {'type': 'text'},
        'judgement_status': {'type': 'text'},
        'other_citations': {'type': 'text'},
        'referred_by': {'type': 'text'},
        'referred_by_urls': {
            'type': 'text',
            'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}
        },
        'respondent': {'type': 'text'},
        'respondent_advocate': {'type': 'text'},
        'search_term': {
            'type': 'text',
            'fields': {'keyword': {'type': 'keyword', 'ignore_above': 256}}
        }
    }
}

# Example draft index mapping with three fields, each having the docs mapping:
draft_index_mapping = {
    "mappings": {
        "properties": {
            "automatic": {
                "properties": {
                    # ... all field mappings from docs index ...
                }
            },
            "manual": {
                "properties": {
                    # ... all field mappings from docs index ...
                }
            },
            "ocr": {
                "properties": {
                    # ... all field mappings from docs index ...
                }
            }
        }
    }
}

# The actual mapping (as you posted) would be:
# {'properties': {'automatic': {'properties': {...}}, 'manual': {'properties': {...}}, 'ocr': {'properties': {...}}}}