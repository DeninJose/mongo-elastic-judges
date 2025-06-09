from elasticsearch import Elasticsearch

from config import ADMIN, CLOUD_ID, PASSWORD

es = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=(ADMIN, PASSWORD)  # Replace with your Cloud credentials
)

def add_document_to_index(index_name, doc_id, document):
    """
    Adds a document to the specified Elasticsearch index.

    :param es_host: Elasticsearch host URL (e.g., 'http://localhost:9200')
    :param index_name: Name of the index
    :param doc_id: Document ID (can be None for auto-generated)
    :param document: Dictionary representing the document
    :return: Elasticsearch response
    """
    response = es.index(index=index_name, id=doc_id, document=document)
    return response


def get_document_by_id(index_name, doc_id):
    """
    Retrieves a document from the specified Elasticsearch index by its ID.

    :param index_name: Name of the index
    :param doc_id: Document ID
    :return: Document if found, else None
    """
    try:
        response = es.get(index=index_name, id=doc_id)
        return response['_source']
    except Exception as e:
        print(f"Error retrieving document with id '{doc_id}' from index '{index_name}': {e}")
        return None


def delete_document_by_id(index_name, doc_id):
    """
    Deletes a document from the specified Elasticsearch index by its ID.

    :param index_name: Name of the index
    :param doc_id: Document ID
    :return: Elasticsearch response or None if not found
    """
    try:
        response = es.delete(index=index_name, id=doc_id)
        return response
    except Exception as e:
        print(f"Error deleting document with id '{doc_id}' from index '{index_name}': {e}")
        return None


if __name__ == "__main__":
    index = "document_drafts"
    doc_id = "1"
    document = {
        "automatic": {
            "appellant": "John Doe",
            "appellant_advocate": "Jane Smith",
            "case_name": "Doe v. Smith",
            "case_no": "12345",
            "case_type": "Civil",
            "cases_referred": ["Case A", "Case B"],
            "cases_referred_urls": ["http://example.com/case_a", "http://example.com/case_b"]
        }
    }
    add_document_to_index(index, doc_id, document)

    document = get_document_by_id(index, doc_id)
    print(document)

    delete_document_by_id(index, doc_id)


