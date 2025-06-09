from pymongo import MongoClient
from elasticsearch import Elasticsearch
import time


MONGO_DATABASE="ll-judgement"
MONGO_COLLECTION="judges"

client = MongoClient(MONGO_URI)
db = client[MONGO_DATABASE]
collection = db[MONGO_COLLECTION]

# print(collection.find_one())

docs = collection.find({})

print("Total Docs in mongo = ", collection.count_documents({}))



es = Elasticsearch("http://localhost:9200")

index_name="judges"

if es.indices.exists(index=index_name):
    es.indices.delete(index=index_name)
es.indices.create(index=index_name)



count = 0
for doc in docs:
    elastic_doc = {
        "name": doc["name"],
    }
    es.index(index=index_name, document=elastic_doc, id=doc["_id"])
    count+= 1

print("Docs in mongo = ", count)

time.sleep(5)

# 3. Retrieve the document
retrieved = es.search(index=index_name, body={"query": {"match_all": {}}})

print("Retrieved documents:")
print(retrieved)

# hits = retrieved.get('hits').get('hits')
# print(len(hits))
# for hit in hits:
#     print(hit.get('_source'))

# es.delete_by_query(index=index_name, query={"match_all": {}})
