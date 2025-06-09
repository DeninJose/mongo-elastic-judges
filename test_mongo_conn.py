from pymongo import MongoClient


MONGO_DATABASE="ll-judgement"
MONGO_COLLECTION="judges"

client = MongoClient(MONGO_URI)
db = client[MONGO_DATABASE]
collection = db[MONGO_COLLECTION]

# print(collection.find_one())

docs = collection.find({})

print("Total Docs in mongo = ", collection.count_documents({}))
