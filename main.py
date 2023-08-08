from pymongo import MongoClient
from bson import ObjectId
from bson.son import SON

# client = MongoClient("localhost", 27017)  # creating an instance of MongoClient

new_client = MongoClient("mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000")

db = new_client.new_database  # creating a database called sample_database

people = db.people  # collection called people

databases = new_client.list_database_names()

# people.insert_one({"name": "Shane", "age": 20})
# people.insert_one({"name": "Kalana", "age": 36})

# if "new_database" in databases:
#     print("database exists")
# else:
#     print("no database in that name")

# db.dropDatabase()

# shane_id = people.insert_one({"name": "Shane", "age": 30}).inserted_id  # inserting values
# people.insert_one({"name": "Lisa", "age": 27, "interests": ["Java", "Python", "JavaScript"]})

# print(f"mikes id = {shane_id}")

# print([p for p in people.find({"name": "Shane"})])

# print([p for p in people.find({"_id": ObjectId("64d20eb99077259901d3778a")})]) # person with the given id

# print([p for p in people.find({"age": {"$gt": 30}})])  # $lt = less than, $gt = greater than

# print(people.count_documents({"age": {"$lt": 30}}))

# people.update_one({"_id": ObjectId("64d20eb99077259901d3778a")}, {"$set": {"age": 100}})
# people.delete_many({"age": {"$gt": 18}})

# iterating items in people collection
# for person in people.find():
#     print(person)

# this returns the average ages of specific person in the database
pipeline = [
    {
        "$group": {
            "_id": "$name",
            "averageAge": {"$avg": "$age"}
        }
    },
    {
        "$sort": SON([("averageAge", -1), ("_id", -1)])  # this sorts the result in specific order
    }
]

results = people.aggregate(pipeline)

# print(results)
#
for result in results:
    print(result)
