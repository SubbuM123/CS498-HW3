from flask import Flask, request, jsonify
from pymongo import MongoClient, WriteConcern, ReadPreference
from pymongo.errors import ConnectionFailure, OperationFailure

# Replace with your Atlas connection string
ATLAS_CONNECTION_STRING = "mongodb+srv://lab6user:lab6user2@cluster0.poyssvy.mongodb.net/?appName=Cluster0"

client = MongoClient(ATLAS_CONNECTION_STRING)
db = client.ev_db
collection = db.vehicles

app = Flask(__name__)
print("Starting")

# 1. Fast but Unsafe Write
# Endpoint: POST /insert-fast
# Payload: A JSON object representing a new EV record.
# Requirement: Insert the record using a Write Concern that acknowledges the write from the PRIMARY node only (Faster, but risks data loss if the primary crashes). Return the inserted document ID as a string.
# 2. Highly Durable Write
# Endpoint: POST /insert-safe
# Payload: A JSON object representing a new EV record.
# Requirement: Insert the record using a Write Concern that ensures the data is written to the majority of replica set members before acknowledging (Slower, but highly durable). Return the inserted document ID as a string.
# 3. Strongly Consistent Read
# Endpoint: GET /count-tesla-primary
# Requirement: Return a JSON object with the format {"count": <total_count>}, while <total_count> is count of Tesla vehicles. You must configure the Read Preference to guarantee strong consistency (strictly reading from the primary node).
# 4. Eventually Consistent Analytical Read
# Endpoint: GET /count-bmw-secondary
# Requirement: Return a JSON object with the format {"count": <total_count>}, while <total_count> is count of BMW vehicles. You must configure the Read Preference to offload this read to a secondary node, demonstrating high availability and eventual consistency.


@app.route("/insert-fast", methods=["POST"])
def insert_fast():
    col = collection.with_options(write_concern=WriteConcern(w=1))
    insert = col.insert_one(request.get_json())
    return str(insert.inserted_id)

@app.route("/insert-safe", methods=["POST"])
def insert_safe():
    col = collection.with_options(write_concern=WriteConcern(w="majority"))
    insert = col.insert_one(request.get_json())

    return str(insert.inserted_id)

@app.route("/count-tesla-primary", methods=["GET"])
def count_tesla_primary():
    col = collection.with_options(read_preference=ReadPreference.PRIMARY)

    return jsonify({"count": col.count_documents({"Make": "TESLA"})})

@app.route("/count-bmw-secondary", methods=["GET"])
def count_bmw_secondary():
    col = collection.with_options(read_preference=ReadPreference.SECONDARY_PREFERRED)

    return jsonify({"count": col.count_documents({"Make": "BMW"})})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)