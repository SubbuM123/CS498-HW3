from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
import csv

# SOURCE: https://stackoverflow.com/questions/27416296/how-to-push-a-csv-data-to-mongodb-using-python


# Replace with your Atlas connection string
ATLAS_CONNECTION_STRING = "mongodb+srv://lab6user:lab6user2@cluster0.poyssvy.mongodb.net/?appName=Cluster0"


client = MongoClient(ATLAS_CONNECTION_STRING)
db = client.ev_db
collection = db.vehicles

with open("Electric_Vehicle_Population_Data.csv", "r", encoding="utf-8") as file:
        data = list(csv.DictReader("Electric_Vehicle_Population_Data.csv"))

        for i in range(0, len(data), 5000):
            batch = data[i:i+5000]
            collection.insert_many(batch)