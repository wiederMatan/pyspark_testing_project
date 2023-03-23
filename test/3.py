import json
from kafka import KafkaConsumer
import pymongo

# Connect to the MongoDB database
client = pymongo.MongoClient("mongodb://localhost:27017/")

# Select the database and collection
db = client["treesize_db"]
col = db["T_INFO"]

# Configure the Kafka consumer
consumer = KafkaConsumer(
    'T_INFO',
    bootstrap_servers=["cnt7-naya-cdh63:9092"],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my_group',
    value_deserializer=lambda x: x.decode('utf-8')
)

# Consume messages from the Kafka topic and insert them into the MongoDB collection
for message in consumer:
    data = message.value
    json_str = data[0]  # extract JSON string from list
    doc_dict = json.loads(json_str)  # parse JSON string into a dictionary
    mydoc = {
        "disc_name": doc_dict["disc_name"],
        "now": doc_dict["now"],
        "computer_name": doc_dict["computer_name"],
        "disc_total": doc_dict["disc_total"],
        "disc_used": doc_dict["disc_used"],
        "disc_free": doc_dict["disc_free"],
        "disc_used_precent": doc_dict["disc_used_precent"],
        "disc_free_precent": doc_dict["disc_free_precent"],
        "disc_type": doc_dict["disc_type"]
    }
    x = col.insert_many(mydoc)
    print(x.inserted_id)