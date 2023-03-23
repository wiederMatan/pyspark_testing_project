from kafka import KafkaConsumer
import pymongo

# Connect to the MongoDB database
client = pymongo.MongoClient("mongodb://localhost:27017/")

# Select the database and collection
db = client["treesize_db"]
col = db["T_INFO"]

# Configure the Kafka consumer
consumer = KafkaConsumer(
    'my_topic',
    bootstrap_servers=["cnt7-naya-cdh63:9092"],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my_group',
    value_deserializer=lambda x: x.decode('utf-8')
)

# Consume messages from the Kafka topic and insert them into the MongoDB collection
for message in consumer:
    data = message.value
    mydoc = {"name": data.get("name"), "address": data.get("address")}
    x = col.insert_one(mydoc)
    print(x.inserted_id)
