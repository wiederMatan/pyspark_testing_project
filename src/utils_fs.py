import re
import os
import json
from kafka import KafkaConsumer
from time import sleep
import pathlib

from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql import types as t
from pyspark.sql.functions import *

from kafka import KafkaConsumer
from kafka import KafkaProducer


def get_all_files(source_file):
    file_names = next(os.walk(source_file), (None, None, []))[2]
    file_names_abs = [source_file + "/" + file_name for file_name in file_names]
    return file_names_abs


def send_file_topic(producer, source_file, topic_dest):
    for x in source_file:
        with open(x, 'r') as file:
            lines = file.readlines()  # returns list of strings
            producer.send(topic=topic_dest, value=json.dumps(lines).encode('utf-8'))
            producer.flush()
            print(lines)
            sleep(15)


def send_data_mongo(consumer, db_collection):
    for message in consumer:
        data = message.value
        json_str = data.strip('[').strip(']').strip('"').replace('\\', '')
        doc_dict = json.loads(json_str)
        x = db_collection.insert_one(doc_dict[0])
        print(f'{x.inserted_id} | {db_collection}')
        sleep(5)
