from src import configuration as c
from src import utils_fs as ut
from datetime import datetime
import time

while True:
    # ======== Read from folder files  ==================== #
    producer = ut.KafkaProducer(
        bootstrap_servers=c.bootstrap_servers,
        client_id='producer',
        acks=1,
        compression_type=None,
        retries=3)

    json_files = ut.get_all_files(c.source_file)

    list_file_info = []
    list_file_info_dir = []
    list_file_dir = []

    for file_name in json_files:
        if ut.re.search(c.filter_info, file_name):
            list_file_info.append(file_name)
        elif ut.re.search(c.filter_info_dir, file_name):
            list_file_info_dir.append(file_name)
        elif ut.re.search(c.filter_dir, file_name):
            list_file_dir.append(ut.os.path.abspath(file_name))


    #check if null
    if len(list_file_info) > 0:
        ut.send_file_topic(producer, list_file_info, c.topic_info)
    # if len(list_file_info_dir) > 0:
    #     ut.send_file_topic(producer, list_file_info_dir, c.topic_info_dir)
    # if len(list_file_dir) > 0:
    #     ut.send_file_topic(producer, list_file_dir, c.topic_dir)

    print(datetime.fromtimestamp(time.time()))
    time.sleep(10)

