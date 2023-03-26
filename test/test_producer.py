from src import configuration as co
from src import utils_fs as ut
from datetime import datetime
import time

while True:
    producer = ut.KafkaProducer(
        bootstrap_servers=co.bootstrap_servers,
        client_id='producer',
        acks=1,
        compression_type=None,
        retries=3)
    #
    # # ======== Read from folder files  ==================== #
    # json_files = ut.get_all_files(co.source_file)
    # print(json_files)
    #
    # list_file_info = []
    # list_file_info_dir = []
    # list_file_dir = []
    #
    # for file_name in json_files:
    #     if ut.re.search(co.filter_info, file_name):
    #         list_file_info.append(file_name)
    #     elif ut.re.search(co.filter_info_dir, file_name):
    #         list_file_info_dir.append(file_name)
    #     elif ut.re.search(co.filter_dir, file_name):
    #         list_file_dir.append(ut.os.path.abspath(file_name))
    #
    #         #==============send to consumer==========================#
    #     #check if null
    # if len(list_file_info) > 0:
    #     ut.send_file_topic(producer, list_file_info, co.topic_info)
    # if len(list_file_info_dir) > 0:
    #     l.log(f"INFO", f"PRODUCER: group INFO_DIR count={len(list_file_info_dir)} send to topic {co.topic_info_dir}")
    #     ut.send_file_topic(producer, list_file_info_dir, co.topic_info_dir)
    # if len(list_file_dir) > 0:
    #     l.log(f"INFO", f"PRODUCER: group DIR count={len(list_file_dir)} send to topic {co.topic_dir}")
    #     ut.send_file_topic(producer, list_file_dir, co.topic_dir)
    #
    # #print(datetime.fromtimestamp(time.time()))
    # time.sleep(10)

