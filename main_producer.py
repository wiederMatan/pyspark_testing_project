from src import configuration as co
from src import utils_fs as ut
from src import my_logger as lg
from os import listdir
from os.path import isfile, join
import shutil
import time


sw_debug = 1
PROJECT_PATH = lg.Path(__file__).absolute().parent.parent
path_log = f'{PROJECT_PATH}/logs/log_producer.log'
l = lg.my_log(path_log, sw_debug)

"""Initialize Producer"""
producer = ut.KafkaProducer(
    bootstrap_servers=co.bootstrap_servers,
    client_id='producer',
    acks=1,
    compression_type=None,
    retries=3)

while True:

    # ======== Read from folder files  ==================== #
    json_files = [co.source_file + "/" + f for f in listdir(co.source_file) if isfile(join(co.source_file, f))]
    l.log(f"INFO", f"PRODUCER: count files =>{len(json_files)}")

    if len(json_files) > 0:

        for file_name in json_files:
            l.log(f"INFO", f"PRODUCER(1): {file_name}")

            if co.filter_info in file_name:
                ut.send_file_topic(producer, file_name, co.topic_info)
                shutil.move(file_name, co.archive_files)
                json_files.remove(file_name)

            elif co.filter_info_dir in file_name:
                ut.send_file_topic(producer, file_name, co.topic_info_dir)
                shutil.move(file_name, co.archive_files)
                json_files.remove(file_name)
            elif co.filter_dir in file_name:
                ut.send_file_topic(producer, file_name, co.topic_dir)
                shutil.move(file_name, co.archive_files)
                json_files.remove(file_name)

    else:
        time.sleep(120)

        # print(datetime.fromtimestamp(time.time()))




