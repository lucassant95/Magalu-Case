import logging

logging.basicConfig(format='[%(asctime)s - %(processName)s - %(threadName)s] - %(levelname)s: %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)
logger = logging.getLogger()


def get_logger():
    global logger
    return logger
