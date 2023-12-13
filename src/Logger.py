import logging
import sys
from datetime import datetime


def initialize_logger(logger, file_drop=True):
    logger = logging.getLogger('data_batch_log')
    logger.info('Batch start')
    logger.propagate = False

    now = datetime.now()
    if file_drop:
        file_name = '../log/' + str(now).replace(':', '_') + '_log.txt'
        logging.basicConfig(filename=file_name, filemode='w', level=logging.INFO)

    formatter = logging.Formatter('%(asctime)s|%(name)s|%(levelname)s:%(message)s')
    stream_handler = logging.StreamHandler()
    if file_drop:
        file_handler = logging.FileHandler(file_name)

    stream_handler.setFormatter(formatter)
    if file_drop:
        file_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)
    if file_drop:
        logger.addHandler(file_handler)

    if sys.gettrace() is not None:
        logger.setLevel(logging.DEBUG)
        stream_handler.setLevel(logging.DEBUG)
        if file_drop:
            file_handler.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
        stream_handler.setLevel(logging.INFO)
        if file_drop:
            file_handler.setLevel(logging.INFO)
