import DataBatch
from datetime import datetime
from dateutil.relativedelta import relativedelta
import logging
import Logger
from BatchConfig import *
import sys

today = datetime.today()

if datetime.now().hour < 12:
    batch_config = SODBatchConfig()
    one_day_shift = relativedelta(days=1)
    batch_date = today - one_day_shift
else:
    batch_config = EODBatchConfig()
    batch_date = today

batch_config_name = type(batch_config).__name__
batch_date_str = batch_date.strftime('%Y-%m-%d')

batch_handler = DataBatch.BatchHandler(batch_config)

if today.weekday() in (0, 6) and batch_config_name == 'SODBatchConfig':
    batch_handler.set_batch_function_flag('do_equity_index_data_batch', 'Off')

if today.weekday() in (5, 6) and batch_config_name == 'EODBatchConfig':
    sys.exit()

# batch_handler.set_batch_function_flag('do_equity_index_data_batch', 'Off')
# batch_handler.set_batch_function_flag('do_crypto_data_batch', 'Off')
# batch_handler.set_batch_function_flag('do_krx_stock_data_batch', 'Off')
# batch_handler.set_batch_function_flag('do_insert_and_delete_krx_listed_stocks_info_batch', 'Off')
# batch_handler.set_batch_function_flag('do_synchronize_krx_stocks_data_ticker_batch', 'Off')
# batch_handler.set_batch_function_flag('do_insert_and_delete_krx_delisted_stocks_info_batch', 'Off')
# batch_handler.set_batch_function_flag('do_update_krx_listed_stocks_info_batch', 'Off')
# batch_handler.set_batch_status_insert_on(False)

if __name__ == '__main__':
    logger = logging.getLogger()
    Logger.initialize_logger(logger)
    batch_handler.set_logger(logger)

    logger.info('Run at ' + str(datetime.now()))
    logger.info('Batch for ' + batch_date_str)
    try:
        batch_handler.run_entire_batch(batch_date_str)
    except Exception as e:
        logger.error(e)
