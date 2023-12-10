from IndividualBatchConfig import *


class SODBatchConfig:
    batch_name = 'SOD_BATCH'
    batch_functions = ['do_equity_index_data_batch',
                       'do_crypto_data_batch']
    batch_function_flags = {'do_equity_index_data_batch': 'On',
                            'do_crypto_data_batch': 'On'}
    batch_function_config_mapping = {'do_equity_index_data_batch': SODEquityIndexDataBatchConfig(),
                                     'do_crypto_data_batch': CryptoDataBatchConfig()}


class EODBatchConfig:
    batch_name = 'EOD_BATCH'
    batch_functions = ['do_insert_and_delete_krx_listed_stocks_info_batch',
                       'do_synchronize_krx_stocks_data_ticker_batch',
                       'do_insert_and_delete_krx_delisted_stocks_info_batch',
                       'do_equity_index_data_batch',
                       'do_krx_stock_data_batch',
                       'do_update_krx_listed_stocks_info_batch']
    batch_function_flags = {'do_insert_and_delete_krx_listed_stocks_info_batch': 'On',
                            'do_synchronize_krx_stocks_data_ticker_batch': 'On',
                            'do_insert_and_delete_krx_delisted_stocks_info_batch': 'On',
                            'do_equity_index_data_batch': 'On',
                            'do_krx_stock_data_batch': 'On',
                            'do_update_krx_listed_stocks_info_batch': 'On'}
    batch_function_config_mapping = {'do_insert_and_delete_krx_listed_stocks_info_batch':
                                         InsertDeleteKRXListedStockInfoBatchConfig(),
                                     'do_synchronize_krx_stocks_data_ticker_batch':
                                         SynchronizeKRXStocksDataTickerBatchConfig(),
                                     'do_insert_and_delete_krx_delisted_stocks_info_batch':
                                         InsertDeleteKRXDelistedStockInfoBatchConfig(),
                                     'do_equity_index_data_batch':
                                         EODEquityIndexDataBatchConfig(),
                                     'do_krx_stock_data_batch':
                                         KRXStockDataBatchConfig(),
                                     'do_update_krx_listed_stocks_info_batch':
                                         UpdateKRXListedStocksInfoBatchConfig()}
