class SODEquityIndexDataBatchConfig:
    upload_list = ['DJI@DJI', 'NAS@IXIC', 'SPI@SPX', 'STX@SX5E']

    asset_class = 'Equity'
    contract_type = 'Index'

    ticker_column_name = 'TICKER'
    batch_function_column_name = 'BATCH_FUNCTION'
    date_column_name = 'DATE'

    data_table = 'EQUITY_INDEX_DATA'

    data_table_columns = ['TICKER', 'DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']
    data_type_mapping_for_json_insert = {'TICKER': 'string',
                                         'DATE': 'string',
                                         'OPEN': 'string',
                                         'HIGH': 'string',
                                         'LOW': 'string',
                                         'CLOSE': 'string',
                                         'VOLUME': 'string'}


class EODEquityIndexDataBatchConfig:
    upload_list = ['HSI@HSCE', 'KOSDAQ', 'KOSPI', 'KPI200', 'NII@NI225']

    asset_class = 'Equity'
    contract_type = 'Index'

    ticker_column_name = 'TICKER'
    batch_function_column_name = 'BATCH_FUNCTION'
    date_column_name = 'DATE'

    data_table = 'EQUITY_INDEX_DATA'

    data_table_columns = ['TICKER', 'DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']
    data_type_mapping_for_json_insert = {'TICKER': 'string',
                                         'DATE': 'string',
                                         'OPEN': 'string',
                                         'HIGH': 'string',
                                         'LOW': 'string',
                                         'CLOSE': 'string',
                                         'VOLUME': 'string'}


class CryptoDataBatchConfig:
    asset_class = 'Crypto'
    contract_type = ['Spot', 'Futures']

    time_frame = '1m'
    timezone = 'UTC'

    ticker_column_name = 'TICKER'
    batch_function_column_name = 'BATCH_FUNCTION'
    date_column_name = 'DATE'
    timezone_column_name = 'TIMEZONE'

    data_table = 'CRYPTO_DATA'

    data_table_columns = ['TICKER', 'DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'TIMEZONE']
    data_type_mapping_for_json_insert = {'TICKER': 'string',
                                         'DATE': 'string',
                                         'OPEN': 'string',
                                         'HIGH': 'string',
                                         'LOW': 'string',
                                         'CLOSE': 'string',
                                         'VOLUME': 'string',
                                         'TIMEZONE': 'string'}


class KRXStockDataBatchConfig:
    num_cores = 12

    asset_class = 'Equity'
    contract_type = 'Stock'

    ticker_column_name = 'TICKER'
    batch_function_column_name = 'BATCH_FUNCTION'
    date_column_name = 'DATE'

    data_tickers_table = 'DATA_TICKER'
    data_table = 'KRX_STOCK_DATA'

    listed_stocks_info_table = 'KRX_LISTED_STOCKS_INFO'

    data_table_columns = ['TICKER', 'DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'MARKET']
    data_table_data_type_mapping_for_json_insert = {'TICKER': 'string',
                                                    'DATE': 'string',
                                                    'OPEN': 'string',
                                                    'HIGH': 'string',
                                                    'LOW': 'string',
                                                    'CLOSE': 'string',
                                                    'VOLUME': 'string',
                                                    'MARKET': 'string'}


class InsertDeleteKRXListedStockInfoBatchConfig:
    markets = ['KOSPI', 'KOSDAQ']

    listed_stocks_info_table = 'KRX_LISTED_STOCKS_INFO'
    deleted_stocks_info_table = 'DELETED_KRX_LISTED_STOCKS_INFO'

    market_column_name = 'MARKET'
    ticker_column_name = 'TICKER'
    delete_batch_date_column_name = 'DELETE_BATCH_DATE'

    listed_stocks_info_table_columns = ['TICKER', 'ISIN', 'NAME', 'MARKET', 'LISTED_SHARES']
    deleted_stocks_info_table_columns = ['TICKER', 'ISIN', 'NAME', 'MARKET', 'LISTED_SHARES', 'DELETE_BATCH_DATE']

    market_id_mapping = {'STK': 'KOSPI',
                         'KSQ': 'KOSDAQ'}

    listed_stocks_info_data_type_mapping = {'TICKER': 'string',
                                            'ISIN': 'string',
                                            'NAME': 'string',
                                            'MARKET': 'string',
                                            'LISTED_SHARES': 'string'}
    deleted_listed_stocks_data_type_mapping = {'TICKER': 'string',
                                               'ISIN': 'string',
                                               'NAME': 'string',
                                               'MARKET': 'string',
                                               'LISTED_SHARES': 'string',
                                               'DELETE_BATCH_DATE': 'string'}


class UpdateKRXListedStocksInfoBatchConfig:
    markets = ['KOSPI', 'KOSDAQ']

    listed_stocks_info_table = 'KRX_LISTED_STOCKS_INFO'
    data_tickers_table = 'DATA_TICKER'
    data_table = 'KRX_STOCK_DATA'
    backup_data_table = 'KRX_STOCK_DATA_BACK_UP'

    market_column_name = 'MARKET'
    ticker_column_name = 'TICKER'
    listed_shares_column_name = 'LISTED_SHARES'
    date_column_name = 'DATE'
    backup_date_column_name = 'BACKUP_DATE'
    batch_function_column_name = 'BATCH_FUNCTION'
    name_column_name = 'NAME'

    listed_stocks_info_table_columns = ['TICKER', 'ISIN', 'NAME', 'MARKET', 'LISTED_SHARES']
    data_tickers_table_columns = ['TICKER', 'NAME', 'ASSET_CLASS', 'CONTRACT_TYPE', 'SOURCE', 'BATCH_FUNCTION']
    data_table_columns = ['TICKER', 'DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'MARKET']
    backup_data_table_columns = ['TICKER', 'DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'MARKET', 'BACKUP_DATE']

    market_id_mapping = {'STK': 'KOSPI',
                         'KSQ': 'KOSDAQ'}

    decorating_apostrophe_mapping_for_update = {'TICKER': True,
                                                'ISIN': True,
                                                'NAME': True,
                                                'MARKET': True,
                                                'LISTED_SHARES': False,
                                                'ASSET_CLASS': True,
                                                'CONTRACT_TYPE': True,
                                                'SOURCE': True,
                                                'BATCH_FUNCTION': True}
    data_type_mapping_for_json_insert = {'TICKER': 'string',
                                         'DATE': 'string',
                                         'OPEN': 'string',
                                         'HIGH': 'string',
                                         'LOW': 'string',
                                         'CLOSE': 'string',
                                         'VOLUME': 'string',
                                         'MARKET': 'string'}
    backup_data_type_mapping_for_json_insert = {'TICKER': 'string',
                                                'DATE': 'string',
                                                'OPEN': 'string',
                                                'HIGH': 'string',
                                                'LOW': 'string',
                                                'CLOSE': 'string',
                                                'VOLUME': 'string',
                                                'MARKET': 'string',
                                                'BACKUP_DATE': 'string'}


class SynchronizeKRXStocksDataTickerBatchConfig:
    markets = ['KOSPI', 'KOSDAQ']

    asset_class = 'Equity'
    contract_type = 'Stock'

    market_column_name = 'MARKET'
    ticker_column_name = 'TICKER'
    name_column_name = 'NAME'

    listed_stocks_info_table = 'KRX_LISTED_STOCKS_INFO'
    data_tickers_table = 'DATA_TICKER'

    data_tickers_table_columns = ['TICKER', 'NAME', 'ASSET_CLASS', 'CONTRACT_TYPE', 'SOURCE', 'BATCH_FUNCTION']

    data_tickers_data_type_mapping = {'TICKER': 'string',
                                      'NAME': 'string',
                                      'ASSET_CLASS': 'string',
                                      'CONTRACT_TYPE': 'string',
                                      'SOURCE': 'string',
                                      'BATCH_FUNCTION': 'string'}

    data_tickers_value_mapping = {'ASSET_CLASS': 'Equity',
                                  'CONTRACT_TYPE': 'Stock',
                                  'SOURCE': 'KRX',
                                  'BATCH_FUNCTION': 'krx_data_crawling'}


class InsertDeleteKRXDelistedStockInfoBatchConfig:
    markets = ['KOSPI', 'KOSDAQ']
    secu_groups = ['주권', '외국주권']

    ticker_column_name = 'TICKER'
    market_column_name = 'MARKET'
    listed_date_column_name = 'LISTED_DATE'
    delisted_date_column_name = 'DELISTED_DATE'

    delisted_stocks_info_table = 'KRX_DELISTED_STOCKS_INFO'

    delisted_stocks_info_table_columns = ['TICKER', 'NAME', 'MARKET', 'SEC_GROUP',
                                          'TYPE', 'LISTED_DATE', 'DELISTED_DATE',
                                          'INDUSTRY', 'PAR_VALUE', 'LISTED_SHARES']

    delisted_stocks_info_table_data_type_mapping = {'TICKER': 'string',
                                                    'NAME': 'string',
                                                    'MARKET': 'string',
                                                    'SEC_GROUP': 'string',
                                                    'TYPE': 'string',
                                                    'LISTED_DATE': 'string',
                                                    'DELISTED_DATE': 'string',
                                                    'INDUSTRY': 'string',
                                                    'PAR_VALUE': 'string',
                                                    'LISTED_SHARES': 'string'}
