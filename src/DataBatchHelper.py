import pandas as pd
from copy import deepcopy
import NaverFinance as Nf
import KRX


# Query helper functions
def select_query_from_data_ticker_table(asset_class, contract_type):
    return "SELECT * FROM DATA_TICKER " \
           "WHERE ASSET_CLASS = '{asset_class}' " \
           "AND CONTRACT_TYPE = '{contract_type}'".format(asset_class=asset_class, contract_type=contract_type)


def data_all_select_query(table_name):
    select_query = 'SELECT * FROM {table_name}'.format(table_name=table_name)

    return select_query


def data_all_delete_query(table_name):
    delete_query = 'DELETE FROM {table_name}'.format(table_name=table_name)

    return delete_query


def price_data_delete_query(table_name, ticker, start_date, end_date):
    delete_query = 'DELETE FROM {table_name} WHERE '.format(table_name=table_name)
    delete_query += 'TICKER = ' + decorate_str_with_apostrophe(ticker) + ' AND '
    delete_query += 'DATE >= ' + decorate_str_with_apostrophe(start_date + ' 00:00:00') + \
                    ' AND DATE < ' + decorate_str_with_apostrophe(end_date + ' 23:59:59')

    return delete_query


def insert_batch_status_query(batch_function_name, batch_date, batch_success, batch_time):
    return 'INSERT INTO BATCH_STATUS ' \
           'VALUES (' + decorate_str_with_apostrophe(batch_function_name) + ', ' + \
           decorate_str_with_apostrophe(batch_date) + ', ' \
           + str(batch_success) + ', ' + \
           decorate_str_with_apostrophe(batch_time) + ')'


def update_query_set_with_dataframe(data, table_name, pk_column_name, decorate_apostrophe_mapping):
    # 반드시 삽입하는 테이블과 data의 columns가 일치하여야 함
    update_query_base = 'UPDATE ' + table_name
    update_query_base += ' SET '

    update_queries = []
    for i in range(len(data)):
        update_query = deepcopy(update_query_base)
        for col in data.columns:
            if col != pk_column_name:
                update_query += col + ' = ' \
                                + decorate_element_for_update_query(data.iloc[i][col],
                                                                    decorate_apostrophe_mapping[col]) \
                                + ', '
        update_query = update_query[:-2] \
                       + ' WHERE ' + pk_column_name + ' = ' + \
                       decorate_element_for_update_query(data.iloc[i][pk_column_name],
                                                         decorate_apostrophe_mapping[pk_column_name])
        update_queries.append(update_query)

    return update_queries


def select_query_where_in_with_list(table_name, column, include_list):
    select_query = data_all_select_query(table_name)
    select_query += ' WHERE ' + column + ' IN ' + convert_list_to_tuple_format_string(include_list)

    return select_query
########################################################################################################################


# String handling helper functions
def decorate_str_with_apostrophe(string):
    return "'" + string + "'"


def decorate_element_for_update_query(element, is_decorate):
    if is_decorate:
        return decorate_str_with_apostrophe(element)
    else:
        return element


def convert_list_to_tuple_format_string(input_list):
    tuple_format_str = '('

    for item in input_list:
        tuple_format_str += decorate_str_with_apostrophe(item)
        tuple_format_str += ', '

    tuple_format_str = tuple_format_str[:-2] + ')'

    return tuple_format_str
########################################################################################################################


# Data handling helper functions
def get_krx_data_by_code_set(code_set, start_date, end_date, batch_config, tickers_info):
    ret = pd.DataFrame()
    for code in code_set:
        df = getattr(Nf,
                     tickers_info.loc[tickers_info[batch_config.ticker_column_name] == code,
                                      batch_config.batch_function_column_name].item())(code, start_date, end_date)
        df.reset_index(drop=False, inplace=True)
        df[batch_config.ticker_column_name] = code
        ret = pd.concat([ret, df], axis=0, ignore_index=False)

    ret = ret[['TICKER', 'DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'FOREIGNEXHAUSTRATIO']]
    ret['DATE'] = ret['DATE'].dt.strftime('%Y-%m-%d')

    return ret


def data_grouping(before_data, new_data, key):
    merged_data = pd.merge(before_data, new_data, how='outer', on=key, indicator=True)

    exist_only_left = merged_data[merged_data['_merge'] == 'left_only']
    exist_both = merged_data[merged_data['_merge'] == 'both']
    exist_only_right = merged_data[merged_data['_merge'] == 'right_only']

    return exist_only_left, exist_both, exist_only_right


def extract_individual_data_from_merged_data(merged_data, merge_key, column_regex, original_columns):
    data_extracted = pd.concat([merged_data[merge_key],
                                merged_data.filter(regex=column_regex).reset_index(drop=True)], axis=1)
    data_extracted.columns = list(map(lambda x: x.replace(column_regex, ''), data_extracted.columns))
    data_extracted = data_extracted[original_columns]

    return data_extracted


def get_krx_listed_stock_info_with_markets(markets):
    data = pd.DataFrame()
    for market in markets:
        df = KRX.krx_listed_info(market)
        data = pd.concat([data, df], axis=0, ignore_index=False)

    return data


def log_list_for_updated_values(compared_data, pk_column, index_for_logging, column_list):
    ret = []
    for updated_column in column_list:
        if not pd.isna(compared_data.loc[index_for_logging, (updated_column, 'self')]):
            update_log = compared_data.loc[index_for_logging, (pk_column, '')] + ': '
            update_log += updated_column + ' '
            update_log += str(compared_data.loc[index_for_logging, (updated_column, 'self')])
            update_log += ' -> '
            update_log += str(compared_data.loc[index_for_logging, (updated_column, 'other')])
            ret.append(update_log)

            return ret


def preprocessing_data_to_fit_equity_index(raw_data_from_module, batch_config, ticker):
    data = raw_data_from_module.copy()
    data[batch_config.ticker_column_name] = ticker
    data.reset_index(inplace=True)
    data = data[batch_config.data_table_columns]
    data[batch_config.date_column_name] = \
        pd.to_datetime(data[batch_config.date_column_name]).dt.strftime('%Y-%m-%d')

    return data


def preprocessing_data_to_fit_krx_stock(raw_data_from_module, batch_config, ticker, market):
    data = raw_data_from_module.copy()
    data[batch_config.ticker_column_name] = ticker
    data[batch_config.market_column_name] = market
    data.reset_index(inplace=True)
    data = data[batch_config.data_table_columns]
    data[batch_config.date_column_name] = \
        pd.to_datetime(data[batch_config.date_column_name]).dt.strftime('%Y-%m-%d')

    return data


def extract_update_list_for_one_column(compare_result_with_tickers, pk_column, column_for_extract):
    data = compare_result_with_tickers.copy()
    data.dropna(inplace=True, subset=[(column_for_extract, 'self'), (column_for_extract, 'other')])
    update_list = list(data[pk_column])

    return update_list
########################################################################################################################
