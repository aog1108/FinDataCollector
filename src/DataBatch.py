import DBHandling as Db
import Binance as Bn
import json
from DataBatchHelper import *
from datetime import datetime
import logging
import numpy as np
import multiprocessing as mp
from functools import partial
import inspect


class BatchHandler:
    def __init__(self, batch_config):
        self.__db_info = Db.FinancialDBInfo(Db.read_financial_db_info())
        self.__db_object = Db.DBObject(self.__db_info)

        self.__batch_config = batch_config

        self.__batch_functions = self.__batch_config.batch_functions
        self.__batch_function_flags = self.__batch_config.batch_function_flags
        self.__batch_function_config_mapping = self.__batch_config.batch_function_config_mapping

        self.__logger = logging.getLogger()
        self.__batch_status_insert_on = True

    def __del__(self):
        self.__db_object.close()

    def set_batch_functions(self, batch_functions):
        self.__batch_functions = batch_functions

    def set_batch_function_flag(self, batch_function_name, flag):
        self.__batch_function_flags[batch_function_name] = flag

    def set_batch_function_flags(self, batch_functions_flag_json):
        self.__batch_function_flags = batch_functions_flag_json

    def set_logger(self, logger):
        self.__logger = logger

    def set_batch_status_insert_on(self, batch_status_insert_on):
        self.__batch_status_insert_on = batch_status_insert_on

    def run_entire_batch(self, batch_date):
        try:
            self.__logger.info(self.__batch_config.batch_name + '\n')
            for batch in self.__batch_functions:
                if self.__batch_function_flags[batch] == 'On':
                    getattr(self, batch)(batch_date)
                    if self.__batch_status_insert_on:
                        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        insert_query = insert_batch_status_query(batch, batch_date, 1, now)
                        self.__db_object.execute(insert_query)
                        self.__db_object.commit()
                    self.__logger.info(decorate_str_with_apostrophe(batch) + ' is complete\n')

        except Exception as e:
            self.__db_object.rollback()
            if self.__batch_status_insert_on:
                now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                insert_query = insert_batch_status_query(batch, batch_date, 0, now)
                self.__db_object.execute(insert_query)
                self.__db_object.commit()
            self.__logger.error(e)
            raise RuntimeError(decorate_str_with_apostrophe(batch) + ' failed because of the above error')

    def do_equity_index_data_batch(self, batch_date):
        batch_name = inspect.currentframe().f_code.co_name
        batch_config = self.__batch_function_config_mapping[batch_name]

        # 네이버 금융에 없는 기간에 대한 배치 수행은 허용하지 않음.
        # 해당 기간을 넉넉하게 2005년 이전으로 잡았음.
        if pd.to_datetime(batch_date) < pd.to_datetime('2005-01-01'):
            raise RuntimeError('Batch before 2005 is not allowed')

        index_ticker = self.__db_object.select_as_dataframe(
            select_query_from_data_ticker_table(batch_config.asset_class, batch_config.contract_type)
        )
        index_ticker = index_ticker.loc[index_ticker[batch_config.ticker_column_name].isin(batch_config.upload_list)]

        count = 0
        for ticker_i in index_ticker.index:
            ticker = index_ticker.loc[ticker_i, batch_config.ticker_column_name]

            delete_query = price_data_delete_query(batch_config.data_table, ticker, batch_date, batch_date)
            self.__db_object.execute(delete_query)

            data = getattr(Nf, index_ticker.loc[ticker_i, batch_config.batch_function_column_name])(ticker,
                                                                                                    batch_date,
                                                                                                    batch_date)

            if not data.empty:
                data[batch_config.ticker_column_name] = ticker
                data.reset_index(inplace=True)
                data = data[batch_config.data_table_columns]
                data[batch_config.date_column_name] = \
                    pd.to_datetime(data[batch_config.date_column_name]).dt.strftime('%Y-%m-%d')
                json_format_data = eval(data.to_json(orient='records'))
                self.__db_object.json_set_insert(json_format_data,
                                                 batch_config.data_table,
                                                 batch_config.data_type_mapping_for_json_insert)
                count += 1
            else:
                self.__logger.info(ticker + ' is not uploaded')

        self.__logger.info('Equity Index Upload: ' + str(count) + '/' + str(len(index_ticker.index)))

        self.__db_object.commit()

    def do_crypto_data_batch(self, batch_date):
        batch_name = inspect.currentframe().f_code.co_name
        batch_config = self.__batch_function_config_mapping[batch_name]

        api_info = Bn.read_api_info()

        for contract in batch_config.contract_type:
            select_query = select_query_from_data_ticker_table(batch_config.asset_class, contract)
            tickers = self.__db_object.select_as_dataframe(select_query)
            exchange = Bn.define_exchange(api_info[0], api_info[1], contract)

            for ticker_i in tickers.index:
                ticker = tickers.loc[ticker_i, batch_config.ticker_column_name]

                delete_query = price_data_delete_query(batch_config.data_table, ticker, batch_date, batch_date)
                self.__db_object.execute(delete_query)

                data = getattr(Bn, tickers.loc[ticker_i, batch_config.batch_function_column_name])(exchange,
                                                                                                   ticker,
                                                                                                   batch_date,
                                                                                                   batch_date,
                                                                                                   batch_config.
                                                                                                   time_frame)

                if not data.empty:
                    data[batch_config.ticker_column_name] = ticker
                    data[batch_config.timezone_column_name] = batch_config.timezone
                    data.reset_index(inplace=True)
                    data = data[batch_config.data_table_columns]
                    data[batch_config.date_column_name] = \
                        pd.to_datetime(data[batch_config.date_column_name]).dt.strftime('%Y-%m-%d %H:%M:%S')
                    json_format_data = data.to_json(orient='records')  # 티커에 역슬래쉬 생기는 현상 방지
                    json_format_data = json.loads(json_format_data)  # 티커에 역슬래쉬 생기는 현상 방지
                    self.__db_object.json_set_insert(json_format_data,
                                                     batch_config.data_table,
                                                     batch_config.data_type_mapping_for_json_insert)
                    self.__logger.info(ticker + ' ' + contract + ' Upload: ' + str(len(data.index)))

        self.__db_object.commit()

    def do_krx_stock_data_batch(self, batch_date):
        batch_name = inspect.currentframe().f_code.co_name
        batch_config = self.__batch_function_config_mapping[batch_name]

        delete_query = data_all_delete_query(batch_config.data_table)
        delete_query += ' WHERE ' + batch_config.date_column_name + ' = ' + decorate_str_with_apostrophe(batch_date)
        self.__db_object.execute(delete_query)

        select_query = select_query_from_data_ticker_table(batch_config.asset_class, batch_config.contract_type)
        tickers = self.__db_object.select_as_dataframe(select_query)

        jobs = np.array_split(list(tickers[batch_config.ticker_column_name]), batch_config.num_cores)

        pool = mp.Pool(batch_config.num_cores)
        func = partial(get_krx_data_by_code_set,
                       start_date=batch_date,
                       end_date=batch_date,
                       batch_config=batch_config,
                       tickers_info=tickers)
        results = pool.map(func, jobs)
        pool.close()
        pool.join()

        data = pd.DataFrame()
        for result in results:
            data = pd.concat([data, result], axis=0, ignore_index=False)

        if not data.empty:
            select_query = data_all_select_query(batch_config.listed_stocks_info_table)
            listed_stocks_info = pd.DataFrame.from_records(self.__db_object.select(select_query))

            joined_data = pd.merge(data,
                                   listed_stocks_info,
                                   left_on=batch_config.ticker_column_name,
                                   right_on=batch_config.ticker_column_name,
                                   how='inner')
            data = joined_data[batch_config.data_table_columns]
            json_format_data = eval(data.to_json(orient='records'))
            self.__db_object.json_set_insert(json_format_data,
                                             batch_config.data_table,
                                             batch_config.data_table_data_type_mapping_for_json_insert)

            data_uploaded_num = len(data)
            tickers_num = len(tickers)
            self.__logger.info('KRX Stock Upload : ' + str(data_uploaded_num) + '/' + str(tickers_num))
        else:
            self.__logger.info('KRX Stock Upload : Nothing')

        self.__db_object.commit()

    def do_insert_and_delete_krx_listed_stocks_info_batch(self, batch_date):
        batch_name = inspect.currentframe().f_code.co_name
        batch_config = self.__batch_function_config_mapping[batch_name]

        select_query = data_all_select_query(batch_config.listed_stocks_info_table)
        select_query += ' WHERE ' + batch_config.market_column_name + ' IN ' \
                        + convert_list_to_tuple_format_string(batch_config.markets)
        before_data = self.__db_object.select_as_dataframe(select_query)

        data = get_krx_listed_stock_info_with_markets(batch_config.markets)
        data.columns = batch_config.listed_stocks_info_table_columns
        data[batch_config.market_column_name] = data[batch_config.market_column_name].map(
            batch_config.market_id_mapping
        )

        merge_key = [batch_config.ticker_column_name]
        grouped_data = data_grouping(before_data, data, merge_key)

        delisted_tickers = list(grouped_data[0][batch_config.ticker_column_name])
        new_listed_tickers = list(grouped_data[-1][batch_config.ticker_column_name])

        # 상장 폐지
        if len(delisted_tickers) > 0:
            related_data = before_data[before_data[batch_config.ticker_column_name].isin(delisted_tickers)]
            related_data[batch_config.delete_batch_date_column_name] = batch_date

            delete_query = data_all_delete_query(batch_config.listed_stocks_info_table)
            delete_query += ' WHERE ' + batch_config.ticker_column_name + ' = '
            tuple_format_str = convert_list_to_tuple_format_string(delisted_tickers)

            json_format_data = eval(related_data.to_json(orient='records'))
            self.__db_object.json_set_insert(json_format_data,
                                             batch_config.deleted_stocks_info_table,
                                             batch_config.deleted_listed_stocks_data_type_mapping)
            # 'WHERE TICKER IN'으로 delete시 timeout 발생하여서, 반복문 이용하여 삭제
            for ticker in delisted_tickers:
                real_delete_query = delete_query + decorate_str_with_apostrophe(ticker)
                self.__db_object.execute(real_delete_query)
            self.__logger.info(tuple_format_str + ' is(are) delisted')
        else:
            self.__logger.info('Nothing to delete from ' + batch_config.listed_stocks_info_table)

        # 신규 상장
        if len(new_listed_tickers) > 0:
            related_data = data[data[batch_config.ticker_column_name].isin(new_listed_tickers)]
            json_format_data = eval(related_data.to_json(orient='records'))
            self.__db_object.json_set_insert(json_format_data,
                                             batch_config.listed_stocks_info_table,
                                             batch_config.listed_stocks_info_data_type_mapping)

            tuple_format_str = convert_list_to_tuple_format_string(new_listed_tickers)
            self.__logger.info(tuple_format_str + ' is(are) newly listed')
        else:
            self.__logger.info('Nothing to insert to ' + batch_config.listed_stocks_info_table)

        self.__db_object.commit()

    def do_update_krx_listed_stocks_info_batch(self, batch_date):
        batch_name = inspect.currentframe().f_code.co_name
        batch_config = self.__batch_function_config_mapping[batch_name]

        select_query = data_all_select_query(batch_config.listed_stocks_info_table)
        select_query += ' WHERE ' + batch_config.market_column_name + ' IN ' \
                        + convert_list_to_tuple_format_string(batch_config.markets)
        before_data = pd.DataFrame.from_records(self.__db_object.select(select_query))

        data = get_krx_listed_stock_info_with_markets(batch_config.markets)
        data.columns = batch_config.listed_stocks_info_table_columns
        data[batch_config.market_column_name] = data[batch_config.market_column_name].map(
            batch_config.market_id_mapping
        )

        merge_key = [batch_config.ticker_column_name]
        grouped_data = data_grouping(before_data, data, merge_key)

        already_exist_data = grouped_data[1]
        already_exist_data_x = extract_individual_data_from_merged_data(already_exist_data,
                                                                        merge_key,
                                                                        '_x',
                                                                        batch_config.listed_stocks_info_table_columns)
        already_exist_data_x[batch_config.listed_shares_column_name] = \
            already_exist_data_x[batch_config.listed_shares_column_name].map(lambda x: str(int(x)))

        already_exist_data_y = extract_individual_data_from_merged_data(already_exist_data,
                                                                        merge_key,
                                                                        '_y',
                                                                        batch_config.listed_stocks_info_table_columns)
        already_exist_data_y[batch_config.listed_shares_column_name] = \
            already_exist_data_y[batch_config.listed_shares_column_name].map(lambda x: str(int(x)))

        compare_result = already_exist_data_x.compare(already_exist_data_y)

        if len(compare_result) > 0:
            # KRX_LISTED_STOCKS_INFO 업데이트
            data_for_update = already_exist_data_y.loc[compare_result.index]
            tickers_for_update = list(data_for_update[batch_config.ticker_column_name])
            compare_result[batch_config.ticker_column_name] = tickers_for_update
            update_queries = update_query_set_with_dataframe(data_for_update,
                                                             batch_config.listed_stocks_info_table,
                                                             batch_config.ticker_column_name,
                                                             batch_config.decorating_apostrophe_mapping_for_update)
            for update_query in update_queries:
                self.__db_object.execute(update_query)

            update_list = list(data_for_update[batch_config.ticker_column_name])
            tuple_format_str = convert_list_to_tuple_format_string(update_list)
            self.__logger.info(tuple_format_str + ' is(are) updated')
            updated_column_list = np.unique(compare_result.columns.get_level_values(0))
            updated_column_list = updated_column_list[np.where(updated_column_list != 'TICKER')]
            for index in compare_result.index:
                update_log_list = log_list_for_updated_values(compare_result,
                                                              batch_config.ticker_column_name,
                                                              index,
                                                              updated_column_list)
                for update_log in update_log_list:
                    self.__logger.info(update_log)

            select_query = select_query_where_in_with_list(batch_config.data_tickers_table,
                                                           batch_config.ticker_column_name,
                                                           tickers_for_update)
            tickers_info = self.__db_object.select_as_dataframe(select_query)

            # NAME 업데이트 된 TICKER에 대해서 DATA_TICKER 테이블 NAME 업데이트
            if (batch_config.name_column_name, 'self') in compare_result.columns:
                name_updated_list = extract_update_list_for_one_column(compare_result,
                                                                       batch_config.ticker_column_name,
                                                                       batch_config.name_column_name)
                tickers_info_to_update_name = tickers_info[
                    tickers_info[batch_config.ticker_column_name].isin(name_updated_list)]

                merge_key = [batch_config.ticker_column_name]
                grouped_data = data_grouping(tickers_info_to_update_name, data_for_update, merge_key)

                merged_data = grouped_data[1]
                merged_data.columns = list(map(lambda x: x.replace('_y', ''), merged_data.columns))
                updated_tickers_info = merged_data[batch_config.data_tickers_table_columns]

                update_queries = update_query_set_with_dataframe(updated_tickers_info,
                                                                 batch_config.data_tickers_table,
                                                                 batch_config.ticker_column_name,
                                                                 batch_config.decorating_apostrophe_mapping_for_update)
                for update_query in update_queries:
                    self.__db_object.execute(update_query)

            # LISTED_SHARES가 업데이트된 대상 추출
            tickers_for_update = list(data_for_update[batch_config.ticker_column_name])
            compare_result[batch_config.ticker_column_name] = tickers_for_update
            compare_result.dropna(inplace=True, subset=[(batch_config.listed_shares_column_name, 'self'),
                                                        (batch_config.listed_shares_column_name, 'other')])
            tickers_for_update = list(compare_result[batch_config.ticker_column_name])
            tickers_info = tickers_info[tickers_info[batch_config.ticker_column_name].isin(tickers_for_update)]

            # 데이터 업데이트 시작
            select_query = select_query_where_in_with_list(batch_config.data_table,
                                                           batch_config.ticker_column_name,
                                                           tickers_for_update)
            data_in_db = self.__db_object.select_as_dataframe(select_query)

            for ticker_i in tickers_info.index:
                ticker = tickers_info.loc[ticker_i, batch_config.ticker_column_name]
                
                # Backup DB에 주가 삽입
                delete_query = 'DELETE FROM ' + batch_config.backup_data_table
                delete_query += ' WHERE ' + batch_config.ticker_column_name + ' = ' + decorate_str_with_apostrophe(
                    ticker)
                delete_query += ' AND ' + batch_config.backup_date_column_name + ' = ' \
                                + decorate_str_with_apostrophe(batch_date)
                self.__db_object.execute(delete_query)

                data_of_one_ticker_in_db = data_in_db[data_in_db[batch_config.ticker_column_name] == ticker]
                data_of_one_ticker_in_db[batch_config.date_column_name] \
                    = pd.to_datetime(data_of_one_ticker_in_db[batch_config.date_column_name]).dt.strftime('%Y-%m-%d')
                data_of_one_ticker_in_db[batch_config.backup_date_column_name] = batch_date

                json_format_data = eval(data_of_one_ticker_in_db.to_json(orient='records'))
                self.__db_object.json_set_insert(json_format_data,
                                                 batch_config.backup_data_table,
                                                 batch_config.backup_data_type_mapping_for_json_insert)

                data = getattr(Nf, tickers_info.loc[ticker_i,
                                                    batch_config.batch_function_column_name])(ticker, '1990-01-01',
                                                                                              batch_date)
                market = np.unique(data_in_db[data_in_db['TICKER'] == ticker]['MARKET'])[0]

                if not data.empty:
                    # 실제 DB에 주가 업데이트
                    delete_query = price_data_delete_query(batch_config.data_table, ticker, '1990-01-01', batch_date)
                    self.__db_object.execute(delete_query)

                    data = preprocessing_data_to_fit_krx_stock(data, batch_config, ticker, market)
                    json_format_data = eval(data.to_json(orient='records'))
                    self.__db_object.json_set_insert(json_format_data,
                                                     batch_config.data_table,
                                                     batch_config.data_type_mapping_for_json_insert)
                    if len(data) != len(data_of_one_ticker_in_db):
                        self.__logger.info(ticker + ' : Updated data length is different')

        else:
            self.__logger.info('Nothing to update from' + batch_config.listed_stocks_info_table)

        self.__db_object.commit()

    def do_synchronize_krx_stocks_data_ticker_batch(self, batch_date):
        batch_name = inspect.currentframe().f_code.co_name
        batch_config = self.__batch_function_config_mapping[batch_name]

        select_query = select_query_from_data_ticker_table(batch_config.asset_class, batch_config.contract_type)
        ticker_data = self.__db_object.select_as_dataframe(select_query)

        select_query = data_all_select_query(batch_config.listed_stocks_info_table)
        select_query += ' WHERE ' + batch_config.market_column_name + ' IN '
        tuple_format_str = convert_list_to_tuple_format_string(batch_config.markets)
        select_query += tuple_format_str
        krx_listed_stocks_info = self.__db_object.select_as_dataframe(select_query)

        merge_key = [batch_config.ticker_column_name]
        grouped_data = data_grouping(ticker_data, krx_listed_stocks_info, merge_key)

        delisted_tickers = list(grouped_data[0][batch_config.ticker_column_name])
        new_listed_tickers = list(grouped_data[-1][batch_config.ticker_column_name])

        if len(delisted_tickers) > 0:
            delete_query = data_all_delete_query(batch_config.data_tickers_table)
            delete_query += ' WHERE ' + batch_config.ticker_column_name + ' = '
            tuple_format_str = convert_list_to_tuple_format_string(delisted_tickers)

            for ticker in delisted_tickers:
                real_delete_query = delete_query + decorate_str_with_apostrophe(ticker)
                self.__db_object.execute(real_delete_query)
            self.__logger.info(tuple_format_str + ' are excluded from ' + batch_config.data_tickers_table)
        else:
            self.__logger.info('Nothing to delete from ' + batch_config.data_tickers_table)

        if len(new_listed_tickers) > 0:
            related_data = krx_listed_stocks_info[
                krx_listed_stocks_info[batch_config.ticker_column_name].isin(new_listed_tickers)
            ]

            insert_data = pd.DataFrame(columns=batch_config.data_tickers_table_columns)
            insert_data[batch_config.ticker_column_name] = related_data[batch_config.ticker_column_name]
            insert_data[batch_config.name_column_name] = related_data[batch_config.name_column_name]
            for item in batch_config.data_tickers_value_mapping.items():
                insert_data[item[0]] = item[1]

            json_format_data = eval(insert_data.to_json(orient='records'))
            self.__db_object.json_set_insert(json_format_data,
                                             batch_config.data_tickers_table,
                                             batch_config.data_tickers_data_type_mapping)

            tuple_format_str = convert_list_to_tuple_format_string(new_listed_tickers)
            self.__logger.info(tuple_format_str + ' are inserted to ' + batch_config.data_tickers_table)
        else:
            self.__logger.info('Nothing to insert to ' + batch_config.data_tickers_table)

        self.__db_object.commit()

    def do_insert_and_delete_krx_delisted_stocks_info_batch(self, batch_date):
        batch_name = inspect.currentframe().f_code.co_name
        batch_config = self.__batch_function_config_mapping[batch_name]

        select_query = data_all_select_query(batch_config.delisted_stocks_info_table)
        select_query += ' WHERE ' + batch_config.market_column_name + ' IN '
        tuple_format_str = convert_list_to_tuple_format_string(batch_config.markets)
        select_query += tuple_format_str

        delisted_data = pd.DataFrame.from_records(self.__db_object.select(select_query))
        krx_delisted_stocks_info = KRX.krx_delisted_info()
        krx_delisted_stocks_info = krx_delisted_stocks_info[
            krx_delisted_stocks_info['Market'].isin(batch_config.markets) &
            krx_delisted_stocks_info['SecuGroup'].isin(batch_config.secu_groups)
            ]
        is_delisted_tickers = delisted_data[batch_config.ticker_column_name].isin(
            krx_delisted_stocks_info['Symbol']
        )
        listed_tickers = list(delisted_data[~is_delisted_tickers][batch_config.ticker_column_name])
        is_already_delisted_tickers = krx_delisted_stocks_info['Symbol'].isin(
            delisted_data[batch_config.ticker_column_name]
        )
        new_delisted_tickers = list(krx_delisted_stocks_info[~is_already_delisted_tickers]['Symbol'])

        if len(listed_tickers) > 0:
            delete_query = data_all_delete_query(batch_config.delisted_stocks_info_table)
            delete_query += ' WHERE ' + batch_config.ticker_column_name + ' = '
            tuple_format_str = convert_list_to_tuple_format_string(listed_tickers)

            for ticker in listed_tickers:
                real_delete_query = delete_query + decorate_str_with_apostrophe(ticker)
                self.__db_object.execute(real_delete_query)
                self.__db_object.commit()
            self.__logger.info(tuple_format_str + ' are excluded from ' + batch_config.delisted_stocks_info_table)
        else:
            self.__logger.info('Nothing to delete from ' + batch_config.delisted_stocks_info_table)

        if len(new_delisted_tickers) > 0:
            related_data = krx_delisted_stocks_info[krx_delisted_stocks_info['Symbol'].isin(
                new_delisted_tickers)
            ]
            related_data.columns = batch_config.delisted_stocks_info_table_columns
            related_data[batch_config.listed_date_column_name] = \
                related_data[batch_config.listed_date_column_name].dt.strftime('%Y-%m-%d')
            related_data[batch_config.delisted_date_column_name] = \
                related_data[batch_config.delisted_date_column_name].dt.strftime('%Y-%m-%d')
            related_data = related_data.replace({np.nan: 'NULL'})

            json_format_data = eval(related_data.to_json(orient='records'))
            for json_data in json_format_data:
                for key in json_data.keys():
                    if json_data[key] == 'NULL':
                        json_data[key] = None

            self.__db_object.json_set_insert(json_format_data,
                                             batch_config.delisted_stocks_info_table,
                                             batch_config.delisted_stocks_info_table_data_type_mapping)

            tuple_format_str = convert_list_to_tuple_format_string(new_delisted_tickers)
            self.__logger.info(tuple_format_str + ' are inserted to ' + batch_config.delisted_stocks_info_table)
        else:
            self.__logger.info('Nothing to insert to ' + batch_config.delisted_stocks_info_table)

        self.__db_object.commit()
