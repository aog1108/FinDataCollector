import pymysql
import pandas as pd


class FinancialDBInfo:
    def __init__(self, db_info_dict):
        self.user = db_info_dict['user']
        self.passwd = db_info_dict['passwd']
        self.host = db_info_dict['host']
        self.db = db_info_dict['db']
        self.charset = db_info_dict['charset']


class DBObject:
    def __init__(self, db_info):
        self.db_object = pymysql.connect(
            user=db_info.user,
            passwd=db_info.passwd,
            host=db_info.host,
            db=db_info.db,
            charset=db_info.charset,
            cursorclass=pymysql.cursors.DictCursor
        )

    def execute(self, query):
        cursor = self.db_object.cursor()
        try:
            cursor.execute(query)
        except pymysql.err as e:
            self.db_object.rollback()
            raise RuntimeError('Execution failed', e)

    def commit(self):
        try:
            self.db_object.commit()
        except pymysql.err as e:
            self.db_object.rollback()
            raise RuntimeError('Commit failed', e)

    def rollback(self):
        self.db_object.rollback()

    def select(self, query):
        cursor = self.db_object.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def select_as_dataframe(self, query):
        json = self.select(query)
        return pd.DataFrame.from_records(json)

    def json_set_insert(self, json_set, table, data_type_mapping):
        cursor = self.db_object.cursor()
        query = json_to_sql_insert_query(json_set[0], table)
        values = []
        for json in json_set:
            values.append(json_to_sql_insert_values(json, data_type_mapping))

        if len(values) == 1:
            cursor.execute(query, values[0])
        else:
            cursor.executemany(query, values)
        self.db_object.commit()

    def close(self):
        self.db_object.close()


class DataTypeConverter:
    def string(self, value):
        return str(value)

    def date(self, value):
        return value.strftime('%Y-%m-%d')

    def datetime(self, value):
        return value.strftime('%Y-%m-%d %H:%M:%S')

    def convert(self, operator, value):
        return getattr(self, operator)(value)


def json_to_sql_insert_query(json, table):
    query = 'INSERT INTO ' + table + ' ('

    for key in json.keys():
        query = query + key + ', '
    query = query[:-2] + ')'

    query = query + ' VALUES ('
    for key in json.keys():
        query = query + '%s' + ', '
    query = query[:-2] + ')'

    return query


def read_financial_db_info():
    file_name = 'C:/Users/JeonSeongHun/PycharmProjects/FinDataCollector/Financial_DB_Info.txt'
    db_info_dict = {}

    with open(file_name) as f:
        for line in f:
            db_property, description = line.strip().split(None, 1)

            db_info_dict[db_property] = description.strip()

    return db_info_dict


def json_to_sql_insert_values(json, data_type_mapping):
    converter = DataTypeConverter()

    values = []
    keys = list(json.keys())
    for i in range(len(keys)):
        values.append(converter.convert(data_type_mapping[keys[i]], json[keys[i]]))

    values = tuple(values)

    return values
