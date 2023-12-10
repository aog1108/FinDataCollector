import ast
import pandas as pd
import requests
from datetime import datetime
import math


def krx_data_crawling(code, start_date, end_date):
    url = 'https://api.finance.naver.com/siseJson.naver'

    start_date = convert_date_yyyymmdd_format(start_date)
    end_date = convert_date_yyyymmdd_format(end_date)

    params = {
        'symbol': code,
        'requestType': 1,
        'startTime': start_date,
        'endTime': end_date,
        'timeframe': 'day'
    }

    res = requests.get(url, params=params)

    data = res.text.strip()
    stock = ast.literal_eval(data)
    stock = pd.DataFrame(stock, columns=stock[0])
    stock.drop(0, inplace=True)

    stock['날짜'] = pd.to_datetime(stock['날짜'])
    ohlcv = ['시가', '고가', '저가', '종가', '거래량']
    stock[ohlcv] = stock[ohlcv].apply(pd.to_numeric)
    stock['외국인소진율'] = stock['외국인소진율'].astype('float64')

    stock.rename(columns={
        '날짜': 'DATE',
        '시가': 'OPEN',
        '고가': 'HIGH',
        '저가': 'LOW',
        '종가': 'CLOSE',
        '거래량': 'VOLUME',
        '외국인소진율': 'FOREIGNEXHAUSTRATIO'}, inplace=True)
    stock.set_index('DATE', inplace=True)

    return stock


def foreign_index_data_crawling(code, start_date, end_date):
    url_base = 'https://finance.naver.com/world/worldDayListJson.nhn?symbol={code}&fdtc=0&page={page}'
    page_tuple = find_page(code, url_base, start_date, end_date)

    df = pd.DataFrame()
    datetime_start = datetime.strptime(start_date, '%Y-%m-%d')
    datetime_end = datetime.strptime(end_date, '%Y-%m-%d')
    if page_tuple[0] != 0:
        for page in range(page_tuple[0], page_tuple[1] + 1):
            page_json = get_foreign_index_json(code, page, url_base)
            page_df = pd.DataFrame(page_json)
            page_df['xymd'] = pd.to_datetime(page_df['xymd'].apply(lambda x: convert_yyyymmdd_to_yyyy_mm_dd_format(x)))
            page_df = page_df[(page_df['xymd'] >= datetime_start) & (page_df['xymd'] <= datetime_end)]
            df = pd.concat([df, page_df])

        df.drop(['symb', 'diff', 'rate'], axis=1, inplace=True)
        df.drop_duplicates(['xymd'], inplace=True)
        df.sort_values(by='xymd', inplace=True)
        df.rename(columns={
            'xymd': 'DATE',
            'open': 'OPEN',
            'high': 'HIGH',
            'low': 'LOW',
            'clos': 'CLOSE',
            'gvol': 'VOLUME'}, inplace=True)
        df.set_index('DATE', inplace=True)

    return df


def get_krx_data_by_code_set(code_set, start_date, end_date):
    data = pd.DataFrame()
    for code in code_set:
        df = krx_data_crawling(code, start_date, end_date)
        df.reset_index(drop=False, inplace=True)
        df['TICKER'] = code
        data = pd.concat([data, df], axis=0, ignore_index=False)

    data = data[['TICKER', 'DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'FOREIGNEXHAUSTRATIO']]

    return data


def convert_date_yyyymmdd_format(date):
    date_type = type(date)

    if date_type is str:
        date = date.replace('-', '')
    elif date_type is datetime or date_type is pd.Timestamp:
        date = date.strftime('%Y-%m-%d')
        date = date.replace('-', '')
    else:
        raise RuntimeError('Type of date should be string or datetime/pandas Timestamp')

    return date


def convert_yyyymmdd_to_yyyy_mm_dd_format(date):
    return date[:4] + '-' + date[4:6] + '-' + date[6:]


def get_foreign_index_json(code, page, url_base):
    url = url_base.format(code=code, page=page)
    raw = requests.get(url, headers={'User-agent': 'Mozilla/5.0'})
    data = raw.json()

    return data


def find_page(code, url_base, start_date, end_date):
    first_page_json = get_foreign_index_json(code, 1, url_base)
    recent_date = convert_yyyymmdd_to_yyyy_mm_dd_format(first_page_json[0]['xymd'])
    recent_datetime = datetime.strptime(recent_date, '%Y-%m-%d')

    one_page_data_num = len(first_page_json)

    # 첫번째 페이지에 바로 있는 경우, 바로 판단하여 리턴
    # 첫번째 페이지보다 큰 날짜가 들어온 경우, 첫번째 페이지 리턴
    start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
    if is_exist_date_in_json(first_page_json, start_date) or start_datetime > recent_datetime:
        estimated_start_page = 1
    else:
        estimated_start_page = 1 + find_page_between(end_date, recent_date, one_page_data_num)
        estimated_start_page = find_appropriate_page(end_date, code, estimated_start_page, url_base)

    end_datetime = datetime.strptime(end_date, '%Y-%m-%d')
    if is_exist_date_in_json(first_page_json, end_date) or end_datetime > recent_datetime:
        estimated_end_page = 1
    else:
        estimated_end_page = 1 + find_page_between(start_date, recent_date, one_page_data_num)
        estimated_end_page = find_appropriate_page(start_date, code, estimated_end_page, url_base)

    return estimated_start_page, estimated_end_page


def find_appropriate_page(date, code, page0, url_base):
    page = page0
    estimated_page_json = get_foreign_index_json(code, page, url_base)
    one_page_data_num = len(estimated_page_json)
    iter = 0
    # last_page보다 넘은 페이지 나왔을 경우에 처리 필요
    while not is_exist_date_in_json(estimated_page_json, date) and iter != 10:
        recent_date = convert_yyyymmdd_to_yyyy_mm_dd_format(estimated_page_json[0]['xymd'])
        page_between = find_page_between(date, recent_date, one_page_data_num)
        page += page_between
        if page == 0:
            break
        estimated_page_json = get_foreign_index_json(code, page, url_base)
        iter += 1

    if iter == 10:
        print(code + " : " + "Failure to find appropriate page")
        # raise RuntimeError("Failure to find an appropriate page")

    return page


def is_exist_date_in_json(json_data, date):
    is_exist = False

    date = datetime.strptime(date, '%Y-%m-%d')
    start_date = datetime.strptime(json_data[-1]['xymd'], '%Y%m%d')
    end_date = datetime.strptime(json_data[0]['xymd'], '%Y%m%d')

    if (date >= start_date) and (date <= end_date):
        is_exist = True

    return is_exist


def find_page_between(date, lasted_date_in_page, one_page_data_num):
    datetime_date = datetime.strptime(date, '%Y-%m-%d')
    lasted_date_in_page = datetime.strptime(lasted_date_in_page, '%Y-%m-%d')

    days_between = (lasted_date_in_page - datetime_date).days
    adjusted_days_between = int(days_between * 252/365)
    page_between = adjusted_days_between / one_page_data_num

    if page_between >= 0:
        page_between = max(1, math.ceil(page_between))
    else:
        page_between = min(-1, math.floor(page_between))

    return page_between
