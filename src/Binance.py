import ccxt
import pandas as pd
from copy import deepcopy
from datetime import datetime
from dateutil.relativedelta import relativedelta


unit_mapping = {'m': 1, 'h': 60}


def read_api_info():
    file_name = 'C:/Users/JeonSeongHun/PycharmProjects/FinDataCollector/Binance_API_Key.txt'
    with open(file_name, 'r') as file:
        lines = file.readlines()
        api_key = lines[1].strip()
        secret = lines[3].strip()
        file.close()

    return api_key, secret


def define_exchange(api_key, secret, default_type, enable_rate_limit=True):
    exchange = ccxt.binance(config={
        'apiKey': api_key,
        'secret': secret,
        'enableRateLimit': enable_rate_limit,
        'options': {
            'defaultType': default_type
        }
    })

    return exchange


# 00시 시작 1일 동안의 데이터를 가져옴.
def fetch_one_day_data(exchange, ticker, date, time_frame):
    date = exchange.parse8601(date + '00:00:00')
    unit = time_frame[-1]
    time_frame_copy = deepcopy(time_frame)
    number_of_time = int(time_frame_copy.replace(unit, ''))
    limit = int(1440 / (number_of_time * unit_mapping[unit]))
    data = pd.DataFrame(columns=['DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME'])

    ohlcv = exchange.fetch_ohlcv(ticker, time_frame, date, limit)
    df = pd.DataFrame(ohlcv, columns=['DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME'])
    data = pd.concat([data, df], axis=0)
    
    # 최대로 가져올 수 있는 데이터 개수 1000개로 제한됨
    limit = max(limit - 1000, 0)

    # 5분봉만 되어도 1일 데이터에 1000개 미만이 되므로, while문 사용 안 함
    if limit > 0:
        limit = limit + 1
        date = data['DATE'].iloc[-1]
        ohlcv = exchange.fetch_ohlcv(ticker, time_frame, date, limit)
        df = pd.DataFrame(ohlcv, columns=['DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME'])
        data = pd.concat([data, df.iloc[1:]], axis=0)

    data['DATE'] = pd.to_datetime(data['DATE'], unit='ms')
    data.set_index('DATE', inplace=True)

    return data


def download_binance_data(exchange, ticker, start_date, end_date, time_frame):
    start_datetime = datetime.strptime(start_date, '%Y-%m-%d')

    date = deepcopy(start_datetime)
    one_day_shift = relativedelta(days=1)
    one_second_shift = relativedelta(seconds=1)

    end_datetime = datetime.strptime(end_date, '%Y-%m-%d') + one_day_shift - one_second_shift

    data = pd.DataFrame(columns=['OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME'])
    data.index.name = 'DATE'
    while date <= end_datetime:
        date_str = date.strftime('%Y-%m-%d')
        df = fetch_one_day_data(exchange, ticker, date_str, time_frame)
        data = pd.concat([data, df], axis=0)
        date += one_day_shift

    data = data.loc[start_datetime: end_datetime]

    return data
