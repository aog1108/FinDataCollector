import FinanceDataReader as Fdr
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from time import sleep
import os
import pandas as pd
from datetime import datetime

rpa_download_route = r'C:\Users\JeonSeongHun\PycharmProjects\FinDataCollector\krx_rpa_route'

krx_listed_stock_info_url = 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020201'
krx_listed_stock_info_download_class_name = 'CI-MDI-UNIT-DOWNLOAD'
krx_listed_stock_info_csv_xpath = \
    '/html/body/div[2]/section[2]/section/section/div/div/form/div[2]/div[2]/div[2]/div/div[2]'


def krx_listed_info(market):
    data = Fdr.StockListing(market)
    data = data[['Code', 'ISU_CD', 'Name', 'MarketId', 'Stocks']]
    return data


def rpa_download_krx_listed_stock_info(download_dir=rpa_download_route):
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_experimental_option('useAutomationExtension', False)
    prefs = {'download.default_directory': download_dir}
    chrome_options.add_experimental_option('prefs', prefs)

    driver = webdriver.Chrome(options=chrome_options)

    try:
        driver.get(krx_listed_stock_info_url)
        sleep(5)
        driver.find_element(By.CLASS_NAME, krx_listed_stock_info_download_class_name).click()
        sleep(1)
        driver.find_element(By.XPATH, krx_listed_stock_info_csv_xpath).click()
        sleep(5)

        files = os.listdir(download_dir)

        if not files:
            raise FileNotFoundError('KRX listed stock info download failed')

        file_name = max([download_dir + '/' + f for f in os.listdir(download_dir)], key=os.path.getctime)
        data = pd.read_csv(file_name, encoding='ANSI')
        data = data[['단축코드', '표준코드', '한글 종목명', '시장구분', '상장주식수']]
        data = data.rename(columns={'단축코드': 'TICKER',
                                    '표준코드': 'ISIN',
                                    '한글 종목명': 'NAME',
                                    '시장구분': 'MARKET',
                                    '상장주식수': 'LISTED_SHARES'})
        data['MARKET'].loc[data['MARKET'] == 'KOSDAQ GLOBAL'] = 'KOSDAQ'
        os.remove(file_name)

    finally:
        driver.quit()

    return data


def krx_delisted_info():
    data = Fdr.StockListing('KRX-DELISTING')
    data = data[['Symbol', 'Name', 'Market',
                 'SecuGroup', 'Kind', 'ListingDate', 'DelistingDate',
                 'Industry', 'ParValue', 'ListingShares']]
    return data


def get_krx_stock_data(ticker, start_date, end_date):
    data = Fdr.DataReader(ticker, start_date, end_date, 'KRX')
    return data


# 액면 분할, 증자, 감자, 소각 등 발생 시 주가 조정 어떻게?
# 상장 -> 상장폐지 업데이트 어떻게?

# 3. LISTED_SHARES가 바뀌면 해당 종목 과거데이터도 전부 반영해서 업데이트
