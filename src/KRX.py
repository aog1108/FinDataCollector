import FinanceDataReader as Fdr


def krx_listed_info(market):
    data = Fdr.StockListing(market)
    data = data[['Code', 'ISU_CD', 'Name', 'MarketId', 'Stocks']]
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
