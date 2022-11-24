
import pandas as pd

from data_loader import ExcelDataLoader
from project_paths import DATA_PATH


class IndexDataLoader:
    def __init__(self):
        self.excel_data_loader = ExcelDataLoader(DATA_PATH)
        self.data_path = DATA_PATH
        
    def load_csi(self):

        filename = 'CSI-INDEX.csv'
        filepath_abs = self.data_path / 'raw' / 'EXTERNAL' / filename
        data = (
            pd.read_csv(filepath_abs)
            .assign(YEAR_MONTH=lambda x: pd.to_datetime(x['YEAR_MONTH'], format='%Y%m'))
            .sort_values(by='YEAR_MONTH')
        )

        return data

    def load_composite_index(self):

        filename = '1_경기종합지수.csv'
        filepath_abs = self.data_path / 'raw' / 'EXTERNAL' / filename
        data = (
            pd.read_csv(filepath_abs)
            .assign(YEAR_MONTH=lambda x: pd.to_datetime(x['YEAR_MONTH'], format='%Y%m'))
            .sort_values(by='YEAR_MONTH')
        )

        return data

    def load_exchange_rate(self):

        filename = '2_USD_KRW 환율.csv'
        filepath_abs = self.data_path / 'raw' / 'EXTERNAL' / filename
        data = (
            pd.read_csv(filepath_abs)
            .assign(YEAR_MONTH=lambda x: pd.to_datetime(x['MONTH']))
            .drop(['MONTH'], axis=1)
            .sort_values(by='YEAR_MONTH')
            .assign(PRICE=lambda x : x['PRICE'].astype(str).str.replace('%', '').astype(float))
            .assign(CHANGE_RATE=lambda x : x['CHANGE RATE'].astype(str).str.replace('%', '').astype(float))
        )

        return data

    def load_interest_rate(self):

        filename = '3_월별 시장 금리.csv'
        filepath_abs = self.data_path / 'raw' / 'EXTERNAL' / filename
        data = (
            pd.read_csv(filepath_abs)
            .assign(YEAR_MONTH=lambda x: pd.to_datetime(x['Unnamed: 0'], format='%Y%m'))
            .drop(['Unnamed: 0'], axis=1)
            .sort_values(by='YEAR_MONTH')
        )

        return data

    def load_consumer_price_index(self):

        filename = '4_소비자물가지수.csv'
        filepath_abs = self.data_path / 'raw' / 'EXTERNAL' / filename
        data = (
            pd.read_csv(filepath_abs)
            .assign(YEAR_MONTH=lambda x: pd.to_datetime(x['MONTH'], format='%Y%m'))
            .drop(['MONTH'], axis=1)
            .sort_values(by='YEAR_MONTH')
        )

        return data

    def load_dow_jones_commodity_index(self):

        filename = '5_Dow Jones Commodity 다우존스 원자재 지수.csv'
        filepath_abs = self.data_path / 'raw' / 'EXTERNAL' / filename
        data = (
            pd.read_csv(filepath_abs)
            .assign(YEAR_MONTH=lambda x: pd.to_datetime(x['MONTH']))
            .drop(['MONTH'], axis=1)
            .sort_values(by='YEAR_MONTH')
            .assign(PRICE=lambda x : x['PRICE'].astype(str).str.replace('%', '').astype(float))
            .assign(CHANGE_RATE=lambda x : x['CHANGE RATE'].astype(str).str.replace('%', '').astype(float))
        )

        return data

    def load_apartment_housing_index(self):

        filename = '6_전국_수도권_지방_아파트_실거래가격지수.csv'
        filepath_abs = self.data_path / 'raw' / 'EXTERNAL' / filename
        data = (
            pd.read_csv(filepath_abs)
            .assign(YEAR_MONTH=lambda x: pd.to_datetime(x['MONTH'], format='%Y%m'))
            .drop(['MONTH'], axis=1)
            .sort_values(by='YEAR_MONTH')
        )

        return data

    def load_import_export_performance(self):

        filename = '7_국가별수출입 실적(미중일).csv'
        filepath_abs = self.data_path / 'raw' / 'EXTERNAL' / filename
        data = pd.read_csv(filepath_abs)

        return data
