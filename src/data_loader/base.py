import os
from pathlib import Path

import pandas as pd


class ExcelDataLoader:
    def __init__(self, data_path: Path):
        self.data_path = data_path
        self.cached_path = data_path / 'cached'
        if not os.path.exists(self.cached_path):
            os.mkdir(self.cached_path)

    def load_ksic_data(self, load_cached=True):
        filename = 'ksic.xlsx'
        filepath_abs = self.data_path / 'raw' / filename
        data = self._load_xlsx(filepath_abs, load_cached=load_cached).T.set_index(
            0).T.fillna(method='ffill').iloc[1:]
        data.columns = ['대분류_코드', '대분류', '중분류_코드', '중분류',
                        '소분류_코드', '소분류', '세분류_코드', '세분류', '세세분류_코드', '세세분류']
        return data

    def load_finance_data(self):
        filename = 'finance_data.txt'
        filepath_abs = self.data_path / 'raw' / 'NUMBLE' / filename
        data = pd.read_csv(filepath_abs, sep='\t')
        return data

    def load_meta_data(self, load_cached=True):
        filename = 'data_layout.xlsx'
        filepath_abs = self.data_path / 'raw' / 'NUMBLE' / filename
        data = self._load_xlsx(filepath_abs, load_cached=load_cached)
        return data

    def load_description_data(self, load_cached=True, sheet_name=None):
        filename = 'data_code.xlsx'
        filepath_abs = self.data_path / 'raw' / 'NUMBLE' / filename
        data = self._load_xlsx(
            filepath_abs,
            cached_filepath=str(filepath_abs).replace(
                '.xlsx', f' {sheet_name}.xlsx'),
            load_cached=load_cached,
            sheet_name=sheet_name,
        )
        return data

    def load_company_data(self, load_cached=True, closed=False):
        if closed is False:
            filename = 'finance_active.xlsx'
        else:
            filename = 'finance_closed.xlsx'
        filepath_abs = self.data_path / 'raw' / 'NUMBLE' / filename

        data_info = self._load_xlsx(
            filepath_abs,
            cached_filepath=str(filepath_abs).replace('.xlsx', ' 기업개요.xlsx'),
            load_cached=load_cached,
            sheet_name='기업개요',
        )
        data_history = self._load_xlsx(
            filepath_abs,
            cached_filepath=str(filepath_abs).replace('.xlsx', ' 휴폐업이력.xlsx'),
            load_cached=load_cached,
            sheet_name='휴폐업이력',
        )
        data = data_info.merge(
            data_history, left_on='BIZ_NO', right_on='BIZ_NO', how='left')
        return data

    def _load_xlsx(
        self,
        filepath: str,
        cached_filepath=None,
        sheet_name=None,
        load_cached=True,
    ):
        if cached_filepath is None:
            filepath_cached = str(filepath).replace(
                str(self.data_path / 'raw'), str(self.cached_path))
        else:
            filepath_cached = str(cached_filepath).replace(
                str(self.data_path / 'raw'), str(self.cached_path))

        if not os.path.exists(filepath_cached) or load_cached is False:
            if sheet_name is None:
                data = pd.read_excel(filepath)
            else:
                data = pd.read_excel(filepath, sheet_name=sheet_name)
            data.to_csv(filepath_cached)
        else:
            data = pd.read_csv(filepath_cached, index_col=0)
        return data
