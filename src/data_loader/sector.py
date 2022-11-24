from typing import List

import pandas as pd

from data_loader import ExcelDataLoader
from project_paths import DATA_PATH


class SectorDataLoader:
    def __init__(self):
        self.excel_data_loader = ExcelDataLoader(DATA_PATH)
        self.meta_data = self.excel_data_loader.load_meta_data()

    def load_dataset(self):

        company_data = self.load_company_data()
        finance_data = self.load_finance_data()

        return (
            company_data
            .merge(finance_data, left_on='BIZ_NO',right_on='사업자번호', how='right')
            .merge(
                (
                    company_data.loc[lambda x: x['Closed'] == True]
                    .loc[:, ['BIZ_NO', 'Closed', 'STAT_OCR_DATE']]
                    .assign(Closed_Year=lambda x: pd.to_datetime(x['STAT_OCR_DATE'], format='%Y%m%d').dt.year)
                ),
                left_on='BIZ_NO',
                right_on='BIZ_NO',
                how='left',
            )
        )

    def load_finance_data(self):
        return self.excel_data_loader.load_finance_data()

    def load_company_data(self, kr_column_names: bool = False):
        company_data = (
            pd.concat(
                [
                    (
                        self.excel_data_loader.load_company_data(closed=False)
                        .assign(Source='Active')),
                    (
                        self.excel_data_loader.load_company_data(closed=True)
                        .assign(Source='Closed')),
                ],
                axis=0,
            )
            .merge(
                (
                    self.excel_data_loader.load_ksic_data()
                     .assign(세세분류_코드=lambda x: x['세세분류_코드'].astype(float))
                ),
                left_on='IND_CD1',
                right_on='세세분류_코드',
                how='left',
            )
            .assign(Closed=lambda x: x['CLSBZ_GB'].apply(lambda y: True if y == 3 else False))
        )

        if kr_column_names is True:
            column_mapping = self._get_korean_column_name_dict(company_data)
            company_data = company_data.rename(columns=column_mapping)

        return company_data

    def _get_korean_column_name_dict(self, data: pd.DataFrame) -> List:
        column_intersection = list(
            set(self.meta_data['영문칼럼명'].tolist()).intersection(set(data.columns.tolist())))

        column_mapping = (
            self.meta_data.loc[lambda x: x['영문칼럼명'].isin(
                column_intersection), ['영문칼럼명', '한글칼럼명']]
            .set_index('영문칼럼명')
            .to_dict()['한글칼럼명']
        )

        return column_mapping

    def add_beaver_indicator(self, data: pd.DataFrame):
        return (
            data.assign(**{'유동자산/부채총계': lambda x: x['유동자산'] / x['부  채  총  계']})
            .assign(**{'당기순이익(손실)/자산총계': lambda x: x['당기순이익(손실)'] / x['자산총계']})
            .assign(**{'부채총계/자산총계': lambda x: x['부  채  총  계'] / x['자산총계']}) # 순운전자본=유동자산-유동부채
            .assign(**{'순운전자본/자산총계': lambda x: (x['유동자산'] - x['유동부채']) / x['자산총계']})
            .assign(**{'유동부채/유동자산': lambda x: x['유동부채'] / x['유동자산']})
        )

    def add_label(self, data: pd.DataFrame):
        return (
            data.assign(
                Years_From_Closed_Year_To_FS=lambda x: x['Closed_Year']
                - pd.to_datetime(x['결산년월'], format='%Y%m%d').dt.year
            )
            .assign(Closed_In_1Yr=lambda x: x['Years_From_Closed_Year_To_FS'].apply(lambda y: 1 if y <= 1 else 0))
            .assign(Closed_In_2Yrs=lambda x: x['Years_From_Closed_Year_To_FS'].apply(lambda y: 1 if y <= 2 else 0))
            .loc[lambda x : x['Years_From_Closed_Year_To_FS']>0]
        )
