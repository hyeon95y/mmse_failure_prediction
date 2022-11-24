import multiprocessing
import os
import time
from pathlib import Path
from typing import List, Union

import pandas as pd
from pytrends.request import TrendReq
from requests import get
from tqdm import tqdm


class GoogleTrendsDataLoader:
    def __init__(self, cached_path: Union[Path, str]):
        self.cached_path = cached_path
        if not os.path.exists(self.cached_path):
            os.mkdir(self.cached_path)

    def load_keywords(
        self,
        keywords: pd.DataFrame,
        max_retry: int = 5,
        retry_interval: int = 2,
        n_jobs: int = 1,
        use_vpn=False,
        vpn_max_retry=20,
        vpn_change_interval=60 * 60,
    ):
        n_jobs = multiprocessing.cpu_count() if n_jobs == -1 else n_jobs

        if n_jobs == 1:
            for idx in tqdm(keywords.index):
                keyword = keywords.loc[idx, 'keyword']
                geo = keywords.loc[idx, 'geo']
                timeframe = keywords.loc[idx, 'timeframe']

                success = False
                filepath = self._get_cached_filepath(keyword, timeframe, geo)

                if not os.path.exists(filepath):
                    for count in range(0, max_retry):
                        if success is False:
                            success, _ = self._load_keyword(
                                keyword, timeframe, geo)
                            time.sleep(retry_interval)
                else:
                    success = True

                keywords.loc[idx, 'success'] = success

        elif n_jobs != 1:
            if use_vpn is True:
                for i in range(0, vpn_max_retry):
                    keywords = keywords.sample(frac=1).assign(
                        max_retry=max_retry).assign(retry_interval=retry_interval)
                    args = keywords.values.tolist()
                    pool = multiprocessing.Pool(n_jobs)

                    pool.starmap_async(
                        self._load_keyword_with_multiprocessing, tqdm(args))
                    time.sleep(vpn_change_interval)
                    pool.terminate()
                    pool.close()
                    pool.join()

                    os.popen("expresso disconnect")
                    time.sleep(5)
                    os.popen("expresso connect")
                    time.sleep(5)
                    print(
                        f"IP has been changed to {get('https://api.ipify.org').content.decode('utf8')}")
            else:
                keywords = keywords.sample(frac=1).assign(
                    max_retry=max_retry).assign(retry_interval=retry_interval)
                args = keywords.values.tolist()
                pool = multiprocessing.Pool(n_jobs)

                pool.starmap_async(
                    self._load_keyword_with_multiprocessing, tqdm(args))
                pool.close()
                pool.join()

        return keywords

    def _load_keyword_with_multiprocessing(self, keyword, geo, timeframe, max_retry, retry_interval):

        filepath = self._get_cached_filepath(keyword, timeframe, geo)

        success = False
        if not os.path.exists(filepath):
            for count in range(0, max_retry):
                if success is False:
                    success, _ = self._load_keyword(keyword, timeframe, geo)
                    time.sleep(retry_interval)

        return

    def load_keyword(
        self, keyword: str, timeframe: Union[List, str], geo: str, max_retry: int = 10, retry_interval: int = 2
    ) -> pd.DataFrame:

        success = False

        for count in range(0, max_retry):
            if success is False:
                success, data = self._load_keyword(keyword, timeframe, geo)
                time.sleep(retry_interval)

        return data

    def _load_keyword(self, keyword: str, timeframe: Union[List, str], geo: str) -> pd.DataFrame:

        filepath = self._get_cached_filepath(keyword, timeframe, geo)

        if os.path.exists(filepath):
            data = self._load_cached_file(keyword, timeframe, geo)
            success = True
        else:
            try:
                pytrend = TrendReq()
                pytrend.build_payload(
                    kw_list=[keyword], timeframe=timeframe, geo=geo)
                data = pytrend.interest_over_time().drop(
                    ["isPartial"], axis=1, errors="ignore")
                success = True
            except Exception as e:
                print(filepath, e)
                success = False
                data = None

            if success is True:
                data.to_csv(filepath)

        return success, data

    def _get_cached_filepath(self, keyword: str, timeframe: Union[List, str], geo: str):

        if timeframe == 'today 5-y':
            start_date = pd.Timestamp.today() - pd.Timedelta(days=260 * 7)
            start_date = start_date - \
                pd.Timedelta(days=1 + start_date.weekday())

            end_date = pd.Timestamp.today()
            end_date = end_date - pd.Timedelta(days=7 + 3 - end_date.weekday())

            timeframe = f'{str(start_date.date())} {str(end_date.date())}'

        filepath = self.cached_path / f'{geo}_{keyword}_{timeframe}.csv'

        return filepath

    def _load_cached_file(self, keyword: str, timeframe: Union[List, str], geo: str) -> pd.DataFrame:
        filepath = self._get_cached_filepath(keyword, timeframe, geo)
        return pd.read_csv(filepath)
