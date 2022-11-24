import pandas as pd
from project_paths import DATA_PATH

class BeaversModel:
    def __init__(self):
        threshold_path = DATA_PATH/'raw'/'Beavers_model_threshold.csv'
        self.threshold = (
            pd.read_csv(threshold_path, index_col=0, names=range(1,6))
            .iloc[2:]
        )
        self.beaver_features_naming_dict = {
            '유동자산/부채총계':'Cash flow/Total debt',
            '당기순이익(손실)/자산총계':'Net income/Total assets',
            '부채총계/자산총계':'Total debt/Total assets',
            '순운전자본/자산총계':'Working capital/Total assets',
            '유동부채/유동자산':'Current ratio',
        }
        self.beaver_features = [
            '유동자산/부채총계',
            '당기순이익(손실)/자산총계',
            '부채총계/자산총계',
            '순운전자본/자산총계',
            '유동부채/유동자산'
        ]
    
    def predict(self, data:pd.DataFrame):
        
        return
    
    def _get_threshold(years_to_close:int, feature_name_kr:str):
        feature_name_en = self.beaver_features_naming_dict[feature_name_kr]
        return self.threshold.loc[feature_name_en, years_to_close]