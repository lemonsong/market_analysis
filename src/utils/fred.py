from fredapi import Fred
import pandas as pd
from src.constants import FRED_API
import sys


class FredClass:
    def __init__(self, FRED_API):
        self.fred = Fred(api_key=FRED_API)

    def search(self, search_text):
        self.search_df = self.fred.search(search_text)
        self.search_df['popularity'] = self.search_df['popularity'].astype(int)
        self.search_df = self.search_df.sort_values(by=['popularity'], ascending=False)
        return self.search_df

    def fetch(self, metrics, start_date):
        self.metrics_df = pd.DataFrame()
        for metric in metrics:
            print(f'Fetch metric: {metric}')
            try:
                metrics_df_sub = self.fred.get_series_latest_release(metric).to_frame()
                # TODO: append first then reset_index?
                metrics_df_sub.reset_index(drop=False, inplace=True)
                metrics_df_sub.columns = ['activity_date', 'value']
                metrics_df_sub['activity_date'] = metrics_df_sub.activity_date.dt.date
                metrics_df_sub['metric'] = metric
                print(metrics_df_sub.dtypes)
                self.metrics_df = self.metrics_df.append(metrics_df_sub)
            except ValueError:
                print(f'Skip metric: {metric}')
                continue
        self.metrics_df = self.metrics_df.loc[self.metrics_df.activity_date >= start_date]
        self.metrics_df = self.metrics_df[['metric', 'activity_date', 'value']]
        return self.metrics_df

    def get_metrics_info(self,metrics):
        info_df = pd.DataFrame()
        for metric in metrics:
            info_df_sub = self.fred.get_series_info(metric)
            info_df = info_df.append(info_df_sub, ignore_index=True)
        info_df = info_df[[
                'id','title','frequency','units','seasonal_adjustment','popularity','notes'
            ]]
        info_df = info_df.rename(columns={'id':'metric'})
        info_df['source'] = 'FRED'
        info_df['deprecated'] = False
        return info_df
