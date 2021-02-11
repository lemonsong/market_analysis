from src.constants import project_dir, FRED_API, QUANDL_API, DATABASE_DB, DATABASE_USER, DATABASE_PW, DATABASE_HOST, DATABASE_PORT
import sys, os
import pandas as pd
from src.utils.pg import PgClass
from src.utils.yahoofinance import YfClass


# collect fred data
stock_price_list = ['JPM','GS', 'C', 'NIO', 'TSM', 'NVDA',
                    'VOX', 'VCR', 'VDC', 'VDE', 'VFH', 'VHT', 'VIS', 'VGT', 'VAW', 'VNQ', 'VPU',
                    'QQQ', 'VOO', 'VO', 'VB']
period = 'max' # when use 1d, it'll generate duplicate records, so don't use 1d
from_api = True
recreate_table = False

if from_api:
    # get ticker prices from yahoo finance api
    yahoofin = YfClass(stock_price_list)
    stock_price_df = yahoofin.get_tickers_history(period)
    stock_price_df.to_csv(os.path.join(project_dir, 'data/raw/yahoofinance_stock_price_df.csv'), encoding='utf-8', index=False)
else:
    # read data from local
    stock_price_df = pd.read_csv(os.path.join(project_dir, 'data/raw/yahoofinance_stock_price_df.csv'))


# connect to postgresql database
table_dest = 'stock.fact_stock_price'
pg_conn = PgClass(DATABASE_DB, DATABASE_USER, DATABASE_PW, DATABASE_HOST, DATABASE_PORT)

# ###############
# create table #
# ###############
if recreate_table:
    table_col_def = """
        ticker character varying(10) NOT NULL,
        activity_date date NOT NULL,
        open numeric,
        high numeric,
        low numeric,
        close numeric,
        volume numeric,
        dividends numeric,
        stock_splits numeric,
        PRIMARY KEY (ticker, activity_date)
                """
    pg_conn.create_table(table_dest, table_col_def)

# ################
# # insert records
# ################

# pg_conn.insert_data(table_dest, fred_metrics_df)

################
# upsert data
################
pg_conn.insert_data(table_dest, stock_price_df, """ON CONFLICT (ticker, activity_date)
                                                 DO UPDATE SET open = EXCLUDED.open,
                                                 high = EXCLUDED.high,
                                                 low = EXCLUDED.low,
                                                 volume = EXCLUDED.volume,
                                                 dividends = EXCLUDED.dividends,
                                                 stock_splits = EXCLUDED.stock_splits
                                                 """)
pg_conn.close_connection()


print('completed')




