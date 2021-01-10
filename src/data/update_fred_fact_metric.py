from src.constants import project_dir, FRED_API, QUANDL_API, DATABASE_DB, DATABASE_USER, DATABASE_PW, DATABASE_HOST, DATABASE_PORT
import sys, os
import pandas as pd
from src.utils.fred import FredClass
from src.utils.pg import PgClass


# collect fred data
fred_metric_list = ['T10YFF', 'DGS10','SP500', 'M2','UMCSENT',
             'PERMIT', 'NEWY636BPPRIV', 'NYBPPRIVSA',
             'ANDENO', 'ACOGNO', 'AMTMNO', 'ACDGNO', 'DGORDER',
             'AWHMAN', 'AWHAETP', 'AWHAEMAN',
             'DTCDISA066MSFRBNY',
             'ICSA', 'NYICLAIMS', 'NJICLAIMS',
             'MDSP', 'DRSFRMACBS', 'M0264AUSM500NNBR']
# fred_metric_list = ['GDP']
start_date_dt = pd.to_datetime('1980-01-01')
from_api=True
if from_api:
    fred = FredClass(FRED_API)
    # get metric info
    fred_metrics_info_df = fred.get_metrics_info(fred_metric_list)
    fred_metrics_info_df.to_csv(os.path.join(project_dir, 'data/raw/fred_metrics_info_df.csv'), encoding='utf-8', index=False)
    # # get data
    fred_metrics_df = fred.fetch(metrics=fred_metric_list,start_date=start_date_dt)
    fred_metrics_df.to_csv(os.path.join(project_dir, 'data/raw/fred.csv'), encoding='utf-8', index=False)
else:
    fred_metrics_info_df = pd.read_csv(os.path.join(project_dir, 'data/raw/fred_metrics_info_df.csv'))
    fred_metrics_df = pd.read_csv(os.path.join(project_dir, 'data/raw/fred.csv'))


# connect to postgresql database
table_dest = 'fred.fact_metric'
pg_conn = PgClass(DATABASE_DB, DATABASE_USER, DATABASE_PW, DATABASE_HOST, DATABASE_PORT)

# # ###############
# # create table #
# # ###############
# table_col_def = """
#     metric character varying(255) NOT NULL,
#     activity_date date NOT NULL,
#     value numeric,
#     PRIMARY KEY (metric, activity_date)
#
#             """
# pg_conn.create_table(table_dest, table_col_def)

# ################
# # insert records
# ################

# pg_conn.insert_data(table_dest, fred_metrics_df)

################
# rewrite data
################
pg_conn.insert_data(table_dest, fred_metrics_df, """ON CONFLICT (metric, activity_date)
                                                 DO UPDATE SET value = EXCLUDED.value""")
pg_conn.insert_data('fred.dim_metric', fred_metrics_info_df, """ON CONFLICT (metric)
                                                 DO UPDATE SET title = EXCLUDED.title,
                                                 frequency = EXCLUDED.frequency,
                                                 units = EXCLUDED.units,
                                                 seasonal_adjustment = EXCLUDED.seasonal_adjustment,
                                                 popularity = EXCLUDED.popularity,
                                                 source = EXCLUDED.source,
                                                 notes = EXCLUDED.notes,
                                                 deprecated = EXCLUDED.deprecated""")

# pg_conn.rewrite_data_union(table_dest, fred_metrics_df, ['metric', 'activity_date'])
# pg_conn.rewrite_data_union('fred.dim_metric', fred_metrics_info_df, ['metric'])

pg_conn.close_connection()


print('completed')




