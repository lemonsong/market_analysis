from fredapi import Fred
from src.constants import FRED_API, QUANDL_API, DATABASE_DB, DATABASE_USER, DATABASE_PW, DATABASE_HOST, DATABASE_PORT
import src.utils.pg as pg
import sys

fred = Fred(api_key=FRED_API)
# data = fred.get_series_latest_release('GDP')
# print(data.tail())

search_df = fred.search("Delinquency Rate MORTGAGE")
search_df['popularity'] = search_df['popularity'].astype(int)
search_df = search_df.sort_values(by=['popularity'], ascending=False)
print(search_df.head())
sys.exit()

# connect to postgresql database
pg_connection, pg_cursor = pg.create_connection(DATABASE_DB, DATABASE_USER, DATABASE_PW, DATABASE_HOST, DATABASE_PORT)

# drop table
pg_cursor.execute("""DROP TABLE IF EXISTS fred.dim_metric""")
# create table
create_table_q = """
CREATE TABLE IF NOT EXISTS fred.dim_metric
(
    id serial NOT NULL PRIMARY KEY,
    abbreviation character varying(255) COLLATE pg_catalog."default" NOT NULL,
    frequency character varying(50) COLLATE pg_catalog."default",
    metric character varying COLLATE pg_catalog."default",
    source text COLLATE pg_catalog."default",
    deprecated boolean
)
"""
pg_cursor.execute(create_table_q)

# insert records
dim_metric = [
    ("T10YFF",'D-missing','10-Year Treasury Constant Maturity Minus Federal Funds Rate', 'FRED', False),
]
dim_metric_records = ", ".join(["%s"] * len(dim_metric))
insert_data_q = (
    f"INSERT INTO fred.dim_metric (abbreviation, frequency, metric, source, deprecated) VALUES {dim_metric_records}"
)
pg_cursor.execute(insert_data_q, dim_metric)

pg_cursor.execute("SELECT * FROM fred.dim_metric;")
print(pg_cursor.fetchone())


pg_connection.commit()
pg_cursor.close()
pg_connection.close()
print('completed')

