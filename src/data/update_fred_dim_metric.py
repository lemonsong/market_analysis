from src.constants import DATABASE_DB, DATABASE_USER, DATABASE_PW, DATABASE_HOST, DATABASE_PORT
import src.utils.pg as pg
from src.utils.pg import PgClass
import pandas as pd

# connect to postgresql database
table_dest = 'fred.dim_metric'
pg_conn = PgClass(DATABASE_DB, DATABASE_USER, DATABASE_PW, DATABASE_HOST, DATABASE_PORT)

# create table
table_col_def = """
            id serial NOT NULL ,
            metric character varying(255) UNIQUE NOT NULL,
            title character varying ,
            frequency character varying(50),
            units character varying(50),
            seasonal_adjustment character varying(100),
            popularity smallint,
            source text,
            notes text,
            deprecated boolean,
            PRIMARY KEY (id, metric)
            """
pg_conn.create_table(table_dest, table_col_def)
#
# # insert records
# dim_metric_list = [
#     ("T10YFF",'D-missing','10-Year Treasury Constant Maturity Minus Federal Funds Rate', 'FRED', False),
# ]
# dim_metric_df = pd.DataFrame(
#     dim_metric_list,
#     columns=['metric', 'frequency', 'title', 'source', 'deprecated']
# )
# pg_conn.insert_data(table_dest, dim_metric_df)
# pg_conn.close_connection()
pg_conn.close_connection()

print('completed')

