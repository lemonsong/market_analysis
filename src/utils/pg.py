import psycopg2
from psycopg2 import OperationalError


class PgClass:
    def __init__(self, db_name, db_user, db_password, db_host, db_port):
        self.connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        self.cursor = self.connection.cursor()

    def create_table(self, table_dest, table_col_def, autocommit=True):
        self.connection.autocommit = autocommit
        # drop table
        self.cursor.execute(f"""DROP TABLE IF EXISTS {table_dest};""")
        # create table
        create_table_q = f"""
        CREATE TABLE IF NOT EXISTS {table_dest}
        (
            {table_col_def}
        )
        """
        self.cursor.execute(create_table_q)
        print(f'Table created: {table_dest}')
        return

    def insert_data(self, table_dest, data_df, conflict_str=None, autocommit=True):
        self.connection.autocommit = autocommit
        # covert dataframe to list of tuples
        data_list = list(data_df.itertuples(index=False))
        # convert column list to string separated by comma without quote
        table_cols = ', '.join(map(str, data_df.columns.tolist()))
        # insert records
        data_list_records = ", ".join(["%s"] * len(data_list))
        insert_data_q = (
            f"INSERT INTO {table_dest} ({table_cols}) VALUES {data_list_records} {conflict_str}"

        )
        self.cursor.execute(insert_data_q, data_list)
        # check sample data
        self.cursor.execute(f"SELECT * FROM {table_dest} LIMIT 5;")
        print(f'Data inserted: {self.cursor.fetchone()}')

    def delete_data(self, table_dest, del_dict, autocommit=True):
        self.connection.autocommit = autocommit
        # create the condition part of SQL for data to delete
        del_condition = ''
        for key, value in del_dict.items():
            del_condition = del_condition + ' AND '
            del_condition = del_condition + key + ' IN (' + ', '.join(map("'{0}'".format, value)) + ')'
        # because we have ' AND ' at the beginning of del_condition
        del_condition = del_condition[5:]
        # construct delete query
        del_q = (
            f"""DELETE FROM {table_dest} WHERE {del_condition}"""
        )
        self.cursor.execute(del_q)

    def rewrite_data_union(self, table_dest, data_df, del_cols, autocommit=True):
        self.connection.autocommit = autocommit
        # automatically create the dictionary that define which records need to delete
        del_dict = {k:list(data_df[k].unique()) for k in del_cols}
        self.delete_data(table_dest, del_dict, autocommit)
        self.insert_data(table_dest, data_df, autocommit=autocommit)

    def close_connection(self):
        self.cursor.close()
        self.connection.close()
