import sqlite3

class SQLite3database:
    def __init__(self, db_file):
        self.connection = None
        try:
            self.connection = sqlite3.connect(db_file)
        except Error as e:
            print(e)

    def df_to_sql(self, df, table_name):
        df.to_sql(table_name, self.connection, if_exists='replace', index=False)

    def execute_query(self, sql_query):
        try:
            c = self.connection.cursor()
            c.execute(sql_query)
        except Error as e:
            print(e)