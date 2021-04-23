import pymysql
from sqlalchemy import create_engine 
import pandas as pd 
import mysql_auth

NYXLDB_login = mysql_auth.NYXLDB_ESD_GameInfo # import NYXLDB_login auth info

class MySQLConnection():
    def __init__(self, input_df=None, login_info=NYXLDB_login):
        dbname = login_info['dbname']
        hostname = login_info['hostname']
        username = login_info['username']
        pwd = login_info['pwd']
        port = login_info['port']

        if input_df is None: 
            pass 

        # define input_df
        self.input_df = input_df
        # dbname
        self.dbname = dbname
        # create engine
        self.engine = create_engine('mysql+pymysql://' + username + ':' + pwd + '@' + hostname + ':' + str(port) + '/' + dbname , echo=False)

    def export_to_db(self, table_name, if_exists='replace'):
        table_name = table_name.lower() # MySQL DB에서 table 이름을 자동으로 소문자로 바꿔주기 때문에 'replace' 기능 쓰려면 필수
        self.input_df.to_sql(name=table_name, con=self.engine, schema=self.dbname, if_exists=if_exists) # if_exsits:{'fail', 'replace', 'append'}

    def get_table_names(self):
        table_names = self.engine.table_names()
        
        return table_names

    def read_table_as_df(self, table_name):
        table_df = pd.read_sql(
            sql=f"SELECT * FROM `{table_name}`",
            con=self.engine,
        )
        if 'index' in table_df.columns.tolist():
            table_df.drop(columns='index', inplace=True) # drop 'index' column
        elif 'level_0' in table_df.columns.tolist():
            table_df.drop(columns='level_0', inplace=True) # drop 'level_0 column

        return table_df

    def import_db_by_sql(self, sql):
        import_df = pd.read_sql(sql=sql, con=self.engine)
        if 'index' in import_df.columns.tolist():
            import_df.drop(columns='index', inplace=True) # drop 'index' column
        elif 'level_0' in import_df.columns.tolist():
            import_df.drop(columns='level_0', inplace=True) # drop 'level_0 column

        return import_df
