import redshift_connector
import configparser
import os


configParser = configparser.RawConfigParser()
config_path = r'C:\Users\xiaolinzheng\creds.ini'

print(config_path )

    
configParser.read(config_path)

print(configParser)




    
configParser.read(config_path)


def get_conn(source='prd-gallium-redshift'):

    rs_credentials = dict(configParser.items(source))
    
    conn = redshift_connector.connect(
        iam=True,
        ssl=True,
        host=rs_credentials['host'],
        port=5439,
        database='prd',
        db_user='',
        cluster_identifier=rs_credentials['cluster_identifier'],
        region='eu-west-1',
        login_url=rs_credentials['login_url'],
        credentials_provider='BrowserSamlCredentialsProvider',
        user='',
        password=''
    )
    conn.rollback()
    conn.autocommit = True
    return conn
    
def sql_2_execute(sql_stat, conn):
    c = conn.cursor()
    c.execute(sql_stat)
    return None

def sql_2_df(sql_stat, conn):
    c = conn.cursor()
    c.execute(sql_stat)
    result= c.fetch_dataframe()
    return result
    
def create_sql_list(sql_stat):
    temp_sql_list = [s.strip() for s in sql_stat.split(';')]
    temp_sql_list = list(filter(None, temp_sql_list))
    return temp_sql_list
    