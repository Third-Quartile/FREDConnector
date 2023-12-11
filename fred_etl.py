"""Extracts data from FRED API, creates tables if neccessary and inserts data into the tables"""
import logging
import os
import struct
from datetime import datetime, timedelta
import pyodbc
import pandas as pd
from fredapi import Fred
from azure import identity
import logging
import time

from dotenv import load_dotenv
if os.path.exists('.env'):
    load_dotenv('.env')
    APIKEY = os.getenv("APIKEY")
    SERVER = os.getenv("SERVER")
    PORT = os.getenv("PORT")
    if PORT:
        SERVER += f",{PORT}"
    DATABASE = os.getenv("DATABASE")
    DB_USER = os.getenv("DB_USER")
    PASSWORD = os.getenv("PASSWORD")
    SYNC_MODE = os.getenv("SYNC_MODE")
else:
    APIKEY = os.environ["APIKEY"]
    SERVER = os.environ["SERVER"]
    DATABASE = os.environ["DATABASE"]
    SYNC_MODE = os.environ["SYNC_MODE"]
    DB_USER = os.environ["DB_USER"]
    PASSWORD = os.environ["PASSWORD"]

TARGET_TABLE = "FinData"

SERIES_ID_LIST = [
    "MORTGAGE30US",
    "OBMMIJUMBO30YF",
    "MORTGAGE15US",
    "MDSP",
    "Q09084USQ507NNBR",
    "DTB3",
    "DTB6",
    "DGS1",
    "DGS2",
    "DGS3",
    "DGS5",
    "DGS7",
    "DGS10",
    "DGS20",
    "DGS30",
    "BAMLH0A0HYM2",
    "BAMLH0A0HYM2EY",
    "BAMLC0A0CM",
    "BAMLC0A4CBBBEY",
    "BAMLC0A4CBBB",
    "BAMLH0A1HYBB",
    "BAMLC0A0CMEY",
    "BAMLC0A2CAAEY",
    "BAMLC0A3CA",
    "BAMLC0A2CAA",
    "BAMLC1A0C13YEY",
    "BAMLC3A0C57YEY",
    "BAMLCC4A0710YTRIV",
    "BAMLC7A0C1015Y"
]

OBSERVATION_DATE  = datetime.now() - timedelta(days=1)

def get_connection():
    """Return a connection to a SQL Server DB"""
    driver_version = "{ODBC Driver 17 for SQL Server}"

    ########The commented code and connection_string are used to connect locally to the test enviroment
    #connection_string=f"""Driver={driver_version};Server=tcp:{SERVER};Database={DATABASE};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30"""
    #
    #credential = identity.DefaultAzureCredential(exclude_interactive_browser_credential=False)
    #token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
    #token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
    #SQL_COPT_SS_ACCESS_TOKEN = 1256  # This connection option is defined by microsoft in msodbcsql.h
    #conn = pyodbc.connect(connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
    ########
    
    connection_string = f"Driver={driver_version};Server=tcp:{SERVER};Database={DATABASE};Uid={DB_USER};Pwd={PASSWORD};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30"
    conn = pyodbc.connect(connection_string)
    
    return conn


def get_max_date(connection,target_table):
    query = f"select max(DATE) from {target_table}"

    with  connection.cursor() as cursor:
        cursor.execute(query)
        #connection.commit()
        max_date = cursor.fetchval()

    if max_date>(datetime.now() - timedelta(weeks=1)).date():
        return (datetime.now() - timedelta(weeks=1)).date()
    else:
        return max_date


def get_latest_data(connection,target_table):
    query = f"SELECT DATE, API_CODE FROM {target_table} WHERE DATE >= '{(datetime.now() - timedelta(weeks=1)).strftime('%Y-%m-%d')}'"

    with  connection.cursor() as cursor:
        cursor.execute(query)
        #connection.commit()
        rows  = cursor.fetchall()
        df_sql = pd.DataFrame.from_records(rows, columns=['DATE', 'API_CODE'])
    
    return df_sql


def get_data_from_fred(series_id_list:list, max_date:datetime, df_sql:pd.DataFrame, observation_date:datetime, sync_mode:str) -> list[dict]:
    """Return data from FRED API"""
    fred = Fred(api_key=APIKEY)
    full_df = pd.DataFrame()
    for series_id in series_id_list:
        if sync_mode=="full_load":
            data_series = fred.get_series(series_id)
        elif sync_mode=="incremental_load":
            data_series = fred.get_series(
                series_id,
                observation_start=max_date,
                observation_end=observation_date.date()
            )

        category_df = data_series.reset_index() #Here we extract the Date index as a new column
        category_df.columns = ['DATE', 'VALUE']
        category_df["API_CODE"] = series_id
        full_df = pd.concat([full_df, category_df], axis=0)

    full_df["ETL_LOADED_AT"] = datetime.now().date()
    full_df['DATE'] = full_df['DATE'].dt.date
    full_df = full_df.dropna(subset=['VALUE'])

    if sync_mode=="full_load":
        filtered_df = full_df[full_df['DATE'] <= observation_date.date()]
    elif sync_mode=="incremental_load":
        full_df_to_compare = full_df.apply(lambda row: f"{row['DATE']}_{row['API_CODE']}", axis=1)
        df_sql_to_compare = df_sql.apply(lambda row: f"{row['DATE']}_{row['API_CODE']}", axis=1)
        filtered_df = full_df[(full_df['DATE'] > max_date) & ~(full_df_to_compare.isin(df_sql_to_compare))]

    records = filtered_df.sort_values(by='DATE', ascending=True).to_dict("records")

    return records


def create_and_load_fin_master_table(connection:pyodbc.Connection) -> None:
    """Create FinMaster table and insert data if the table does not exists"""
    logging.info("\nStart creating table if not exists and loading data: FinMaster.")
    
    query = """
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'FinMaster')
    BEGIN
        CREATE TABLE FinMaster (
            ID INT,
            APICode VARCHAR(50),
            Description NVARCHAR(500)
        );

        INSERT INTO FinMaster (ID, APICode, Description)
        VALUES
            (1,	 'MORTGAGE30US',      '30-Year Fixed Rate Mortgage Average in the United States'),
            (2,	 'OBMMIJUMBO30YF',    '30-Year Fixed Rate Jumbo Mortgage Index'),
            (3,	 'MORTGAGE15US',      '15-Year Fixed Rate Mortgage Average in the United States'),
            (4,	 'MDSP',	          'Mortgage Debt Service Payments as a Percent of Disposable Personal Income'),
            (5,	 'Q09084USQ507NNBR',  'Mortgage Delinquency Rates for United States'),
            (6,	 'DTB3',	          '3-Month Treasury Bill Secondary Market Rate, Discount Basis'),
            (7,	 'DTB6',	          '6-Month Treasury Bill Secondary Market Rate, Discount Basis'),
            (8,	 'DGS1',	          'Market Yield on U.S. Treasury Securities at 1-Year Constant Maturity, Quoted on an Investment Basis'),
            (9,	 'DGS2',	          'Market Yield on U.S. Treasury Securities at 2-Year Constant Maturity, Quoted on an Investment Basis'),
            (10, 'DGS3',	          'Market Yield on U.S. Treasury Securities at 3-Year Constant Maturity, Quoted on an Investment Basis'),
            (11, 'DGS5',	          'Market Yield on U.S. Treasury Securities at 5-Year Constant Maturity, Quoted on an Investment Basis'),
            (12, 'DGS7',	          'Market Yield on U.S. Treasury Securities at 7-Year Constant Maturity, Quoted on an Investment Basis'),
            (13, 'DGS10',	          'Market Yield on U.S. Treasury Securities at 10-Year Constant Maturity, Quoted on an Investment Basis'),
            (14, 'DGS20',	          'Market Yield on U.S. Treasury Securities at 20-Year Constant Maturity, Quoted on an Investment Basis'),
            (15, 'DGS30',	          'Market Yield on U.S. Treasury Securities at 30-Year Constant Maturity, Quoted on an Investment Basis'),
            (16, 'BAMLH0A0HYM2',	  'ICE BofA US High Yield Index Option-Adjusted Spread'),
            (17, 'BAMLH0A0HYM2EY',	  'ICE BofA US High Yield Index Effective Yield'),
            (18, 'BAMLC0A0CM',	      'ICE BofA US Corporate Index Option-Adjusted Spread'),
            (19, 'BAMLC0A4CBBBEY',	  'ICE BofA BBB US Corporate Index Effective Yield'),
            (20, 'BAMLC0A4CBBB',	  'ICE BofA BBB US Corporate Index Option-Adjusted Spread'),
            (21, 'BAMLH0A1HYBB',	  'ICE BofA BB US High Yield Index Option-Adjusted Spread'),
            (22, 'BAMLC0A0CMEY',	  'ICE BofA US Corporate Index Effective Yield'),
            (23, 'BAMLC0A2CAAEY',	  'ICE BofA AA US Corporate Index Effective Yield'),
            (24, 'BAMLC0A3CA',	      'ICE BofA Single-A US Corporate Index Option-Adjusted Spread'),
            (25, 'BAMLC0A2CAA',	      'ICE BofA AA US Corporate Index Option-Adjusted Spread'),
            (26, 'BAMLC1A0C13YEY',	  'ICE BofA 1-3 Year US Corporate Index Effective Yield'),
            (27, 'BAMLC3A0C57YEY',	  'ICE BofA 5-7 Year US Corporate Index Effective Yield'),
            (28, 'BAMLCC4A0710YTRIV', 'ICE BofA 7-10 Year US Corporate Index Total Return Index Value'),
            (29, 'BAMLC7A0C1015Y',	  'ICE BofA 10-15 Year US Corporate Index Option-Adjusted Spread');
    END;
    """

    with  connection.cursor() as cursor:
        cursor.execute(query)
    connection.commit()


def create_table_if_not_exists(target_table:str, connection:pyodbc.Connection) -> None:
    """Creates FinData table if not exists"""
    logging.info(f"\nStart creating table if not exists: {target_table}.")

    create_table_if_not_exists_query = f"""
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{target_table}')
    BEGIN
        CREATE TABLE {target_table} (
            ID INT IDENTITY(1,1),
            DATE DATE,
            VALUE FLOAT,
            API_CODE NVARCHAR(255),
            ETL_LOADED_AT DATE
        )
    END;
    """

    with  connection.cursor() as cursor:
        cursor.execute(create_table_if_not_exists_query)
    connection.commit()


def truncate_table(target_table:str, connection:pyodbc.Connection) -> None:
    """Truncates FinData table when sync_mode=='full_load'"""
    logging.info(f"\nStart truncating table: {target_table}.")
    
    truncate_table_query = f"""TRUNCATE TABLE {target_table}"""

    with  connection.cursor() as cursor:
        cursor.execute(truncate_table_query)
    connection.commit()


def insert_data(records:list[dict], connection:pyodbc.Connection, target_table:str) -> None:
    """Insert data into table FinData"""
    logging.info(f"\nStart insertig data into temp table: {target_table}_Temp.")

    data = [(record['DATE'], record['VALUE'], record['API_CODE'], record['ETL_LOADED_AT']) for record in records]
    insert_query = f"INSERT INTO {target_table} (DATE, VALUE, API_CODE, ETL_LOADED_AT) VALUES (?, ?, ?, ?)"

    attempt = 1
    while attempt<4:
        try:
            with  connection.cursor() as cursor:
                cursor.fast_executemany = True
                cursor.executemany(insert_query, data)
            connection.commit()
            break
        except Exception as e:
            logging.info(f"Insert failed on attempt {attempt}")
            logging.info(e)
            logging.info("Sleep 20 seconds")
            time.sleep(20)
            attempt += 1


def fred_main() -> None:
    """Extracts data from FRED API, creates tables if neccessary and inserts data into the tables"""

    logging.info(f"OBSERVATION_DATE: {OBSERVATION_DATE}")

    attempt = 1
    while attempt<4:
        try:
            connection = get_connection()
            break
        except Exception as e:
            logging.info(f"Failed on attempt {attempt}")
            logging.info(e)
            logging.info("Sleep 60 seconds")
            time.sleep(60*2**attempt)
            attempt += 1

    create_table_if_not_exists(TARGET_TABLE, connection)

    max_date = get_max_date(connection,TARGET_TABLE)

    df_sql = get_latest_data(connection,TARGET_TABLE)

    logging.info(f"start extraction from {str(max_date)}")

    records = get_data_from_fred(SERIES_ID_LIST, max_date, df_sql, OBSERVATION_DATE, SYNC_MODE)

    logging.info(f"\nData from fred retrieved: {len(records)} records.")
    if len(records) > 0:

        create_and_load_fin_master_table(connection)

        if SYNC_MODE=="full_load":
            truncate_table(TARGET_TABLE, connection)

        chunk_size = 1000
        list_of_lists = [records[x:x+chunk_size] for x in range(0, len(records), chunk_size)]
        start_datetime = datetime.now()
        for n_batch, batch in enumerate(list_of_lists):
            start_batch = datetime.now()
            insert_data(batch, connection, TARGET_TABLE)
            logging.info(f"Finish batch {n_batch}, in {(datetime.now() - start_batch).seconds} seconds of {(datetime.now() - start_datetime).seconds} seconds")

    connection.close()


if __name__ == "__main__":
    fred_main()
