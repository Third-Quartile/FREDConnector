import openpyxl
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import sqlalchemy
import urllib
from fuzzywuzzy import process

# Constants
excel_path = r"Income_Statement_40501_Accrual.xlsx"
sheet_name = "Report1"
EXCLUDE_VALUES = ['50000', '50001', '50010', '50012', '50039', '50199', '53000', 
                  '53499', '57999', '60001', '60010', '60499', '60600', '60699', 
                  '61000', '61010', '61299', '61300', '61599', '61699', '61800', 
                  '62199', '62400', '63299', '63400', '63699', '64000', '64299', 
                  '64500', '64699', '79990', '79999', '80000', '80199', '80300', 
                  '80599', '80600', '80699', '80700', '82999', '98998']

def load_excel_sheet(excel_path, sheet_name):
    wb = openpyxl.load_workbook(excel_path)
    return wb[sheet_name]

def extract_head_data(sheet):
    property_value = sheet['A1'].value
    report_type_value = sheet['A2'].value
    date_value = sheet['A3'].value.split('=')[1].strip()
    end_of_month = datetime.strptime(date_value, '%b %Y') + relativedelta(months=1) - timedelta(days=1)
    end_of_month_str = end_of_month.strftime('%m/%d/%Y')
    report_detail_value = sheet['A4'].value
    return property_value, report_type_value, end_of_month_str, report_detail_value

def get_matching_id(deal_name, deal_dict):
    """Find a matching deal ID based on deal name using fuzzy matching."""
    threshold = 65  # Adjust the threshold as needed
    
    match = process.extractOne(deal_name, deal_dict.keys())
    if match[1] >= threshold:
        return deal_dict[match[0]]
    return None

def main():
    #database connection variables
    server_name = "q3solutions-dev.database.windows.net"
    databasename = "PropertyManager-dev"
    username = "q3solutions-dev"
    password = "Q3testMcQ"

    # Using urllib.parse.quote_plus to create connection parameters

    params = urllib.parse.quote_plus(f'DRIVER={{ODBC Driver 17 for SQL Server}};'
                                    f'SERVER={server_name};'
                                    f'DATABASE={databasename};'
                                    f'UID={username};'
                                    f'PWD={password}')
    
    # Create a connection to the SQL Server database using SQLAlchemy and PyODBC
    engine = sqlalchemy.create_engine(f'mssql+pyodbc:///?odbc_connect={params}')

    # Load Excel sheet and extract header data
    sheet = load_excel_sheet(excel_path, sheet_name)
    property_value, report_type_value, end_of_month_str, report_detail_value = extract_head_data(sheet)

    # Open a connection to the database and load data from the 'UW_Deals' table
    with engine.connect() as connection:
        df_deals = pd.read_sql('SELECT * FROM UW_Deals', connection)
    deal_dict = df_deals.set_index('DealName')['DealID'].to_dict()

    # Parsing Income Statement
    df_is = pd.DataFrame(sheet.values)
    df_is = df_is.dropna(how='all')
    df_is = df_is.iloc[4:]
    df_is.columns = df_is.iloc[0]
    df_is = df_is.iloc[1:]

    # Rename columns
    df_is.columns = ['Codes', 'CodeName', 'Period_to_Date', 'Perc_Period_to_Date', 'Year_to_Date', 'Perc_Year_to_Date']

    # Drop unnecessary columns
    df_is = df_is.drop(columns=['Perc_Period_to_Date', 'Year_to_Date', 'Perc_Year_to_Date'])

    # Insert Deal and Date columns
    df_is.insert(0, 'DealID', property_value)
    df_is['DealID'] = df_is['DealID'].apply(lambda x: get_matching_id(x, deal_dict))
    df_is.insert(1, 'Date', end_of_month_str)

    # Exclude rows with specified values in column 1
    df_is = df_is[~df_is['Codes'].isin(EXCLUDE_VALUES)]

    # Trim out spaces to the left of the text in the CodeName field
    df_is['CodeName'] = df_is['CodeName'].str.lstrip()

    with engine.connect() as connection:
        df_is_level4 = pd.read_sql('SELECT ID, Level3ID FROM IS_Level4', connection)
        df_is_level3 = pd.read_sql('SELECT ID, Level2ID FROM IS_Level3', connection)
        df_is_level2 = pd.read_sql('SELECT ID, Level1ID FROM IS_Level2', connection)

    # Set the correct datatypes before merging
    df_is['Codes'] = df_is['Codes'].astype(str)
    df_is_level4['ID'] = df_is_level4['ID'].astype(str)
    df_is_level3['ID'] = df_is_level3['ID'].astype(str)
    df_is_level2['ID'] = df_is_level2['ID'].astype(str)
    df_is_level4['Level3ID'] = df_is_level4['Level3ID'].astype(str)
    df_is_level3['Level2ID'] = df_is_level3['Level2ID'].astype(str)
    df_is_level2['Level1ID'] = df_is_level2['Level1ID'].astype(str)

    # Merge dataframes
    df_is = df_is.merge(df_is_level4, how='left', left_on='Codes', right_on='ID')
    df_is = df_is.merge(df_is_level3, how='left', left_on='Level3ID', right_on='ID')
    df_is = df_is.merge(df_is_level2, how='left', left_on='Level2ID', right_on='ID')

    # Rearrange the columns to the required order
    df_is = df_is[['DealID', 'Date', 'Level1ID', 'Level2ID', 'Level3ID', 'Codes', 'CodeName', 'Period_to_Date']]

    # Define the table name for data insertion
    table_name = 'RA_IncomeStatement'
    print(df_is)

    # try:
    #     # Insert data into the SQL table
    #     df_is.to_sql(table_name, engine, if_exists='append', index=False)
    #
    #     print("Data successfully inserted into the SQL table.")
    # except Exception as e:
    #     print(f"Error inserting data into SQL table: {e}")

if __name__ == "__main__":
    main()
