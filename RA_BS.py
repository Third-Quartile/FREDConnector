import openpyxl
import pandas as pd
from datetime import datetime, timedelta
from fuzzywuzzy import fuzz, process
from dateutil.relativedelta import relativedelta
import cProfile
import sqlalchemy
import urllib
import pyodbc


def open_excel_file(file_path, sheet_name):
    """Open an Excel file and return the specified sheet."""
    wb = openpyxl.load_workbook(file_path, data_only=True)
    return wb[sheet_name]


def extract_info_from_sheet(sheet):
    """Extract information from the Excel sheet."""
    header = [cell.value for row in sheet['A1:A4'] for cell in row]

    if len(header) < 4:
        raise ValueError("Header information missing in the sheet.")

    property_value = header[0]
    report_type_value = header[1]
    date_value = header[2].split('=')[1].strip()

    try:
        end_of_month = datetime.strptime(date_value, '%b %Y') + relativedelta(months=1) - timedelta(days=1)
        end_of_month_str = end_of_month.strftime('%m/%d/%Y')
    except ValueError:
        raise ValueError("Invalid date format in the sheet header.")

    report_detail_value = header[3]

    return {
        'DealID': property_value,
        'report_type': report_type_value,
        'date': end_of_month_str,
        'report_detail': report_detail_value
    }


def load_deal_data(engine):
    """Load deal data from an SQL Connection."""
    with engine.connect() as connection:
        df_deals = pd.read_sql('SELECT * FROM UW_Deals', connection)
    return df_deals


def get_matching_id(deal_name, deal_dict):
    """Find a matching deal ID based on deal name using fuzzy matching."""
    threshold = 65  # Adjust the threshold as needed

    match = process.extractOne(deal_name, deal_dict.keys())
    if match[1] >= threshold:
        return deal_dict[match[0]]
    return None


def preprocess_dataframe(df):
    """Preprocess the DataFrame."""
    # Slice and rename columns
    df = df.iloc[6:-2]  # Exclude irrelevant rows and columns
    df = df.drop(df.columns[3], axis=1)  # Drop the "Beginning Balance" column
    df['Duplicated_Column'] = df.iloc[:,
                              -1]  # duplicate the "Net Change" column to set up the debits and credits column
    df.columns = ['Codes', 'CodeName', 'Forward_Balance', 'Debit', 'Credit']  # Rename the columns
    df = df.dropna(subset=['Codes'])

    # Convert 'Codes' column to integers
    df['Codes'] = df['Codes'].astype(int)

    # Drop rows with codes and values that should be excluded
    exclude_values = [10000, 10010, 10011, 10299, 11000, 11199, 12000,
                      12199, 12299, 13000, 13999, 14000, 14499, 19999,
                      20000, 20001, 20010, 20599, 20600, 20899, 29999,
                      30000, 30399, 39999, 12290]
    df = df[~df['Codes'].isin(exclude_values)]

    df['Codes'] = df['Codes'].fillna(0).astype(int)

    # Drop rows where 'Codes' is equal to 0
    df = df[df['Codes'] != 0]

    # Trim out spaces to the left of the text in the CodeName field
    df['CodeName'] = df['CodeName'].str.lstrip()

    return df


def set_debits_and_credits(df):
    """Set up debits and credits."""
    df.loc[(df['Debit'] < 0) & (df['Codes'] < 20000), 'Debit'] = 0
    df.loc[(df['Credit'] > 0) & (df['Codes'] < 20000), 'Credit'] = 0
    df.loc[(df['Debit'] > 0) & (df['Codes'] > 20000), 'Debit'] = 0
    df.loc[(df['Credit'] < 0) & (df['Codes'] > 20000), 'Credit'] = 0

    return df


def main():
    excel_path = r"Balance_Sheet_PeriodChange_40501_Accrual.xlsx"
    sheet_name = "Report1"

    #database connection variables
    server_name = "q3solutions-dev.database.windows.net"
    database_name = "PropertyManager-dev"
    username = "q3solutions-dev"
    password = "Q3testMcQ"


    # Using urllib.parse.quote_plus to create connection parameter
    params = urllib.parse.quote_plus(f'DRIVER={{ODBC Driver 17 for SQL Server}};'
                                     f'SERVER={server_name};'
                                     f'DATABASE={database_name};'
                                     f'UID={username};'
                                     f'PWD={password}')

    # Create a connection to the SQL Server database using SQLAlchemy and PyODBC
    engine = sqlalchemy.create_engine(f'mssql+pyodbc:///?odbc_connect={params}')

    try:
        sheet = open_excel_file(excel_path, sheet_name)
        info = extract_info_from_sheet(sheet)
    except Exception as e:
        print(f"Error: {e}")
        return

    try:
        df_deals = load_deal_data(engine)
    except Exception as e:
        print(f"Error loading deal data: {e}")
        return

    deal_dict = dict(zip(df_deals['DealName'], df_deals['DealID']))

    df_tb = pd.read_excel(excel_path, sheet_name=sheet_name, skiprows=4)

    try:
        df_tb = preprocess_dataframe(df_tb)
    except Exception as e:
        print(f"Error preprocessing DataFrame: {e}")
        return

    df_tb = set_debits_and_credits(df_tb)

    # Add 'Date' and 'DealID' columns
    df_tb['Date'] = info['date']
    df_tb['DealID'] = info['DealID']
    df_tb['DealID'] = df_tb['DealID'].apply(get_matching_id, args=(deal_dict,))

    columns_to_check = ['Forward_Balance', 'Debit', 'Credit']
    df_tb = df_tb.dropna(subset=columns_to_check, how='all')

    # Set up the dataframe so the Debit column is positive and the Credit column is negative
    df_tb['Debit'] = df_tb['Debit'].abs()
    df_tb['Credit'] = df_tb['Credit'].abs()
    df_tb['Credit'] = df_tb['Credit'] * -1

    # Melt the DataFrame to transform it into a normalized format
    df_norm = pd.melt(df_tb, id_vars=['Date', 'DealID', 'Codes', 'CodeName'],
                        value_vars=['Forward_Balance', 'Debit', 'Credit'],
                        var_name='Balance_Type', value_name='Amount')

    # Remove rows where 'Amount' is 0
    df_norm = df_norm.query('Amount != 0')

    # Reorder the columns
    df_norm = df_norm[['DealID', 'Date', 'Balance_Type', 'Codes', 'CodeName', 'Amount']]

    # Insert data into an SQL table (replace 'your_table_name' with the actual table name)
    table_name = 'RA_BalanceSheet'
    print(df_norm)
    # try:
    #     df_norm.to_sql(table_name, engine, if_exists='append', index=False)
    #     print("Data successfully inserted into the SQL table.")
    # except Exception as e:
    #     print(f"Error inserting data into SQL table: {e}")


if __name__ == "__main__":
        # cProfile.run("main()", sort="cumulative")
        main()