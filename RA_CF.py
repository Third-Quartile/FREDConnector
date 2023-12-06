# Import necessary libraries
import openpyxl
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import urllib
from fuzzywuzzy import process
from dateutil.relativedelta import relativedelta

# Step 1: Load and extract header information from the Excel sheet
def load_excel_data(excel_path, sheet_name):
    wb = openpyxl.load_workbook(excel_path)
    sheet = wb[sheet_name]

    property_value = sheet['A1'].value
    report_type_value = sheet['A2'].value
    date_value = sheet['A3'].value.split('=')[1].strip()
    end_of_month = datetime.strptime(date_value, '%b %Y') + relativedelta(months=1) - timedelta(days=1)
    end_of_month_str = end_of_month.strftime('%m/%d/%Y')
    report_detail_value = sheet['A4'].value

    df_head = pd.DataFrame({
        'property': [property_value],
        'report_type': [report_type_value],
        'date': [end_of_month_str],
        'report_detail': [report_detail_value]
    })

    return df_head, end_of_month_str

# Step 2: Setup SQL Connection
def setup_sql_connection():
    params = urllib.parse.quote_plus(r'DRIVER={ODBC Driver 17 for SQL Server};'
                                     f'SERVER=q3solutions-dev.database.windows.net;'
                                     r'DATABASE=PropertyManager-dev;'
                                     r'UID=q3solutions-dev;'
                                     r'PWD=Q3testMcQ')
    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')
    return engine

# Step 3: Get matching ID for deals
def get_matching_id(deal_name, deal_dict):
    match = process.extractOne(deal_name, deal_dict.keys())
    if match[1] > 65:  # We set a threshold for matching to avoid wrong matches
        return deal_dict[match[0]]
    return None

# Step 4: Process the Excel data
def process_excel_data(excel_path, sheet_name, property_value, end_of_month_str, engine):
    df = pd.read_excel(excel_path, sheet_name=sheet_name, skiprows=5)
    df = df.dropna(how='all')
    df.columns = ['Codes', 'CodeName', 'Period_to_Date', 'Perc_Period_to_Date', 'Year_to_Date', 'Perc_Year_to_Date']
    # Trim out spaces to the left of the text in the CodeName field
    df['CodeName'] = df['CodeName'].str.lstrip()

    with engine.connect() as connection:
        df_deals = pd.read_sql('SELECT * FROM UW_Deals', connection)

    deal_dict = df_deals.set_index('DealName')['DealID'].to_dict()

    lower_bound, upper_bound = None, None
    for index, value in enumerate(df['CodeName']):
        if value == 'Period to Date':
            lower_bound = index
        elif value == 'Year to Date':
            upper_bound = index
            break
    
    # If both 'Period to Date' and 'Year to Date' rows were found, process the data
    if lower_bound is not None and upper_bound is not None:
        # Process the rows before 'Period to Date'
        df_cf = df.iloc[:lower_bound].copy()  # Make a copy of the relevant rows
        df_cf['Codes'] = pd.to_numeric(df_cf['Codes'], errors='coerce')  # Convert 'Codes' to numeric
        df_cf = df_cf.dropna(subset=['Codes'])  # Drop rows where 'Codes' is NaN
        df_cf = df_cf.drop(columns=['Year_to_Date', 'Perc_Year_to_Date', 'Perc_Period_to_Date'])  # Drop unnecessary columns
        df_cf = df_cf[~df_cf['Codes'].between(50000, 98998) & ~df_cf['Codes'].isin([20010, 20899])]  # Filter 'Codes' values
        df_cf['DealID'] = property_value  # Add the property value as 'Deal'
        df_cf['DealID'] = df_cf['DealID'].apply(get_matching_id, args=(deal_dict,))  # Get the matching ID for the deal name

        # Add 'Level' information based on 'Codes' values
        df_cf['Level'] = df_cf['Codes'].apply(lambda x: 'Adjustment' if str(x).startswith('1') else
                                            'Current Liability' if str(x).startswith('2') and x != 20899 else
                                            'Total Non-Current Liabilities' if x == 20899 else
                                            'Owner Distribution' if str(x).startswith('3') else '')
        df_cf['Date'] = end_of_month_str  # Add the date
        df_cf = df_cf[['DealID', 'Date', 'Level', 'Codes', 'CodeName', 'Period_to_Date']]  # Keep only the necessary columns
        df_cf = df_cf.reset_index(drop=True)  # Reset the index

        # Process the rows between 'Period to Date' and 'Year to Date'
        df_cf2 = df.iloc[lower_bound+1:upper_bound].copy()  # Make a copy of the relevant rows
        df_cf2['Codes'] = pd.to_numeric(df_cf2['Codes'], errors='coerce')  # Convert 'Codes' to numeric
        df_cf2 = df_cf2.dropna(subset=['Codes'])  # Drop rows where 'Codes' is NaN
        df_cf2['DealID'] = property_value  # Add the property value as 'Deal'
        df_cf2['DealID'] = df_cf2['DealID'].apply(get_matching_id, args=(deal_dict,))  # Get the matching ID for the deal name
        df_cf2['Level'] = 'Period to Date'  # Set 'Level' to 'Period to Date'
        df_cf2['Date'] = end_of_month_str  # Add the date
        df_cf2 = df_cf2.rename(columns={'Perc_Period_to_Date': 'Ending_Balance'})  # Rename the 'Perc_Period_to_Date' column
        df_cf2 = df_cf2[['DealID', 'Date', 'Level', 'Codes', 'CodeName', 'Ending_Balance']]  # Keep only the necessary columns
        df_cf2 = df_cf2.reset_index(drop=True)  # Reset the index
    
    # Write both DataFrames to the SQL database
    # df_cf.to_sql('RA_Cashflow', engine, if_exists='append', index=False)
    # df_cf2.to_sql('RA_Cashflow', engine, if_exists='append', index=False)
    print(df_cf2)

#     print("Data successfully inserted into the RA_CF table")
#
# Main Execution
def process_data(input_path):
    sheet_name = "Report1"
    df_head, end_of_month_str = load_excel_data(input_path, sheet_name)
    engine = setup_sql_connection()
    process_excel_data(input_path, sheet_name, df_head['property'][0], end_of_month_str, engine)


if __name__ == "__main__":
    input_path = r"Cash_Flow_40501_Accrual.xlsx"
    process_data(input_path)
