import os
import pandas as pd
from sqlalchemy import create_engine
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# SQLite database file
db_file = 'financial_data_test33.db'
engine = create_engine(f'sqlite:///{db_file}')

# Folder containing the Excel files
folder_path = r'C:\Users\Admin\Sumit\Raw data\Raw input files'

# Function to extract headers from a sample file
def extract_profit_or_loss_header(sample_file_path):
    df = pd.read_excel(sample_file_path, sheet_name='Data Sheet')
    return df.iloc[14:30, 0].tolist()

def extract_balance_sheet_header(sample_file_path):
    df = pd.read_excel(sample_file_path, sheet_name='Data Sheet')
    return df.iloc[54:71, 0].tolist()

def extract_cash_flow_header(sample_file_path):
    df = pd.read_excel(sample_file_path, sheet_name='Data Sheet')
    return df.iloc[79:84, 0].tolist()

def extract_quarterly_table_header(sample_file_path):
    df = pd.read_excel(sample_file_path, sheet_name='Data Sheet')
    return df.iloc[39:49, 0].tolist()

# Function to process each Excel file for PnL table
def process_pnl_file(file_path, ProfitOrLossHeader):
    df = pd.read_excel(file_path, sheet_name='Data Sheet')
    company_info_df = pd.read_excel(file_path, sheet_name='Data Sheet', usecols='B', nrows=1, header=None)
    company_name = str(company_info_df.iloc[0, 0]).strip() if not company_info_df.empty else "Unknown Company"
    
    data = df.iloc[14:30, 1:11].values.T
    transposed_df = pd.DataFrame(data, columns=ProfitOrLossHeader)
    transposed_df.insert(0, 'company_name', company_name)
    
    if 'Report date' in transposed_df.columns:
        transposed_df['Report date'] = pd.to_datetime(transposed_df['Report date']).dt.date
    
    transposed_df.to_sql('Profit_Loss', engine, if_exists='append', index=False)

# Function to process each Excel file for Balance Sheet table
def process_balance_sheet_file(file_path, BalanceSheetHeader):
    df = pd.read_excel(file_path, sheet_name='Data Sheet')
    company_info_df = pd.read_excel(file_path, sheet_name='Data Sheet', usecols='B', nrows=1, header=None)
    company_name = str(company_info_df.iloc[0, 0]).strip() if not company_info_df.empty else "Unknown Company"
    
    data = df.iloc[54:71, 1:11].values.T
    transposed_df = pd.DataFrame(data, columns=BalanceSheetHeader)
    transposed_df.insert(0, 'company_name', company_name)
    
    if 'Report date' in transposed_df.columns:
        transposed_df['Report date'] = pd.to_datetime(transposed_df['Report date']).dt.date
    
    transposed_df = transposed_df.loc[:, ~transposed_df.columns.duplicated()]
    transposed_df.to_sql('Balance_Sheet', engine, if_exists='append', index=False)

# Function to process each Excel file for CashFlow table
def process_cash_flow_file(file_path, CashFlowHeader):
    df = pd.read_excel(file_path, sheet_name='Data Sheet')
    company_info_df = pd.read_excel(file_path, sheet_name='Data Sheet', usecols='B', nrows=1, header=None)
    company_name = str(company_info_df.iloc[0, 0]).strip() if not company_info_df.empty else "Unknown Company"

    data = df.iloc[79:84, 1:11].values.T
    transposed_df = pd.DataFrame(data, columns=CashFlowHeader)
    transposed_df.insert(0, 'company_name', company_name)
    
    if 'Report date' in transposed_df.columns:
        transposed_df['Report date'] = pd.to_datetime(transposed_df['Report date']).dt.date
    
    transposed_df.to_sql('CashFlow', engine, if_exists='append', index=False)

# Function to process each Excel file for QuarterlyTable
def process_quarterly_table_file(file_path, QuarterlyTableHeader):
    df = pd.read_excel(file_path, sheet_name='Data Sheet')
    company_info_df = pd.read_excel(file_path, sheet_name='Data Sheet', usecols='B', nrows=1, header=None)
    company_name = str(company_info_df.iloc[0, 0]).strip() if not company_info_df.empty else "Unknown Company"

    data = df.iloc[39:49, 1:11].values.T
    transposed_df = pd.DataFrame(data, columns=QuarterlyTableHeader)
    transposed_df.insert(0, 'company_name', company_name)
    
    if 'Report date' in transposed_df.columns:
        transposed_df['Report date'] = pd.to_datetime(transposed_df['Report date']).dt.date
    
    transposed_df.to_sql('Quarterly', engine, if_exists='append', index=False)

# Function to process each Excel file for Prices table
def process_prices_file(file_path):
    df = pd.read_excel(file_path, sheet_name='Data Sheet')
    company_info_df = pd.read_excel(file_path, sheet_name='Data Sheet', usecols='B', nrows=1, header=None)
    company_name = str(company_info_df.iloc[0, 0]).strip() if not company_info_df.empty else "Unknown Company"

    prices_date = df.iloc[79, 1:11].values
    prices_data = df.iloc[88, 1:11].values

    prices_df = pd.DataFrame({
        "company_name": company_name,
        "Report date": prices_date,
        "Price": prices_data
    })

    prices_df.to_sql('Prices', engine, if_exists='append', index=False)

# Extract headers from a sample file
sample_file_path = os.path.join(folder_path, os.listdir(folder_path)[0])
ProfitOrLossHeader = extract_profit_or_loss_header(sample_file_path)
BalanceSheetHeader = extract_balance_sheet_header(sample_file_path)
CashFlowHeader = extract_cash_flow_header(sample_file_path)
QuarterlyTableHeader = extract_quarterly_table_header(sample_file_path)

# List to track skipped files
skipped_files = []

# Process all Excel files in the folder
for filename in os.listdir(folder_path):
    if filename.endswith('.xlsx') or filename.endswith('.xls'):
        file_path = os.path.join(folder_path, filename)
        try:
            process_pnl_file(file_path, ProfitOrLossHeader)
            process_balance_sheet_file(file_path, BalanceSheetHeader)
            process_cash_flow_file(file_path, CashFlowHeader)
            process_quarterly_table_file(file_path, QuarterlyTableHeader)
            process_prices_file(file_path)
            print(f"Processed: {filename}")  # Print the processed file name
        except Exception as e:
            logging.error(f"Failed to process file {filename}: {e}")
            skipped_files.append(filename)  # Add to skipped files

# Print skipped files
if skipped_files:
    print("Skipped files:")
    for skipped in skipped_files:
        print(skipped)

print("Data processing complete.")
