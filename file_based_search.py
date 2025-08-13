#!/usr/bin/env python3
import os
import pandas as pd

# ==============================
# CONFIGURATION
# ==============================
FLAT_FILE_PATH = "customers.txt"  # Flat file with customer numbers
EXCEL_FILES = [
    "file1.xlsx",
    "file2.xlsx",
    "file3.xlsx",
    "file4.xlsx",
    "file5.xlsx"
]
COLUMNS_TO_CHECK = ["CustomerID", "ColumnA", "ColumnB"]  # Columns to read from Excel
OUTPUT_FILE = "customer_details_output.csv"


# ==============================
# FUNCTIONS
# ==============================
def load_customer_numbers(file_path):
    """Load customer numbers from flat file into a list."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]


def extract_columns_from_excel(customer_numbers, excel_file, columns_to_check):
    """
    Read Excel once and extract ColumnA and ColumnB for each customer number.
    Returns dict: {customer_number: (ColumnA_value, ColumnB_value) or (None, None)}
    """
    df = pd.read_excel(excel_file, usecols=columns_to_check, engine="openpyxl")
    
    # Normalize all data to string and strip
    for col in columns_to_check:
        df[col] = df[col].astype(str).str.strip()
    
    # Create mapping: CustomerID -> (ColumnA, ColumnB)
    customer_map = {}
    for row in df.itertuples(index=False):
        customer_map[row[0]] = (row[1], row[2])  # ColumnA, ColumnB

    # Prepare results for all customers in flat file
    results = {}
    for cust in customer_numbers:
        if cust in customer_map:
            results[cust] = customer_map[cust]
        else:
            results[cust] = (None, None)  # Not found
    
    return results


def extract_all_excels(customer_numbers, excel_files, columns_to_check):
    """
    Extract ColumnA and ColumnB for all customers from all Excel files.
    Returns a nested dict: {excel_file: {customer_number: (ColumnA, ColumnB)}}
    """
    all_results = {}
    for excel_file in excel_files:
        print(f"[INFO] Processing {excel_file} ...")
        results = extract_columns_from_excel(customer_numbers, excel_file, columns_to_check)
        all_results[excel_file] = results
    return all_results


def save_results(all_results, output_file):
    """Save the results to CSV."""
    # Transform nested dict to flat dict for DataFrame
    flat_data = {}
    for excel_file, cust_map in all_results.items():
        colA_name = f"{excel_file}_ColumnA"
        colB_name = f"{excel_file}_ColumnB"
        for cust, (colA_val, colB_val) in cust_map.items():
            if cust not in flat_data:
                flat_data[cust] = {}
            flat_data[cust][colA_name] = colA_val
            flat_data[cust][colB_name] = colB_val
    
    df = pd.DataFrame(flat_data).T  # transpose to have customers as rows
    df.index.name = "CustomerNumber"
    df.to_csv(output_file)
    print(f"[INFO] Results saved to {output_file}")


# ==============================
# MAIN SCRIPT
# ==============================
if __name__ == "__main__":
    # Validate files
    if not os.path.exists(FLAT_FILE_PATH):
        raise FileNotFoundError(f"Flat file not found: {FLAT_FILE_PATH}")
    for file in EXCEL_FILES:
        if not os.path.exists(file):
            raise FileNotFoundError(f"Excel file not found: {file}")

    # 1. Load customer numbers
    customer_numbers = load_customer_numbers(FLAT_FILE_PATH)
    print(f"[INFO] Loaded {len(customer_numbers)} customer numbers.")

    # 2. Extract ColumnA and ColumnB for all Excel files
    all_results = extract_all_excels(customer_numbers, EXCEL_FILES, COLUMNS_TO_CHECK)

    # 3. Save results
    save_results(all_results, OUTPUT_FILE)

    print("[INFO] Customer ColumnA and ColumnB extraction complete.")