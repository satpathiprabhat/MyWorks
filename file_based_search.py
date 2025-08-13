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
COLUMNS_TO_CHECK = ["CustomerID", "ColumnA", "ColumnB"]  # Columns in Excel
OUTPUT_FILE = "search_results_multi_column.csv"


# ==============================
# FUNCTIONS
# ==============================
def load_customer_numbers(file_path):
    """Load customer numbers from flat file into a list."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]


def check_customers_in_excel(customer_numbers, excel_file, columns_to_check):
    """
    Read Excel once and check all customer numbers with other columns.
    Returns dict: {customer_number: True/False}
    """
    # Read only the relevant columns
    df = pd.read_excel(excel_file, usecols=columns_to_check, engine="openpyxl")
    
    # Normalize all data to string and strip
    for col in columns_to_check:
        df[col] = df[col].astype(str).str.strip()
    
    # Create a set of tuples for all rows
    excel_tuples = set([tuple(row) for row in df[columns_to_check].values])
    
    # Check each customer number with placeholder values for other columns
    # Since flat file has only customer number, we match only CustomerID
    results = {}
    for cust in customer_numbers:
        # Create tuple with customer number in first position, others as wildcards
        match = any(t[0] == cust for t in excel_tuples)
        results[cust] = match
    
    return results


def check_all_excels(customer_numbers, excel_files, columns_to_check):
    """Check all customer numbers in all Excel files."""
    all_results = {}
    for excel_file in excel_files:
        print(f"[INFO] Processing {excel_file} ...")
        results = check_customers_in_excel(customer_numbers, excel_file, columns_to_check)
        all_results[excel_file] = results
    return all_results


def save_results(all_results, output_file):
    """Save the results to CSV."""
    df = pd.DataFrame(all_results)
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

    # 2. Check all Excel files
    all_results = check_all_excels(customer_numbers, EXCEL_FILES, COLUMNS_TO_CHECK)

    # 3. Save results
    save_results(all_results, OUTPUT_FILE)

    print("[INFO] Multi-column Excel search complete.")