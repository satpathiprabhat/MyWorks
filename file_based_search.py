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
CUSTOMER_COLUMN = "CustomerID"  # Column in Excel containing customer numbers
OUTPUT_FILE = "search_results_optimized.csv"


# ==============================
# FUNCTIONS
# ==============================
def load_customer_numbers(file_path):
    """Load customer numbers from flat file into a set."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return set(line.strip() for line in f if line.strip())


def check_customers_in_excel(customer_numbers, excel_file, customer_column):
    """Read Excel once and check all customer numbers."""
    df = pd.read_excel(excel_file, usecols=[customer_column], engine="openpyxl")
    excel_customers = set(df[customer_column].astype(str).str.strip())
    results = {cust: (cust in excel_customers) for cust in customer_numbers}
    return results


def check_all_excels(customer_numbers, excel_files, customer_column):
    """Check all customer numbers in all Excel files."""
    all_results = {}
    for excel_file in excel_files:
        print(f"[INFO] Processing {excel_file} ...")
        results = check_customers_in_excel(customer_numbers, excel_file, customer_column)
        all_results[excel_file] = results
    return all_results


def save_results(all_results, output_file):
    """Save the results to CSV."""
    # Convert to DataFrame
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
    all_results = check_all_excels(customer_numbers, EXCEL_FILES, CUSTOMER_COLUMN)

    # 3. Save results
    save_results(all_results, OUTPUT_FILE)

    print("[INFO] Optimized Excel search complete.")