#!/usr/bin/env python3
import csv
import os
import pandas as pd

# ==============================
# CONFIGURATION
# ==============================
FLAT_FILE_PATH = "customers.txt"  # Flat file with customer numbers
CSV_FILES = {
    "file1.csv": 0,  # CSV path : column index of customer number
    "file2.csv": 0,
    "file3.csv": 0,
    "file4.csv": 0,
    "file5.csv": 0
}
OUTPUT_FILE = "search_results_streaming.csv"


# ==============================
# FUNCTIONS
# ==============================
def load_flat_customers(file_path):
    """Load customer numbers from flat file into a set."""
    customers = set()
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            cust = line.strip()
            if cust:
                customers.add(cust)
    print(f"[INFO] Loaded {len(customers)} customers from flat file.")
    return customers


def search_customer_in_csv(customer, file_path, column_index):
    """Stream CSV line by line and return True if customer exists."""
    with open(file_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader, None)  # Skip header
        for row in reader:
            if len(row) > column_index and row[column_index].strip() == customer:
                return True
    return False


def check_customers_streaming(flat_customers, csv_files):
    """Check each customer against all CSVs in a memory-efficient streaming way."""
    results = {}
    for cust in flat_customers:
        results[cust] = {}
        for fname, col_idx in csv_files.items():
            found = search_customer_in_csv(cust, fname, col_idx)
            results[cust][fname] = found
        print(f"[INFO] Checked customer: {cust}")
    return results


def save_results(results, output_file):
    """Save results to CSV."""
    df = pd.DataFrame.from_dict(results, orient='index')
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
    for file in CSV_FILES:
        if not os.path.exists(file):
            raise FileNotFoundError(f"CSV file not found: {file}")

    # 1. Load flat file customers
    flat_customers = load_flat_customers(FLAT_FILE_PATH)

    # 2. Check customers using streaming search
    results = check_customers_streaming(flat_customers, CSV_FILES)

    # 3. Save results
    save_results(results, OUTPUT_FILE)

    print("[INFO] Streaming processing complete.")