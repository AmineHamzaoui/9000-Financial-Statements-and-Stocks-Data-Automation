import os
import json
import pandas as pd
from glob import glob

# === Set your folder paths ===
stock_data_dir = "D:/ETL/stock_data"         # Replace with your actual path
stock_splits_dir = "D:/ETL/stock_splits"     # Replace with your actual path

# === Define helper function to extract ticker from filename ===
def extract_ticker(filename):
    return os.path.basename(filename).split("_")[0]

# === Process stock data file ===
def process_stock_data_file(filepath):
    ticker = extract_ticker(filepath)
    with open(filepath, 'r') as f:
        raw_data = json.load(f)
    df = pd.DataFrame(raw_data)
    df.rename(columns={
        'd': 'date',
        'o': 'Stock Price History Open',
        'h': 'Stock Price History High',
        'l': 'Stock Price History Low',
        'c': 'Stock Price History Close',
        'v': 'Stock Volume',
        'ma50': '50-day MA',
        'ma200': '200-day MA'
    }, inplace=True)
    df["ticker"] = ticker
    return df

# === Process stock splits file ===
def process_stock_split_file(filepath):
    ticker = extract_ticker(filepath)
    with open(filepath, 'r') as f:
        raw_data = json.load(f)
    df = pd.DataFrame(raw_data)
    df.rename(columns={
        'd': 'date',
        'c': 'Stock Split Value'
    }, inplace=True)
    df["ticker"] = ticker
    return df

# === Loop over all stock data and stock splits ===
stock_data_files = glob(os.path.join(stock_data_dir, "*_stock_data.json"))
stock_split_files = glob(os.path.join(stock_splits_dir, "*_stock_splits.json"))

print(f"Found {len(stock_data_files)} stock data files and {len(stock_split_files)} split files...")

# === Combine all data ===
stock_data_list = [process_stock_data_file(fp) for fp in stock_data_files]
stock_split_list = [process_stock_split_file(fp) for fp in stock_split_files]

all_stock_data = pd.concat(stock_data_list, ignore_index=True)
all_stock_splits = pd.concat(stock_split_list, ignore_index=True)

# === Merge datasets ===
merged_data = pd.merge(
    all_stock_data,
    all_stock_splits,
    on=["date", "ticker"],
    how="left"  # Keep all stock data even if no split info
)

# === Optional: Export the final dataset ===
output_path = "merged_stock_data.csv"
merged_data.to_csv(output_path, index=False)
print(f"Saved merged data to {output_path}")
