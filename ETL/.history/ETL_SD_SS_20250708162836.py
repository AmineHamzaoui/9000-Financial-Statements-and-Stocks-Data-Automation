import os
import json
import pandas as pd
from glob import glob
from tqdm import tqdm

# Paths
stock_data_dir = "D:/ETL/stock_data"
stock_splits_dir = "D:/ETL/stock_splits"
output_dir = "D:/ETL/output_merged"
os.makedirs(output_dir, exist_ok=True)

# Helper


def extract_ticker(filename):
    return os.path.basename(filename).split("_")[0]


def process_stock_data_file(filepath, ticker):
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


def process_stock_split_file(filepath, ticker):
    with open(filepath, 'r') as f:
        raw_data = json.load(f)
    df = pd.DataFrame(raw_data)
    df.rename(columns={
        'd': 'date',
        'c': 'Stock Split Value'
    }, inplace=True)
    df["ticker"] = ticker
    return df


# Load file mappings
stock_data_files = {extract_ticker(f): f for f in glob(
    os.path.join(stock_data_dir, "*_stock_data.json"))}
stock_split_files = {extract_ticker(f): f for f in glob(
    os.path.join(stock_splits_dir, "*_stock_splits.json"))}

# Iterate ticker by ticker
for ticker in tqdm(stock_data_files.keys(), desc="Processing tickers"):
    stock_fp = stock_data_files[ticker]
    stock_df = process_stock_data_file(stock_fp, ticker)

    split_fp = stock_split_files.get(ticker)
    if split_fp:
        split_df = process_stock_split_file(split_fp, ticker)
        merged_df = pd.merge(stock_df, split_df, on=[
                             "date", "ticker"], how="left")
    else:
        merged_df = stock_df  # No split file for this ticker

    # Save merged data to disk
    merged_df.to_csv(os.path.join(
        output_dir, f"{ticker}_merged.csv"), index=False)
