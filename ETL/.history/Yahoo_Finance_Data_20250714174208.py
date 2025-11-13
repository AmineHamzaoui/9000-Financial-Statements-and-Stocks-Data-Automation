import os
import yfinance as yf
import pandas as pd

# === Step 1: Load Tickers from File ===

"""
def get_ticker_list():
    path = os.path.join(os.path.dirname(__file__), 'unique_tickers.txt')
    try:
        with open(path, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print("Ticker list file not found!")
        return []

# === Step 2: Create Folder to Save Individual CSVs ===


def create_output_folder():
    output_dir = os.path.join(os.path.dirname(__file__), 'historical_chunks')
    os.makedirs(output_dir, exist_ok=True)
    return output_dir

# === Step 3: Fetch and Save Individual Ticker Data ===


def fetch_and_save_data(tickers, output_dir):
    for ticker in tickers:
        print(f"Fetching data for {ticker}...")
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="max")

            if not hist.empty:
                hist = hist.copy()
                hist.reset_index(inplace=True)
                hist['Ticker'] = ticker
                output_path = os.path.join(output_dir, f"{ticker}.csv")
                hist.to_csv(output_path, index=False)
            else:
                print(f"No data found for {ticker}.")
        except Exception as e:
            print(f"Error fetching {ticker}: {e}")

# === Step 4: Combine All CSV Files into One ===

"""


def combine_all_csvs(output_dir):
    files = [f for f in os.listdir(output_dir) if f.endswith('.csv')]
    all_data = []

    for file in files:
        file_path = os.path.join(output_dir, file)
        try:
            df = pd.read_csv(file_path)
            all_data.append(df)
        except Exception as e:
            print(f"Error reading {file}: {e}")

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        final_path = os.path.join(os.path.dirname(
            __file__), 'historical_data.csv')
        combined_df.to_csv(final_path, index=False)
        print(f"\n✅ All data saved to: {final_path}")
    else:
        print("⚠️ No valid data files found to combine.")


# === Main ===
if __name__ == "__main__":
    """
    tickers = get_ticker_list()
    if not tickers:
        print("No tickers found. Exiting.")
        exit()

    output_dir = create_output_folder()
    fetch_and_save_data(tickers, output_dir)"""
    combine_all_csvs("D:/ETL/historical_chunks")
import os
import yfinance as yf
import pandas as pd
from datetime import datetime

def get_ticker_list():
    path = os.path.join(os.path.dirname(__file__), 'unique_tickers.txt')
    try:
        with open(path, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print("Ticker list file not found!")
        return []

def fetch_today_data(tickers):
    today_str = datetime.today().strftime('%Y-%m-%d')
    today_data = []

    for ticker in tickers:
        print(f"Fetching latest data for {ticker}...")
        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(period="1d")  # Only today

            if not hist.empty:
                hist = hist.copy()
                hist.reset_index(inplace=True)
                hist['Ticker'] = ticker
                hist = hist[hist['Date'].dt.strftime('%Y-%m-%d') == today_str]
                today_data.append(hist)
            else:
                print(f"No data found for {ticker}.")
        except Exception as e:
            print(f"Error fetching {ticker}: {e}")

    if today_data:
        return pd.concat(today_data, ignore_index=True)
    else:
        return pd.DataFrame()

def append_to_csv(new_data, file_path):
    if not os.path.exists(file_path):
        print("No existing historical file. Creating a new one.")
        new_data.to_csv(file_path, index=False)
    else:
        existing_df = pd.read_csv(file_path, parse_dates=["Date"])
        # Remove duplicates based on Ticker + Date
        combined = pd.concat([existing_df, new_data], ignore_index=True)
        combined.drop_duplicates(subset=['Ticker', 'Date'], keep='last', inplace=True)
        combined.to_csv(file_path, index=False)
        print(f"✅ Updated historical file: {file_path}")

# === Main ===
if __name__ == "__main__":
    tickers = get_ticker_list()
    if not tickers:
        print("No tickers found. Exiting.")
        exit()

    new_data = fetch_today_data(tickers)
    if new_data.empty:
        print("No new data to append.")
    else:
        append_to_csv(new_data, "D:/ETL/historical_data.csv")