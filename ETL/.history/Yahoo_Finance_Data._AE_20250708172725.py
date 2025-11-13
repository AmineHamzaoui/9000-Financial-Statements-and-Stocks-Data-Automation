import os
import yfinance as yf
import pandas as pd

# === FUNCTION TO LOAD TICKERS ===


def get_ticker_list():
    path = os.path.join(os.path.dirname(__file__), 'unique_tickers.txt')
    try:
        with open(path, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print("Ticker list file not found!")
        return []


# === LOAD TICKERS ===
tickers = get_ticker_list()
if not tickers:
    print("No tickers found. Exiting.")
    exit()

# === FETCH HISTORICAL DATA ===
all_data = {}  # To store each ticker’s data

for ticker in tickers:
    print(f"Fetching data for {ticker}...")
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="max")

        if not hist.empty:
            all_data[ticker] = hist
        else:
            print(f"No data found for {ticker}.")
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")

# === COMBINE INTO ONE DATAFRAME (optional) ===
if all_data:
    combined_df = pd.concat(all_data, axis=0)
    combined_df.index.names = ['Ticker', 'Date']

    # Save to CSV (optional)
    output_path = os.path.join(
        os.path.dirname(__file__), 'historical_data.csv')
    combined_df.to_csv(output_path)
    print(f"\nAll data saved to {output_path}")
else:
    print("No data to save.")
