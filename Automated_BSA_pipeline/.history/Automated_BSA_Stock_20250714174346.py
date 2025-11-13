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
        combined.drop_duplicates(
            subset=['Ticker', 'Date'], keep='last', inplace=True)
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
