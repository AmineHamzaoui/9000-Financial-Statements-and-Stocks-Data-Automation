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


def fetch_today_data(ticker):
    today_str = datetime.today().strftime('%Y-%m-%d')
    print(f"Fetching latest data for {ticker}...")
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="1d")  # Only today
        if not hist.empty:
            hist = hist.copy()
            hist.reset_index(inplace=True)
            hist['Ticker'] = ticker
            hist = hist[hist['Date'].dt.strftime('%Y-%m-%d') == today_str]
            return hist
        else:
            print(f"⚠️ No data found for {ticker}")
            return pd.DataFrame()
    except Exception as e:
        print(f"❌ Error fetching {ticker}: {e}")
        return pd.DataFrame()


def append_ticker_data(ticker, file_path):
    new_data = fetch_today_data(ticker)
    if new_data.empty:
        return

    if os.path.exists(file_path):
        existing_df = pd.read_csv(file_path, parse_dates=["Date"], sep=';')
        new_data = new_data.reindex(
            columns=existing_df.columns)  # Align columns
        combined = pd.concat([existing_df, new_data], ignore_index=True)
        combined.drop_duplicates(
            subset=['Ticker', 'Date'], keep='last', inplace=True)
        combined.to_csv(file_path, index=False, sep=';')
        print(f"✅ Appended data for {ticker}")
    else:
        print(f"❌ File does not exist: {file_path}")


# === Main ===
if __name__ == "__main__":
    file_path = "C:/Users/nss_1/Pictures/Automated_BSA_pipeline/Test_dataset.csv"
    tickers = get_ticker_list()
    if not tickers:
        print("No tickers found. Exiting.")
        exit()

    for ticker in tickers:
        append_ticker_data(ticker, file_path)
