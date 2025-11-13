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
    today_str = main_df["Date"] = (
        pd.to_datetime(main_df["Date"], errors='coerce')  # safely convert
        .dt.tz_localize(None)                             # remove timezone
        .dt.strftime('%Y-%m-%d')                          # format as string
    )
    print(f"Fetching latest data for {ticker}...")
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="1d")
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


def append_all_tickers_once(tickers, file_path):
    if not os.path.exists(file_path):
        print(f"❌ File does not exist: {file_path}")
        return

    existing_df = pd.read_csv(file_path, parse_dates=["Date"], sep=';')
    today_str = datetime.today().strftime('%Y-%m-%d')

    # Tickers already in the dataset
    known_tickers = set(existing_df['Ticker'].unique())

    # Tickers already present for today
    tickers_today = set(
        existing_df[existing_df['Date'].dt.strftime(
            '%Y-%m-%d') == today_str]['Ticker'].unique()
    )

    all_new_data = []

    for ticker in tickers:
        if ticker not in known_tickers:
            print(f"🚫 Skipping {ticker}: not in existing dataset.")
            continue
        if ticker in tickers_today:
            print(f"⏭️ Skipping {ticker}: already fetched for today.")
            continue

        new_data = fetch_today_data(ticker)
        if not new_data.empty:
            new_data = new_data.reindex(columns=existing_df.columns)
            all_new_data.append(new_data)

    if not all_new_data:
        print("⚠️ No new data to append.")
        return

    combined = pd.concat([existing_df] + all_new_data, ignore_index=True)
    combined.drop_duplicates(
        subset=['Ticker', 'Date'], keep='last', inplace=True)
    combined.to_csv(file_path, index=False, sep=';')
    print("✅ New data appended and saved.")


# === Main ===
if __name__ == "__main__":
    file_path = "C:/Users/nss_1/Pictures/Automated_BSA_pipeline/final_features_output_updated.csv"
    tickers = get_ticker_list()
    if not tickers:
        print("No tickers found. Exiting.")
        exit()

    append_all_tickers_once(tickers, file_path)
