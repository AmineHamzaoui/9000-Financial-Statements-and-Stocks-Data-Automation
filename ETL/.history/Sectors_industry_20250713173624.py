import os
import time
import yfinance as yf
import pandas as pd


def get_ticker_list():
    path = os.path.join(os.path.dirname(__file__), 'unique_tickers.txt')
    try:
        with open(path, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print("Ticker list file not found!")
        return []


def get_sector_industry_with_resume(tickers, output_file='tickers_sector_industry.csv'):
    output_path = os.path.join(os.path.dirname(__file__), output_file)

    # Load existing data if available
    if os.path.exists(output_path):
        existing = pd.read_csv(output_path)
        processed = set(existing['Ticker'])
    else:
        existing = pd.DataFrame(columns=['Ticker', 'Sector', 'Industry'])
        processed = set()

    data = []
    for ticker in tickers:
        if ticker in processed:
            continue
        try:
            info = yf.Ticker(ticker).info
            sector = info.get('sector', 'N/A')
            industry = info.get('industry', 'N/A')
            row = {'Ticker': ticker, 'Sector': sector, 'Industry': industry}
            print(f"{ticker}: {sector} | {industry}")
            data.append(row)
        except Exception as e:
            print(f"Failed to fetch data for {ticker}: {e}")
            data.append({'Ticker': ticker, 'Sector': 'N/A', 'Industry': 'N/A'})
        time.sleep(1.5)  # Throttle to avoid rate limiting

    # Merge and save results
    df_new = pd.DataFrame(data)
    df_all = pd.concat([existing, df_new], ignore_index=True)
    df_all.drop_duplicates(subset='Ticker', inplace=True)
    df_all.to_csv(output_path, index=False)
    print(f"\nSaved to {output_path}")
    return df_all


if __name__ == "__main__":
    tickers = get_ticker_list()
    if not tickers:
        exit("No tickers found.")
    get_sector_industry_with_resume(tickers)
