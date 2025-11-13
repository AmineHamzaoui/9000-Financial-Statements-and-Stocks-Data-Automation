import os
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

def get_sector_industry(tickers):
    data = []
    for ticker in tickers:
        try:
            info = yf.Ticker(ticker).info
            sector = info.get('sector', 'N/A')
            industry = info.get('industry', 'N/A')
            data.append({'Ticker': ticker, 'Sector': sector, 'Industry': industry})
        except Exception as e:
            print(f"Failed to fetch data for {ticker}: {e}")
            data.append({'Ticker': ticker, 'Sector': 'N/A', 'Industry': 'N/A'})
    return pd.DataFrame(data)

if __name__ == "__main__":
    tickers = get_ticker_list()
    if not tickers:
        exit()

    df = get_sector_industry(tickers)
    print(df)

    # Optionally save to CSV
    output_path = os.path.join(os.path.dirname(__file__), 'tickers_sector_industry.csv')
    df.to_csv(output_path, index=False)
    print(f"\nSaved to {output_path}")