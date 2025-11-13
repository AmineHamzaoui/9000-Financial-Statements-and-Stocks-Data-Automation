
import pandas as pd
import pandas as pd
import talib
from tqdm import tqdm

# --- CONFIGURATION ---
INPUT_FILE = 'your_ohlc_data.csv'   # Replace with your actual file path
OUTPUT_FILE = 'all_candlestick_signals.csv'

# --- LOAD DATA ---
# Only essential columns (edit 'Volume' if needed)
df = pd.read_csv(INPUT_FILE, parse_dates=['Date'], usecols=['Ticker', 'Date', 'Open', 'High', 'Low', 'Close'])
df = df.sort_values(by=['Ticker', 'Date'])

# --- LOAD TA-Lib CANDLESTICK FUNCTIONS ---
pattern_functions = {name: getattr(talib, name) for name in dir(talib) if name.startswith('CDL')}
pattern_names = list(pattern_functions.keys())

# --- PROCESS BY TICKER TO REDUCE MEMORY USAGE ---
results = []  # list to collect processed DataFrames
for ticker, group in tqdm(df.groupby('Ticker'), desc="Processing Tickers"):

    # Prepare inputs
    open_ = group['Open'].values
    high_ = group['High'].values
    low_ = group['Low'].values
    close_ = group['Close'].values

    # Apply all TA-Lib pattern functions
    for name, func in pattern_functions.items():
        try:
            group[name] = func(open_, high_, low_, close_)
        except Exception as e:
            print(f"Error applying {name} to {ticker}: {e}")
            group[name] = 0  # fallback if something fails

    # Append processed group to results list
    results.append(group)

# --- COMBINE AND SAVE ---
final_df = pd.concat(results, ignore_index=True)
final_df.to_csv(OUTPUT_FILE, index=False)

print(f"\n✅ Done. Saved all candlestick pattern columns to: {OUTPUT_FILE}")
import os
import yfinance as yf
import pandas as pd
from datetime import datetime
from time import sleep

def get_ticker_list():
    path = os.path.join(os.path.dirname(__file__), 'unique_tickers.txt')
    try:
        with open(path, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print("❌ Ticker list file not found!")
        return []

def get_after_hours_info(ticker):
    try:
        df = yf.download(ticker, period="1d", interval="1m", prepost=True, progress=False)
        if df.empty:
            return None

        df = df.reset_index()
        df['Time'] = df['Datetime'].dt.time

        # Regular close: last value before 16:00
        regular_session = df[df['Time'] < pd.to_datetime("16:00").time()]
        after_hours = df[df['Time'] >= pd.to_datetime("16:00").time()]

        if not regular_session.empty and not after_hours.empty:
            regular_close = regular_session['Close'].iloc[-1]
            after_hours_close = after_hours['Close'].iloc[-1]
            after_hours_change = (after_hours_close - regular_close) / regular_close

            return {
                'Ticker': ticker,
                'Date': df['Datetime'].iloc[0].date(),
                'Regular Close': regular_close,
                'After-Hours Close': after_hours_close,
                'After-Hours Change (%)': round(after_hours_change * 100, 2)
            }
        else:
            return None

    except Exception as e:
        print(f"❌ Error with {ticker}: {e}")
        return None

# Read tickers
tickers = get_ticker_list()
print(f"🔍 Found {len(tickers)} tickers.")

# Process all tickers
results = []
for ticker in tickers:
    print(f"📥 Processing {ticker}")
    data = get_after_hours_info(ticker)
    if data:
        results.append(data)
    sleep(1)  # Avoid throttling

# Convert to DataFrame
df_after_hours = pd.DataFrame(results)
print("\n✅ Done. After-hours data:")
print(df_after_hours)

# Optional: Save to CSV
df_after_hours.to_csv("after_hours_data.csv", index=False)
import pandas as pd
import numpy as np

def detect_smc_patterns(df, swing_window=5, imbalance_threshold=0.1):
    df = df.copy()

    # --- 1. Detect BOS: Break above swing high or below swing low ---
    df['Swing_High'] = df['High'].shift(1).rolling(window=swing_window).max()
    df['Swing_Low'] = df['Low'].shift(1).rolling(window=swing_window).min()

    df['BOS_Up'] = df['Close'] > df['Swing_High']
    df['BOS_Down'] = df['Close'] < df['Swing_Low']

    # --- 2. Detect CHoCH: BOS in one direction, then opposite BOS ---
    df['BOS_Direction'] = np.select(
        [df['BOS_Up'], df['BOS_Down']],
        ['up', 'down'],
        default=None
    )

    df['Prev_BOS'] = df['BOS_Direction'].ffill()
    df['CHOCH'] = df['BOS_Direction'] != df['Prev_BOS']
    df['CHOCH'] = df['CHOCH'] & df['BOS_Direction'].notnull()

    # --- 3. Detect Imbalance (Fair Value Gaps) ---
    # Price gap: between previous candle low and next candle high
    df['Imbalance'] = (
        (df['Low'].shift(-1) > df['High']) | 
        (df['High'].shift(-1) < df['Low'])
    )

    # Optional: mark imbalance severity
    df['Imbalance_Size'] = abs(df['Low'].shift(-1) - df['High'])

    return df[['Date', 'Open', 'High', 'Low', 'Close', 'BOS_Up', 'BOS_Down', 'CHOCH', 'Imbalance', 'Imbalance_Size']]

# Price/Earnings (P/E)
df['P/E'] = df['Close'] / df['EPS - Earnings Per Share']
df['P/S'] = df['Market Cap'] / df['Revenue']

# Price/Book (P/B)
df['P/B'] = df['Market Cap'] / (df['Book Value Per Share'] * df['Shares Outstanding'])

# Price/Cash
df['Price/Cash'] = df['Market Cap'] / df['Cash On Hand']

# Price/Free Cash Flow
df['Price/Free Cash Flow'] = df['Market Cap'] / (df['Free Cash Flow Per Share'] * df['Shares Outstanding'])

# EV/EBITDA → Only if Enterprise Value is known → NOT available here

# Gross Margin
df['Gross Margin (%)'] = df['Gross Margin'] * 100  # Already exists

# Operating Margin
df['Operating Margin (%)'] = df['Operating Margin'] * 100  # Already exists

# Net Profit Margin
df['Net Profit Margin (%)'] = df['Net Profit Margin'] * 100  # Already exists

# ROA, ROE, ROI
df['ROA (%)'] = df['ROA - Return On Assets'] * 100
df['ROE (%)'] = df['ROE - Return On Equity'] * 100
df['ROI (%)'] = df['ROI - Return On Investment'] * 100

# Debt/Equity
df['Debt/Equity'] = df['Debt/Equity Ratio']
# Long-Term Debt/Equity
df['Long-Term Debt/Equity'] = df['Long-term Debt / Capital']
# Current Ratio
df['Current Ratio'] = df['Current Ratio']

import pandas as pd

# Sample input: make sure your dataset includes these columns
# df = pd.read_csv('your_file.csv')  # Assuming your file has Date, Ticker, and Close
df['Date'] = pd.to_datetime(df['Date'])
df = df.sort_values(['Ticker', 'Date'])

# Group by ticker
grouped = df.groupby('Ticker')

# Rolling window features
df['20D_High'] = grouped['Close'].transform(lambda x: x.rolling(window=20, min_periods=1).max())
df['20D_Low'] = grouped['Close'].transform(lambda x: x.rolling(window=20, min_periods=1).min())

df['50D_High'] = grouped['Close'].transform(lambda x: x.rolling(window=50, min_periods=1).max())
df['50D_Low'] = grouped['Close'].transform(lambda x: x.rolling(window=50, min_periods=1).min())

df['52W_High'] = grouped['Close'].transform(lambda x: x.rolling(window=252, min_periods=1).max())
df['52W_Low'] = grouped['Close'].transform(lambda x: x.rolling(window=252, min_periods=1).min())

# All-Time high/low (up to that date)
df['AllTime_High'] = grouped['Close'].transform(lambda x: x.expanding().max())
df['AllTime_Low'] = grouped['Close'].transform(lambda x: x.expanding().min())

# Reset index for cleanliness
df = df.sort_values(['Ticker', 'Date']).reset_index(drop=True)

# Display first few rows
print(df.head())
