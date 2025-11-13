
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
