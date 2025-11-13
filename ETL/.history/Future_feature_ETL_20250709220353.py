# feature_engineering_pipeline.py

import os
import pandas as pd
import numpy as np
import yfinance as yf
from tqdm import tqdm
import talib

# ----------------- Config -----------------
INPUT_FILE = 'your_ohlc_data.csv'
TICKER_FILE = 'unique_tickers.txt'

# ----------------- Load Base Data -----------------
def load_base_data():
    df = pd.read_csv(INPUT_FILE, parse_dates=['Date'])
    df.sort_values(['Ticker', 'Date'], inplace=True)
    return df

# ----------------- Candlestick Patterns -----------------
def add_candlestick_patterns(df):
    pattern_funcs = {name: getattr(talib, name) for name in dir(talib) if name.startswith('CDL')}
    results = []

    for ticker, group in tqdm(df.groupby('Ticker'), desc="Adding candlestick patterns"):
        open_, high_, low_, close_ = group['Open'].values, group['High'].values, group['Low'].values, group['Close'].values
        for name, func in pattern_funcs.items():
            try:
                group[name] = func(open_, high_, low_, close_)
            except:
                group[name] = 0
        results.append(group)

    return pd.concat(results, ignore_index=True)

# ----------------- SMC Patterns -----------------
def detect_smc_patterns(df, swing_window=5):
    df['Swing_High'] = df['High'].shift(1).rolling(window=swing_window).max()
    df['Swing_Low'] = df['Low'].shift(1).rolling(window=swing_window).min()
    df['BOS_Up'] = df['Close'] > df['Swing_High']
    df['BOS_Down'] = df['Close'] < df['Swing_Low']

    df['BOS_Direction'] = np.select(
        [df['BOS_Up'], df['BOS_Down']],
        ['up', 'down'],
        default=None
    )
    df['Prev_BOS'] = df.groupby('Ticker')['BOS_Direction'].ffill()
    df['CHOCH'] = df['BOS_Direction'] != df['Prev_BOS']
    df['CHOCH'] = df['CHOCH'] & df['BOS_Direction'].notnull()

    df['Imbalance'] = (
        (df['Low'].shift(-1) > df['High']) | 
        (df['High'].shift(-1) < df['Low'])
    )
    df['Imbalance_Size'] = abs(df['Low'].shift(-1) - df['High'])
    return df

# ----------------- Rolling and SMA/Volatility Features -----------------
def add_rolling_features(df):
    grouped = df.groupby('Ticker')
    for win in [20, 50, 200]:
        df[f'{win}D_SMA'] = grouped['Close'].transform(lambda x: x.rolling(window=win).mean())
        df[f'{win}D_High'] = grouped['Close'].transform(lambda x: x.rolling(window=win).max())
        df[f'{win}D_Low'] = grouped['Close'].transform(lambda x: x.rolling(window=win).min())
    df['AllTime_High'] = grouped['Close'].transform(lambda x: x.expanding().max())
    df['AllTime_Low'] = grouped['Close'].transform(lambda x: x.expanding().min())
    df['Daily Change %'] = grouped['Close'].pct_change() * 100
    df['Change from Open %'] = ((df['Close'] - df['Open']) / df['Open']) * 100
    return df

# ----------------- After-Hours Data -----------------
def get_after_hours_info(ticker):
    try:
        df = yf.download(ticker, period="1d", interval="1m", prepost=True, progress=False)
        if df.empty:
            return None
        df = df.reset_index()
        df['Time'] = df['Datetime'].dt.time
        regular = df[df['Time'] < pd.to_datetime("16:00").time()]
        after = df[df['Time'] >= pd.to_datetime("16:00").time()]
        if not regular.empty and not after.empty:
            reg_close = regular['Close'].iloc[-1]
            after_close = after['Close'].iloc[-1]
            return {
                'Ticker': ticker,
                'Date': df['Datetime'].iloc[0].date(),
                'After-Hours Close': after_close,
                'After-Hours Change %': ((after_close - reg_close) / reg_close) * 100
            }
    except Exception as e:
        print(f"❌ After-hours error: {e}")
    return None

# ----------------- Financial Features (Yahoo) -----------------
def get_ticker_list():
    path = os.path.join(os.path.dirname(__file__), TICKER_FILE)
    return pd.read_csv(path, header=None)[0].tolist()

def extract_fundamental_features(tickers):
    all_data = []
    for ticker in tqdm(tickers, desc="Extracting financial features"):
        try:
            tk = yf.Ticker(ticker)
            info = tk.info
            hist = tk.history(period="1y", interval="1d")
            price = info.get("currentPrice")
            forward_eps = info.get("forwardEps")
            trailing_eps = info.get("trailingEps")
            peg = info.get("pegRatio")
            eps_growth = info.get("earningsGrowth")
            payout = info.get("payoutRatio")
            revenue_growth = info.get("revenueGrowth")
            insider = info.get("heldPercentInsiders")
            institutional = info.get("heldPercentInstitutions")

            # RSI & ATR
            delta = hist['Close'].diff()
            gain, loss = delta.clip(lower=0), -delta.clip(upper=0)
            rs = gain.rolling(14).mean() / loss.rolling(14).mean()
            rsi_14 = 100 - (100 / (1 + rs)).iloc[-1] if not rs.empty else None
            tr = pd.concat([
                hist['High'] - hist['Low'],
                abs(hist['High'] - hist['Close'].shift()),
                abs(hist['Low'] - hist['Close'].shift())
            ], axis=1).max(axis=1)
            atr = tr.rolling(14).mean().iloc[-1] if not tr.empty else None

            all_data.append({
                'Ticker': ticker,
                'Forward P/E': price / forward_eps if forward_eps else None,
                'PEG': peg,
                'EPS Growth (%)': eps_growth * 100 if eps_growth else None,
                'Payout Ratio (%)': payout * 100 if payout else None,
                'Sales Growth (%)': revenue_growth * 100 if revenue_growth else None,
                'Insider Ownership (%)': insider * 100 if insider else None,
                'Institutional Ownership (%)': institutional * 100 if institutional else None,
                'RSI (14)': rsi_14,
                'ATR (14)': atr
            })

        except Exception as e:
            print(f"⚠️ Error with {ticker}: {e}")

    return pd.DataFrame(all_data)

# ----------------- Merge & Validate -----------------
def merge_and_validate(df_main, df_fundamental):
    df = df_main.merge(df_fundamental, on='Ticker', how='left')
    df = df.sort_values(['Ticker', 'Date']).reset_index(drop=True)
    # Validation
    for col in ['Forward P/E', 'PEG', 'RSI (14)', 'ATR (14)']:
        if df[col].isnull().all():
            print(f"⚠️ Warning: Column {col} is all NaNs")
    return df

# ----------------- Main -----------------
if __name__ == "__main__":
    base_df = load_base_data()
    base_df = add_candlestick_patterns(base_df)
    base_df = detect_smc_patterns(base_df)
    base_df = add_rolling_features(base_df)
    tickers = base_df['Ticker'].unique().tolist()
    fund_df = extract_fundamental_features(tickers)
    final_df = merge_and_validate(base_df, fund_df)
    final_df.to_csv("final_features_output.csv", index=False)
    print("✅ Feature engineering pipeline completed and saved.")
