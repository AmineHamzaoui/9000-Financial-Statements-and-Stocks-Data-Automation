# feature_engineering_pipeline.py

import os
import pandas as pd
import numpy as np
import yfinance as yf
import talib
from tqdm import tqdm
from smartmoneyconcepts import smc
# ----------------- Config -----------------
INPUT_FILE = 'D:/ETL/stock_with_fundamentals.csv'
TICKER_FILE = 'unique_tickers.txt'

# ----------------- Load Base Data -----------------


def load_base_data():
    df = pd.read_csv(INPUT_FILE, parse_dates=['date_x'], sep=';')
    df.sort_values(['Ticker', 'date_x'], inplace=True)
    return df

# ----------------- Candlestick Patterns -----------------


def add_candlestick_patterns(df):
    pattern_funcs = {name: getattr(talib, name)
                     for name in dir(talib) if name.startswith('CDL')}
    results = []
    for ticker, group in tqdm(df.groupby('Ticker'), desc="Adding candlestick patterns"):
        open_, high_, low_, close_ = group['Open'].values, group[
            'High'].values, group['Low'].values, group['Close'].values
        for name, func in pattern_funcs.items():
            try:
                group[name] = func(open_, high_, low_, close_)
            except:
                group[name] = 0
        results.append(group)
    return pd.concat(results, ignore_index=True)

# ----------------- SMC Patterns + VOB, POI, POC -----------------


def detect_smc_patterns(df, swing_length=5):
    results = []

    for ticker, group in df.groupby("Ticker_x"):
        group = group.sort_values("date_x").reset_index(drop=True)

        # Rename for compatibility with SMC library
        ohlc = group.rename(columns={
            "Open": "open", "High": "high", "Low": "low", "Close": "close"
        })[["open", "high", "low", "close"]].copy()

        # ---- 1. Swing Highs and Lows ----
        swing = smc.swing_highs_lows(ohlc, swing_length=swing_length)
        group["swing"] = swing["HighLow"]
        group["swing_level"] = swing["Level"]

        # ---- 2. Break of Structure / Change of Character ----
        bos_choch = smc.bos_choch(ohlc, swing, close_break=True)
        group["BOS"] = bos_choch["BOS"]
        group["CHOCH"] = bos_choch["CHOCH"]
        group["structure_level"] = bos_choch["Level"]
        group["structure_broken_index"] = bos_choch["BrokenIndex"]

        # ---- 3. Fair Value Gap ----
        fvg = smc.fvg(ohlc, join_consecutive=False)
        group["FVG"] = fvg["FVG"]
        group["FVG_Top"] = fvg["Top"]
        group["FVG_Bottom"] = fvg["Bottom"]
        group["FVG_MitigatedIndex"] = fvg["MitigatedIndex"]

        # ---- 4. Order Blocks ----
        ob = smc.ob(ohlc, swing, close_mitigation=False)
        group["OrderBlock"] = ob["OB"]
        group["OB_Top"] = ob["Top"]
        group["OB_Bottom"] = ob["Bottom"]
        group["OB_Volume"] = ob["OBVolume"]
        group["OB_Percentage"] = ob["Percentage"]

        # ---- 5. Liquidity ----
        liq = smc.liquidity(ohlc, swing, range_percent=0.01)
        group["Liquidity"] = liq["Liquidity"]
        group["Liquidity_Level"] = liq["Level"]
        group["Liquidity_End"] = liq["End"]
        group["Liquidity_Swept"] = liq["Swept"]

        results.append(group)

    return pd.concat(results).reset_index(drop=True)

# ----------------- POC (rolling mode of close price) -----------------


def add_poc(df, window=50):
    df['POC'] = df.groupby('Ticker')['Close'].transform(lambda x: x.rolling(
        window).apply(lambda s: s.mode().iloc[0] if not s.mode().empty else np.nan))
    return df

# ----------------- Rolling Features -----------------


def add_rolling_features(df):
    grouped = df.groupby('Ticker')
    for win in [20, 50, 200]:
        df[f'{win}D_SMA'] = grouped['Close'].transform(
            lambda x: x.rolling(window=win).mean())
        df[f'{win}D_High'] = grouped['Close'].transform(
            lambda x: x.rolling(window=win).max())
        df[f'{win}D_Low'] = grouped['Close'].transform(
            lambda x: x.rolling(window=win).min())
    df['52W_High'] = grouped['Close'].transform(
        lambda x: x.rolling(window=252).max())
    df['52W_Low'] = grouped['Close'].transform(
        lambda x: x.rolling(window=252).min())
    df['AllTime_High'] = grouped['Close'].transform(
        lambda x: x.expanding().max())
    df['AllTime_Low'] = grouped['Close'].transform(
        lambda x: x.expanding().min())
    df['Daily Change %'] = grouped['Close'].pct_change() * 100
    df['Change from Open %'] = ((df['Close'] - df['Open']) / df['Open']) * 100
    return df

# ----------------- Fundamental Features from Yahoo -----------------


def get_ticker_list():
    path = os.path.join(os.path.dirname(__file__), TICKER_FILE)
    return pd.read_csv(path, header=None)[0].tolist()


def extract_fundamental_features(tickers):
    all_data = []
    for ticker in tqdm(tickers, desc="Extracting financial features"):
        try:
            tk = yf.Ticker(ticker)
            info = tk.info
            hist = tk.history(period="max", interval="1d")
            price = info.get("currentPrice")
            forward_eps = info.get("forwardEps")
            trailing_eps = info.get("trailingEps")
            peg = info.get("pegRatio")
            eps_growth = info.get("earningsGrowth")
            eps_qoq = info.get("earningsQuarterlyGrowth")
            payout = info.get("payoutRatio")
            revenue_growth = info.get("revenueGrowth")
            insider = info.get("heldPercentInsiders")
            institutional = info.get("heldPercentInstitutions")

            # RSI
            delta = hist['Close'].diff()
            gain, loss = delta.clip(lower=0), -delta.clip(upper=0)
            rs = gain.rolling(14).mean() / loss.rolling(14).mean()
            rsi_14 = 100 - (100 / (1 + rs)).iloc[-1] if not rs.empty else None

            # ATR
            tr = pd.concat([
                hist['High'] - hist['Low'],
                abs(hist['High'] - hist['Close'].shift()),
                abs(hist['Low'] - hist['Close'].shift())
            ], axis=1).max(axis=1)
            atr_14 = tr.rolling(14).mean().iloc[-1] if not tr.empty else None

            # Gap and YTD Return
            gap = hist['Open'].iloc[-1] - \
                hist['Close'].iloc[-2] if len(hist) > 1 else None
            ytd_return = ((hist['Close'].iloc[-1] / hist['Close'].iloc[0]
                           ) - 1) * 100 if not hist.empty else None

            # Earnings Surprise (%)
            eps_surprise = None
            earnings_df = tk.earnings_dates
            if earnings_df is not None and not earnings_df.empty:
                earnings_df = earnings_df.reset_index()
                latest = earnings_df.iloc[0]
                estimate = latest['Estimate']
                reported = latest['Reported EPS']
                if pd.notnull(estimate) and pd.notnull(reported) and estimate != 0:
                    eps_surprise = ((reported - estimate) / estimate) * 100

            all_data.append({
                'Ticker': ticker,
                'Forward P/E': price / forward_eps if forward_eps else None,
                'PEG': peg,
                'EPS Growth Next Year (%)': eps_growth * 100 if eps_growth else None,
                'EPS Growth QoQ (%)': eps_qoq * 100 if eps_qoq else None,
                'EPS Surprise (%)': eps_surprise,  # ✅ Added here
                'Payout Ratio (%)': payout * 100 if payout else None,
                'Sales Growth (%)': revenue_growth * 100 if revenue_growth else None,
                'Insider Ownership (%)': insider * 100 if insider else None,
                'Institutional Ownership (%)': institutional * 100 if institutional else None,
                'RSI (14)': rsi_14,
                'ATR (14)': atr_14,
                'Gap': gap,
                'YTD Return (%)': ytd_return,
            })
        except Exception as e:
            print(f"⚠️ Error with {ticker}: {e}")
    return pd.DataFrame(all_data)
# ----------------- Validation -----------------


def validate_features(df, expected_columns):
    print("\n[Validation Report]")
    for col in expected_columns:
        if col not in df.columns:
            print(f"❌ Missing: {col}")
        elif df[col].isna().all():
            print(f"⚠️ All values missing in column: {col}")
        else:
            print(f"✅ {col}: Sample = {df[col].dropna().head(3).tolist()}")


# ----------------- Main -----------------
if __name__ == "__main__":
    base_df = load_base_data()
    base_df = add_candlestick_patterns(base_df)
    base_df = detect_smc_patterns(base_df)
    base_df = add_poc(base_df)
    base_df = add_rolling_features(base_df)
    tickers = base_df['Ticker'].unique().tolist()
    fund_df = extract_fundamental_features(tickers)
    final_df = base_df.merge(fund_df, on='Ticker', how='left')
    final_df = final_df.sort_values(['Ticker', 'Date']).reset_index(drop=True)

    # Add financial ratios
    final_df['Shares Outstanding'].replace(0, np.nan, inplace=True)
    final_df['Book Value Per Share'].replace(0, np.nan, inplace=True)
    final_df['Free Cash Flow Per Share'].replace(0, np.nan, inplace=True)
    final_df['Revenue'].replace(0, np.nan, inplace=True)
    final_df['Cash On Hand'].replace(0, np.nan, inplace=True)
    final_df['EPS - Earnings Per Share'].replace(0, np.nan, inplace=True)

    final_df['P/S'] = final_df['Market Cap'] / final_df['Revenue']
    final_df['P/B'] = final_df['Market Cap'] / \
        (final_df['Book Value Per Share'] * final_df['Shares Outstanding'])
    final_df['Price/Cash'] = final_df['Market Cap'] / final_df['Cash On Hand']
    final_df['Price/Free Cash Flow'] = final_df['Market Cap'] / \
        (final_df['Free Cash Flow Per Share'] * final_df['Shares Outstanding'])

    final_df['Gross Margin (%)'] = final_df['Gross Margin'] * 100
    final_df['Operating Margin (%)'] = final_df['Operating Margin'] * 100
    final_df['Net Profit Margin (%)'] = final_df['Net Profit Margin'] * 100
    final_df['ROA (%)'] = final_df['ROA - Return On Assets'] * 100
    final_df['ROE (%)'] = final_df['ROE - Return On Equity'] * 100
    final_df['ROI (%)'] = final_df['ROI - Return On Investment'] * 100

    final_df['Debt/Equity'] = final_df['Debt/Equity Ratio']
    final_df['Long-Term Debt/Equity'] = final_df['Long-term Debt / Capital']
    final_df['Current Ratio'] = final_df['Current Ratio']
    final_df['P/E'] = final_df['Close'] / final_df['EPS - Earnings Per Share']
    final_df['EV'] = final_df['Market Cap'] + \
        final_df['Total Liabilities'] - final_df['Cash On Hand']
    final_df['EV/EBITDA'] = final_df['EV'] / final_df['EBITDA']
    final_df['EV/Sales'] = final_df['EV'] / final_df['Revenue']
    final_df['dividend_growth'] = final_df['dividends'].pct_change() * 100
    final_df['EPS Growth TTM'] = final_df.groupby(
        'Ticker')['EPS - Earnings Per Share'].pct_change(periods=252) * 100
    final_df['EPS Growth Past 3 Years'] = final_df.groupby(
        'Ticker')['EPS - Earnings Per Share'].pct_change(periods=756) * 100
    final_df['EPS Growth Past 5 Years'] = final_df.groupby(
        'Ticker')['EPS - Earnings Per Share'].pct_change(periods=1260) * 100
    final_df['Sales Growth QoQ'] = final_df.groupby(
        'Ticker')['Revenue'].pct_change(periods=1) * 100
    final_df['Sales Growth TTM'] = final_df.groupby(
        'Ticker')['Revenue'].pct_change(periods=252) * 100
    final_df['Sales Growth Past 3 Years'] = final_df.groupby(
        'Ticker')['Revenue'].pct_change(periods=756) * 100
    final_df['Sales Growth Past 5 Years'] = final_df.groupby(
        'Ticker')['Revenue'].pct_change(periods=1260) * 100
    final_df['ROIC'] = final_df['EBIT'] / \
        (final_df['Total Assets'] - final_df['Total Current Liabilities'])
    final_df['Quick Ratio'] = (final_df['Total Current Assets'] -
                               final_df['Inventory']) / final_df['Total Current Liabilities']
    # Ensure 'Date' is datetime and data is sorted
    final_df['Date'] = pd.to_datetime(final_df['Date'])
    final_df = final_df.sort_values(['Ticker', 'Date'])

    # Extract year from Date
    final_df['Year'] = final_df['Date'].dt.year

    # Get first close price of each year per ticker
    first_close = final_df.groupby(['Ticker', 'Year'])[
        'Close'].first().rename('YTD_Base_Close')

    # Join back to original data (matches on Ticker and Year)
    final_df = final_df.join(first_close, on=['Ticker', 'Year'])

    # Compute YTD Return
    final_df['Performance YTD'] = (
        final_df['Close'] / final_df['YTD_Base_Close'] - 1) * 100
    final_df['Performance 1Y'] = final_df.groupby(
        'Ticker')['Close'].pct_change(periods=252) * 100
    final_df['Performance 3Y'] = final_df.groupby(
        'Ticker')['Close'].pct_change(periods=756) * 100
    final_df['Performance 5Y'] = final_df.groupby(
        'Ticker')['Close'].pct_change(periods=1260) * 100
    for ticker in final_df['Ticker_x']:
        
    # Validation
    validate_features(final_df, [
        "Forward P/E", "PEG", "EPS Growth (%)", "RSI (14)", "ATR (14)",
        "20D_SMA", "50D_SMA", "200D_SMA", "52W_High", "52W_Low",
        "AllTime_High", "AllTime_Low", "CHOCH", "FVG", "OrderBlock", "Liquidity", "POC", "Gap",
        "P/S", "P/B", "Price/Cash", "Price/Free Cash Flow", "P/E"
    ])

    final_df.to_csv("final_features_output.csv", index=False)
    print("\n✅ Feature engineering complete. Output saved to final_features_output.csv")
