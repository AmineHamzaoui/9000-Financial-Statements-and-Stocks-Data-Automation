import os
import pandas as pd
import numpy as np
import yfinance as yf
import talib
from tqdm import tqdm
from smartmoneyconcepts import smc

# ----------------- Config -----------------
INPUT_FILE = 'C:\Users\nss_1\Pictures\Automated_BSA_pipeline\Test_dataset.csv'
FINAL_OUTPUT = 'final_features_output.csv'
OUTPUT_PARQUET = 'intermediate_features.parquet'
CHUNKSIZE = 100_000


def validate_features(df, expected_columns):
    print("\n[Validation Report]")
    for col in expected_columns:
        if col not in df.columns:
            print(f"❌ Missing: {col}")
        elif df[col].isna().all():
            print(f"⚠️ All values missing in column: {col}")
        else:
            sample = df[col].dropna().head(3).tolist()
            print(f"✅ {col}: Sample = {sample}")


def load_new_data():
    df = pd.read_csv(INPUT_FILE, parse_dates=['date_x'], sep=';')
    df.sort_values(['Ticker_x', 'date_x'], inplace=True)

    if os.path.exists(FINAL_OUTPUT):
        prev = pd.read_csv(FINAL_OUTPUT, parse_dates=['date_x'], sep=';')
        merged = df.merge(prev[['Ticker_x', 'date_x']], on=['Ticker_x', 'date_x'], how='left', indicator=True)
        new_df = df[merged['_merge'] == 'left_only']
        print(f"🆕 Found {len(new_df)} new rows to process.")
        return new_df
    else:
        print("⚠️ No previous output found. Processing full dataset.")
        return df


def add_candlestick_patterns(df):
    pattern_funcs = {name: getattr(talib, name) for name in dir(talib) if name.startswith('CDL')}
    results = []
    for ticker, group in tqdm(df.groupby('Ticker_x'), desc="Adding candlestick patterns"):
        open_, high_, low_, close_ = group['Open'].values, group['High'].values, group['Low'].values, group['Close'].values
        for name, func in pattern_funcs.items():
            try:
                group[name] = func(open_, high_, low_, close_)
            except Exception:
                group[name] = 0
        results.append(group)
    return pd.concat(results, ignore_index=True)


def detect_smc_patterns(df, swing_length=5):
    results = []
    for ticker, group in df.groupby('Ticker_x'):
        group = group.sort_values('date_x').reset_index(drop=True)
        ohlc = group[['Open', 'High', 'Low', 'Close', 'Volume']].rename(
            columns={'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'})
        swing = smc.swing_highs_lows(ohlc, swing_length=swing_length)
        group['swing'] = swing['HighLow']
        group['swing_level'] = swing['Level']
        bos_choch = smc.bos_choch(ohlc, swing, close_break=True)
        group['BOS'] = bos_choch['BOS']
        group['CHOCH'] = bos_choch['CHOCH']
        group['structure_level'] = bos_choch['Level']
        group['structure_broken_index'] = bos_choch['BrokenIndex']
        fvg = smc.fvg(ohlc, join_consecutive=False)
        group['FVG'] = fvg['FVG']
        group['FVG_Top'] = fvg['Top']
        group['FVG_Bottom'] = fvg['Bottom']
        group['FVG_MitigatedIndex'] = fvg['MitigatedIndex']
        ob = smc.ob(ohlc, swing, close_mitigation=False)
        group['OrderBlock'] = ob['OB']
        group['OB_Top'] = ob['Top']
        group['OB_Bottom'] = ob['Bottom']
        group['OB_Volume'] = ob['OBVolume']
        group['OB_Percentage'] = ob['Percentage']
        liq = smc.liquidity(ohlc, swing, range_percent=0.01)
        group['Liquidity'] = liq['Liquidity']
        group['Liquidity_Level'] = liq['Level']
        group['Liquidity_End'] = liq['End']
        group['Liquidity_Swept'] = liq['Swept']
        results.append(group)
    return pd.concat(results).reset_index(drop=True)


def add_poc(df, window=50):
    df['POC'] = df.groupby('Ticker_x')['Close'].transform(
        lambda x: x.rolling(window).apply(lambda s: s.mode().iloc[0] if not s.mode().empty else np.nan))
    return df


def add_rolling_features(df):
    grouped = df.groupby('Ticker_x')
    for win in [20, 50, 200]:
        df[f'{win}D_SMA'] = grouped['Close'].transform(lambda x: x.rolling(win).mean())
        df[f'{win}D_High'] = grouped['Close'].transform(lambda x: x.rolling(win).max())
        df[f'{win}D_Low'] = grouped['Close'].transform(lambda x: x.rolling(win).min())
    df['52W_High'] = grouped['Close'].transform(lambda x: x.rolling(252).max())
    df['52W_Low'] = grouped['Close'].transform(lambda x: x.rolling(252).min())
    df['AllTime_High'] = grouped['Close'].transform(lambda x: x.expanding().max())
    df['AllTime_Low'] = grouped['Close'].transform(lambda x: x.expanding().min())
    df['Daily Change %'] = grouped['Close'].pct_change() * 100
    df['Change from Open %'] = (df['Close'] - df['Open']) / df['Open'] * 100
    return df


def extract_yahoo_metrics(tickers):
    all_data = []
    for ticker in tqdm(tickers, desc="Extracting Yahoo metrics"):
        try:
            tk = yf.Ticker(ticker)
            info = tk.info
            hist = tk.history(period="max", interval="1d")

            delta = hist['Close'].diff()
            gain = delta.clip(lower=0)
            loss = -delta.clip(upper=0)
            rs = gain.rolling(14).mean() / loss.rolling(14).mean()
            rsi_14 = 100 - (100 / (1 + rs)).iloc[-1] if not rs.empty else None

            tr = pd.concat([
                hist['High'] - hist['Low'],
                abs(hist['High'] - hist['Close'].shift()),
                abs(hist['Low'] - hist['Close'].shift())
            ], axis=1).max(axis=1)
            atr_14 = tr.rolling(14).mean().iloc[-1] if not tr.empty else None

            gap = hist['Open'].iloc[-1] - hist['Close'].iloc[-2] if len(hist) > 1 else None
            ytd_return = ((hist['Close'].iloc[-1] / hist['Close'].iloc[0]) - 1) * 100 if not hist.empty else None

            eps_surprise = None
            try:
                edf = tk.earnings_dates
                if edf is not None and not edf.empty:
                    edf = edf.reset_index()
                    if 'Estimate' in edf.columns and 'Reported EPS' in edf.columns:
                        estimate = edf.iloc[0]['Estimate']
                        reported = edf.iloc[0]['Reported EPS']
                        if pd.notnull(estimate) and pd.notnull(reported) and estimate != 0:
                            eps_surprise = (reported - estimate) / estimate * 100
            except:
                pass

            beta = info.get('beta')

            all_data.append({
                'Ticker_x': ticker,
                'RSI (14)': rsi_14,
                'ATR (14)': atr_14,
                'Gap': gap,
                'YTD Return (%)': ytd_return,
                'EPS Surprise (%)': eps_surprise,
                'Beta': beta
            })

        except Exception as e:
            print(f"⚠️ Error {ticker}: {e}")
    return pd.DataFrame(all_data)


def process_chunk(chunk):
    chunk = add_candlestick_patterns(chunk)
    chunk = detect_smc_patterns(chunk)
    chunk = add_poc(chunk)
    chunk = add_rolling_features(chunk)
    return chunk


if __name__ == "__main__":
    print("🚀 Starting incremental feature engineering pipeline...")

    if os.path.exists(OUTPUT_PARQUET):
        os.remove(OUTPUT_PARQUET)
        print("🧹 Removed previous intermediate file.")

    new_data = load_new_data()
    if new_data.empty:
        print("✅ No new data to process. Exiting.")
        exit()

    reader = [new_data]

    for i, chunk in enumerate(tqdm(reader, desc="🔄 Processing new data")):
        try:
            processed = process_chunk(chunk)
            processed.to_parquet(
                OUTPUT_PARQUET,
                engine='fastparquet',
                compression='snappy',
                index=False,
                append=(i > 0)
            )
        except Exception as e:
            print(f"[Chunk {i}] ❌ Failed: {e}")

    try:
        base_df = pd.read_parquet(OUTPUT_PARQUET, engine='fastparquet')
        print(f"📦 Loaded base_df with shape: {base_df.shape}")
    except Exception as e:
        print(f"❌ Error reading Parquet: {e}")
        exit()

    try:
        tickers = base_df['Ticker_x'].unique().tolist()
        yahoo_df = extract_yahoo_metrics(tickers)
        final_df = base_df.merge(yahoo_df, on='Ticker_x', how='left')
    except Exception as e:
        print(f"❌ Error merging Yahoo metrics: {e}")
        exit()

    try:
        final_df.sort_values(['Ticker_x', 'date_x'], inplace=True)
        final_df.replace(0, np.nan, inplace=True)

        final_df['P/S'] = final_df['Market Cap'] / final_df['Revenue']
        final_df['P/B'] = final_df['Market Cap'] / (final_df['Book Value Per Share'] * final_df['Shares Outstanding'])
        final_df['Price/Cash'] = final_df['Market Cap'] / final_df['Cash On Hand']
        final_df['Price/Free Cash Flow'] = final_df['Market Cap'] / (final_df['Free Cash Flow Per Share'] * final_df['Shares Outstanding'])
        final_df['Gross Margin (%)'] = final_df['Gross Margin'] * 100
        final_df['Operating Margin (%)'] = final_df['Operating Margin'] * 100
        final_df['Net Profit Margin (%)'] = final_df['Net Profit Margin'] * 100
        final_df['ROA (%)'] = final_df['ROA - Return On Assets'] * 100
        final_df['ROE (%)'] = final_df['ROE - Return On Equity'] * 100
        final_df['ROI (%)'] = final_df['ROI - Return On Investment'] * 100
        final_df['P/E'] = final_df['Close'] / final_df['EPS - Earnings Per Share']
        final_df['EV'] = final_df['Market Cap'] + final_df['Total Liabilities'] - final_df['Cash On Hand']
        final_df['EV/EBITDA'] = final_df['EV'] / final_df['EBITDA']
        final_df['EV/Sales'] = final_df['EV'] / final_df['Revenue']
        final_df['dividend_growth'] = final_df['Dividends'].pct_change() * 100
    except Exception as e:
        print(f"❌ Error in financial ratios: {e}")
        exit()

    try:
        final_df['Date'] = pd.to_datetime(final_df['date_x'])
        final_df.sort_values(['Ticker_x', 'Date'], inplace=True)
        final_df['Year'] = final_df['Date'].dt.year
        first_close = final_df.groupby(['Ticker_x', 'Year'])['Close'].first().rename('YTD_Base_Close')
        final_df = final_df.join(first_close, on=['Ticker_x', 'Year'])
        final_df['Performance YTD'] = (final_df['Close'] / final_df['YTD_Base_Close'] - 1) * 100
        final_df['Performance 1Y'] = final_df.groupby('Ticker_x')['Close'].pct_change(periods=252) * 100
        final_df['Performance 3Y'] = final_df.groupby('Ticker_x')['Close'].pct_change(periods=756) * 100
        final_df['Performance 5Y'] = final_df.groupby('Ticker_x')['Close'].pct_change(periods=1260) * 100
    except Exception as e:
        print(f"❌ Error in performance metrics: {e}")
        exit()

    try:
        if os.path.exists(FINAL_OUTPUT):
            existing_df = pd.read_csv(FINAL_OUTPUT, parse_dates=['date_x'], sep=';')
            combined_df = pd.concat([existing_df, final_df], ignore_index=True)
            combined_df.drop_duplicates(subset=['Ticker_x', 'date_x'], keep='last', inplace=True)
        else:
            combined_df = final_df

        validate_features(combined_df, ['P/S', 'P/B', 'EV/EBITDA', 'EV/Sales', 'P/E'])
        combined_df.to_csv(FINAL_OUTPUT, sep=';', index=False)
        print(f"\n✅ Feature engineering complete. Output saved to: {FINAL_OUTPUT}")
    except Exception as e:
        print(f"❌ Final saving failed: {e}")
