import pandas as pd

# Assume df is your DataFrame

# Price/Earnings (P/E)
df['P/E'] = df['Close'] / df['EPS - Earnings Per Share']

# Forward P/E → NOT available directly unless you have forward EPS
""" 
import yfinance as yf
import pandas as pd
import os

def get_ticker_list():
    path = os.path.join(os.path.dirname(__file__), 'unique_tickers.txt')
    try:
        with open(path, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print("❌ Ticker list file not found!")
        return []

def get_forward_eps_dataframe():
    tickers = get_ticker_list()
    data = []

    for t in tickers:
        try:
            ticker = yf.Ticker(t)
            forward_eps = ticker.info.get("forwardEps")
            data.append({"Ticker": t, "Forward EPS [Next Year]": forward_eps})
        except Exception as e:
            print(f"⚠️ Error retrieving data for {t}: {e}")
            data.append({"Ticker": t, "Forward EPS [Next Year]": None})

    return pd.DataFrame(data)
"""

# PEG → Needs forward EPS and expected growth — NOT computable from this data
"""
import yfinance as yf
import pandas as pd
import os

def get_ticker_list():
    path = os.path.join(os.path.dirname(__file__), 'unique_tickers.txt')
    try:
        with open(path, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print("❌ Ticker list file not found!")
        return []

def get_peg_dataframe():
    tickers = get_ticker_list()
    data = []

    for t in tickers:
        try:
            ticker = yf.Ticker(t)
            info = ticker.info

            pe = info.get("trailingPE")
            eps_growth = info.get("earningsGrowth")  # Typically 5-year CAGR

            peg = (pe / (eps_growth * 100)) if pe and eps_growth else None

            data.append({
                "Ticker": t,
                "PEG Ratio": peg,
                "P/E Ratio": pe,
                "EPS Growth [5Y]": eps_growth * 100 if eps_growth else None
            })
        except Exception as e:
            print(f"⚠️ Error retrieving data for {t}: {e}")
            data.append({
                "Ticker": t,
                "PEG Ratio": None,
                "P/E Ratio": None,
                "EPS Growth [5Y]": None
            })

    return pd.DataFrame(data)"""
# Price/Sales (P/S)
"""
import yfinance as yf
import pandas as pd
import os

def get_ticker_list():
    path = os.path.join(os.path.dirname(__file__), 'unique_tickers.txt')
    try:
        with open(path, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print("❌ Ticker list file not found!")
        return []

def get_dividend_growth_dataframe():
    tickers = get_ticker_list()
    data = []

    for t in tickers:
        try:
            ticker = yf.Ticker(t)
            dividends = ticker.dividends

            if dividends.empty:
                raise ValueError("No dividend data")

            annual_div = dividends.resample("Y").sum()

            if len(annual_div) >= 4:
                d_now = annual_div.iloc[-1]
                d_3y_ago = annual_div.iloc[-4]

                growth = ((d_now / d_3y_ago) ** (1/3)) - 1 if d_3y_ago > 0 else None
                data.append({
                    "Ticker": t,
                    "Dividend Growth [3Y] (%)": round(growth * 100, 2) if growth is not None else None,
                    "Dividend (This Year)": round(d_now, 4),
                    "Dividend (3Y Ago)": round(d_3y_ago, 4)
                })
            else:
                data.append({
                    "Ticker": t,
                    "Dividend Growth [3Y] (%)": None,
                    "Dividend (This Year)": None,
                    "Dividend (3Y Ago)": None
                })

        except Exception as e:
            print(f"⚠️ Error for {t}: {e}")
            data.append({
                "Ticker": t,
                "Dividend Growth [3Y] (%)": None,
                "Dividend (This Year)": None,
                "Dividend (3Y Ago)": None
            })

    return pd.DataFrame(data)

# Example usage
if __name__ == "__main__":
    df_div_growth = get_dividend_growth_dataframe()
    print(df_div_growth)"""
    """""
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