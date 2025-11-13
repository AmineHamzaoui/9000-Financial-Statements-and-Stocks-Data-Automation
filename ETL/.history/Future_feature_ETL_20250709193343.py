import pandas as pd

# Assume df is your DataFrame

# Price/Earnings (P/E)
df['P/E'] = df['Close'] / df['EPS - Earnings Per Share']

# Forward P/E → NOT available directly unless you have forward EPS
""" 
import yfinance as yf
import pandas as pd

# Example list of tickers (replace with your actual list)
tickers = ["AAPL", "MSFT", "GOOGL", "AMZN"]

# Create an empty list to collect data
eps_data = []

for t in tickers:
    ticker = yf.Ticker(t)
    
    try:
        earnings_df = ticker.earnings_forecasts  # future earnings forecasts
    except Exception:
        earnings_df = None

    if earnings_df is not None and not earnings_df.empty:
        entry = {"Ticker": t}
        for year in earnings_df.index:
            if "Earnings Estimate" in earnings_df.columns:
                entry[f"Forward EPS [{year}]"] = earnings_df.at[year, "Earnings Estimate"]
        eps_data.append(entry)
    else:
        eps_data.append({"Ticker": t})

# Create a DataFrame from the collected data
forward_eps_df = pd.DataFrame(eps_data)

# Print or save the final DataFrame
print(forward_eps_df)
"""
# PEG → Needs forward EPS and expected growth — NOT computable from this data

# Price/Sales (P/S)
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