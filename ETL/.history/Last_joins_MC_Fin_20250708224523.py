import pandas as pd

# === Step 1: Load macroeconomic + Yahoo Finance market data ===
# Assume 'Macro_YF' has daily trading data: 'date', 'ticker', 'close', etc.
Macro_YF = pd.read_csv("D:/ETL/Macro_YF_1.csv
                       ", index_col=0, parse_dates=True)

# Load daily market cap data: contains 'date', 'ticker', 'market_cap'
Market_cap = pd.read_csv("D:/ETL/all_market_caps.csv",
                         sep=",", low_memory=False)

# Ensure date formats are consistent
Macro_YF['date'] = pd.to_datetime(Macro_YF['date'])
Market_cap['date'] = pd.to_datetime(Market_cap['date'])

# === Step 2: Merge daily macro and market cap data ===
# This is a regular left join on trading days and ticker
merged_df = pd.merge(
    Macro_YF,
    Market_cap,
    how='left',
    left_on=['date', 'ticker'],
    right_on=['date', 'ticker']
)

# === Step 3: Load financial statement data (quarterly fundamentals) ===
# Assume 'financial_data' has columns: 'date' (quarter end), 'Ticker', 'EPS', 'Revenue', etc.
financial_data = pd.read_csv(
    "D:/ETL/final_financial_data.csv", sep=",", low_memory=False)

# Convert dates to datetime format for merge_asof
merged_df['Date'] = pd.to_datetime(merged_df['date'])
financial_data['date'] = pd.to_datetime(financial_data['date'])

# Standardize column names for consistency
merged_df.rename(columns={'ticker': 'Ticker'}, inplace=True)
financial_data.rename(columns={'ticker': 'Ticker'}, inplace=True)

# === Step 4: Sort for merge_asof (REQUIRED) ===
merged_df.sort_values(['Ticker', 'Date'], inplace=True)
financial_data.sort_values(['Ticker', 'date'], inplace=True)

# === Step 5: Point-in-time join (merge_asof) ===
# For each (Ticker, Date) in merged_df, find the most recent financials <= that date
df_merged = pd.merge_asof(
    merged_df,
    financial_data,
    left_on='Date',
    right_on='date',
    by='Ticker',
    direction='backward'
)

# === Step 6: Clean and export ===
# Drop the extra 'date' column from financials (keep 'Date' from daily data)
df_merged.drop(columns=['date'], inplace=True)

# Save the result to CSV
df_merged.to_csv("stock_with_fundamentals.csv", index=False)

print("✅ Merged DataFrame saved as 'stock_with_fundamentals.csv'")
