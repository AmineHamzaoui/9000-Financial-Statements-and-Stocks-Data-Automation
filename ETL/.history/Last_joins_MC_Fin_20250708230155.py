import pandas as pd

# === Step 1: Load macroeconomic + Yahoo Finance market data ===
# Use on_bad_lines='warn' to skip malformed rows (e.g., in line 94950)
Macro_YF = pd.read_csv("D:/ETL/Macro_YF_1.csv", sep=";")

# Convert date column to datetime
Macro_YF['date'] = pd.to_datetime(Macro_YF['date'])

# Load daily market cap data: contains 'date', 'ticker', 'market_cap'
Market_cap = pd.read_csv("D:/ETL/all_market_caps.csv",
                         sep=",", low_memory=False)
Market_cap['date'] = pd.to_datetime(Market_cap['date'])

# === Step 2: Merge daily macro and market cap data ===
merged_df = pd.merge(
    Macro_YF,
    Market_cap,
    how='left',
    on=['date', 'ticker']
)

# Rename for consistency with fundamentals
merged_df['Date'] = pd.to_datetime(merged_df['date'])  # used for merge_asof
merged_df.rename(columns={'ticker': 'Ticker'}, inplace=True)

# === Step 3: Load financial statement data (quarterly fundamentals) ===
financial_data = pd.read_csv(
    "D:/ETL/final_financial_data.csv", sep=";", low_memory=False)
financial_data['date'] = pd.to_datetime(financial_data['date'])
financial_data.rename(columns={'ticker': 'Ticker'}, inplace=True)

# === Step 4: Sort for merge_asof ===
merged_df.sort_values(['Ticker', 'Date'], inplace=True)
financial_data.sort_values(['Ticker', 'date'], inplace=True)

# === Step 5: Point-in-time join (merge_asof) ===
df_merged = pd.merge_asof(
    merged_df,
    financial_data,
    left_on='Date',
    right_on='date',
    by='Ticker',
    direction='backward'
)

# === Step 6: Clean and export ===
df_merged.drop(columns=['date'], inplace=True)  # drop date from financials
df_merged.to_csv("stock_with_fundamentals.csv", index=False)

print("✅ Merged DataFrame saved as 'stock_with_fundamentals.csv'")
