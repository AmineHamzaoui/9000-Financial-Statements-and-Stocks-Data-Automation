import pandas as pd

# === Step 1: Load macroeconomic + Yahoo Finance market data ===
Macro_YF = pd.read_csv(
    "D:/ETL/Macro_YF_1.csv",
    sep=";",
    on_bad_lines='warn',
    low_memory=False
)
Macro_YF['date'] = pd.to_datetime(Macro_YF['date'], dayfirst=True)

# === Step 2: Load daily market cap data ===
Market_cap = pd.read_csv(
    "D:/ETL/all_market_caps.csv",
    sep=";",
    low_memory=False
)
Market_cap['date'] = pd.to_datetime(Market_cap['date'], dayfirst=True)

# === Step 3: Merge daily macro and market cap data ===
merged_df = pd.merge(
    Macro_YF,
    Market_cap,
    how='left',
    on=['date', 'ticker']
)

# Prepare for merge_asof
merged_df['Date'] = pd.to_datetime(merged_df['date'], dayfirst=True)
merged_df.rename(columns={'ticker': 'Ticker'}, inplace=True)

# === Step 4: Load quarterly financial statement data ===
financial_data = pd.read_csv(
    "D:/ETL/final_financial_data.csv",
    sep=";",
    low_memory=False
)
financial_data['date'] = pd.to_datetime(financial_data['date'], dayfirst=True)
financial_data.rename(columns={'ticker': 'Ticker'}, inplace=True)

# === Step 5: Sort for merge_asof ===
merged_df = merged_df.sort_values(by=['Ticker', 'Date']).reset_index(drop=True)
financial_data = financial_data.sort_values(
    by=['Ticker', 'date']).reset_index(drop=True)

# ✅ Safety check
if not merged_df['Date'].is_monotonic_increasing:
    raise ValueError("merged_df['Date'] is not sorted properly")
if not financial_data['date'].is_monotonic_increasing:
    raise ValueError("financial_data['date'] is not sorted properly")

# === Step 6: Point-in-time join (merge_asof) ===
df_merged = pd.merge_asof(
    merged_df,
    financial_data,
    left_on='Date',
    right_on='date',
    by='Ticker',
    direction='backward'
)

# === Step 7: Clean and export ===
df_merged.drop(columns=['date'], inplace=True)  # drop financials' date
df_merged.to_csv("stock_with_fundamentals.csv", index=False)

print("✅ Merged DataFrame saved as 'stock_with_fundamentals.csv'")
