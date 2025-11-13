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
merged_df['Ticker'] = merged_df['Ticker'].astype(str).str.strip()

# === Step 4: Load quarterly financial statement data ===
financial_data = pd.read_csv(
    "D:/ETL/final_financial_data.csv",
    sep=";",
    low_memory=False
)
financial_data['date'] = pd.to_datetime(financial_data['date'], dayfirst=True)
financial_data.rename(columns={'ticker': 'Ticker'}, inplace=True)
financial_data['Ticker'] = financial_data['Ticker'].astype(str).str.strip()

# === Step 5: Remove bad ticker groups (unsorted dates) ===
unsorted_tickers = []
for ticker, group in merged_df.groupby("Ticker"):
    if not group['Date'].is_monotonic_increasing:
        unsorted_tickers.append(ticker)

if unsorted_tickers:
    print(
        f"\n❌ Dropping {len(unsorted_tickers)} tickers with unsorted dates: {unsorted_tickers[:5]}...")
    merged_df = merged_df[~merged_df['Ticker'].isin(unsorted_tickers)]

# === Step 6: Clean missing or empty tickers ===
merged_df = merged_df[merged_df['Ticker'].notnull()]
merged_df = merged_df[merged_df['Ticker'].str.strip() != '']

financial_data = financial_data[financial_data['Ticker'].notnull()]
financial_data = financial_data[financial_data['Ticker'].str.strip() != '']

# === Step 7: Final sorting for merge_asof ===
merged_df = merged_df.sort_values(by=['Ticker', 'Date']).reset_index(drop=True)
financial_data = financial_data.sort_values(
    by=['Ticker', 'date']).reset_index(drop=True)

# === Step 8: Final monotonicity check ===
merged_check = merged_df.groupby("Ticker")['Date'].apply(
    lambda x: x.is_monotonic_increasing).all()
finance_check = financial_data.groupby("Ticker")['date'].apply(
    lambda x: x.is_monotonic_increasing).all()

print("✅ Merged DataFrame sorted correctly:", merged_check)
print("✅ Financial Data sorted correctly:", finance_check)

if not (merged_check and finance_check):
    raise ValueError(
        "❌ Sorting failed: some tickers have non-increasing dates.")

# === Step 9: Point-in-time join (merge_asof) ===
df_merged = pd.merge_asof(
    merged_df,
    financial_data,
    left_on='Date',
    right_on='date',
    by='Ticker',
    direction='backward'
)

# === Step 10: Clean and export ===
df_merged.drop(columns=['date'], inplace=True)
df_merged.to_csv("stock_with_fundamentals.csv", index=False)

print("✅ Merged DataFrame saved as 'stock_with_fundamentals.csv'")
