import pandas as pd

# === Step 1: Load macroeconomic + Yahoo Finance market data ===
print("Loading Macro_YF data...")
Macro_YF = pd.read_csv(
    "D:/ETL/Macro_YF_1.csv",
    sep=";",
    on_bad_lines='warn',
    low_memory=False
)
Macro_YF['date'] = pd.to_datetime(Macro_YF['date'], dayfirst=True)
print(f"Macro_YF loaded with {len(Macro_YF)} rows")

# === Step 2: Load daily market cap data ===
print("Loading Market_cap data...")
Market_cap = pd.read_csv(
    "D:/ETL/all_market_caps.csv",
    sep=";",
    low_memory=False
)
Market_cap['date'] = pd.to_datetime(Market_cap['date'], dayfirst=True)
print(f"Market_cap loaded with {len(Market_cap)} rows")

# === Step 3: Merge daily macro and market cap data ===
print("Merging Macro_YF and Market_cap...")
merged_df = pd.merge(
    Macro_YF,
    Market_cap,
    how='left',
    on=['date', 'ticker']
)
print(f"Merged DataFrame has {len(merged_df)} rows")

# Prepare for merge_asof
merged_df['Date'] = pd.to_datetime(merged_df['date'], dayfirst=True)
merged_df.rename(columns={'ticker': 'Ticker'}, inplace=True)
merged_df['Ticker'] = merged_df['Ticker'].astype(str).str.strip()

# === Step 4: Load quarterly financial statement data ===
print("Loading financial data...")
financial_data = pd.read_csv(
    "D:/ETL/final_financial_data.csv",
    sep=";",
    low_memory=False
)
financial_data['date'] = pd.to_datetime(financial_data['date'], dayfirst=True)
financial_data.rename(columns={'ticker': 'Ticker'}, inplace=True)
financial_data['Ticker'] = financial_data['Ticker'].astype(str).str.strip()
print(f"Financial data loaded with {len(financial_data)} rows")

# === Step 5: Remove bad ticker groups (unsorted dates) ===
print("Checking for unsorted dates...")
unsorted_tickers = []
for ticker, group in merged_df.groupby("Ticker"):
    if not group['Date'].is_monotonic_increasing:
        unsorted_tickers.append(ticker)

if unsorted_tickers:
    print(f"❌ Dropping {len(unsorted_tickers)} tickers with unsorted dates: {unsorted_tickers[:5]}...")
    merged_df = merged_df[~merged_df['Ticker'].isin(unsorted_tickers)]
    print(f"After removal, {len(merged_df)} rows remain")

# === Step 6: Verify and clean financial_data ===
print("Checking financial_data for unsorted dates...")
financial_unsorted = []
for ticker, group in financial_data.groupby("Ticker"):
    if not group['date'].is_monotonic_increasing:
        financial_unsorted.append(ticker)

if financial_unsorted:
    print(f"❌ Dropping {len(financial_unsorted)} tickers from financial_data with unsorted dates: {financial_unsorted[:5]}...")
    financial_data = financial_data[~financial_data['Ticker'].isin(financial_unsorted)]
    print(f"After removal, financial_data has {len(financial_data)} rows")

# === Step 7: Final sort (full DataFrame) ===
print("Performing final sorting...")
merged_df = merged_df.sort_values(by=['Ticker', 'Date'], ascending=[True, True]).reset_index(drop=True)
financial_data = financial_data.sort_values(by=['Ticker', 'date'], ascending=[True, True]).reset_index(drop=True)

# === Step 8: Final safety checks ===
print("Running final validation checks...")

# Check 1: Verify merged_df is sorted properly
merged_check = merged_df.groupby("Ticker")['Date'].apply(lambda x: x.is_monotonic_increasing)
if not merged_check.all():
    bad_tickers = merged_check[~merged_check].index.tolist()
    print(f"❌ Critical Error: These tickers still have non-increasing dates: {bad_tickers[:10]}...")
    raise ValueError("Merged DataFrame contains tickers with unsorted dates")

# Check 2: Verify financial_data is sorted properly
financial_check = financial_data.groupby("Ticker")['date'].apply(lambda x: x.is_monotonic_increasing)
if not financial_check.all():
    bad_tickers = financial_check[~financial_check].index.tolist()
    print(f"❌ Critical Error: These financial data tickers have non-increasing dates: {bad_tickers[:10]}...")
    raise ValueError("Financial DataFrame contains tickers with unsorted dates")

# Check 3: Verify no duplicate dates within tickers
dup_dates_merged = merged_df.duplicated(subset=['Ticker', 'Date'], keep=False)
if dup_dates_merged.any():
    print(f"⚠ Warning: Found {dup_dates_merged.sum()} duplicate date entries in merged_df")
    # Optionally: merged_df = merged_df.drop_duplicates(subset=['Ticker', 'Date'])

dup_dates_financial = financial_data.duplicated(subset=['Ticker', 'date'], keep=False)
if dup_dates_financial.any():
    print(f"⚠ Warning: Found {dup_dates_financial.sum()} duplicate date entries in financial_data")
    # Optionally: financial_data = financial_data.drop_duplicates(subset=['Ticker', 'date'])

# === Step 9: Point-in-time join (merge_asof) ===
print("Performing merge_asof...")
try:
    df_merged = pd.merge_asof(
        merged_df,
        financial_data,
        left_on='Date',
        right_on='date',
        by='Ticker',
        direction='backward'
    )
    print("✅ Merge successful!")
except Exception as e:
    print("❌ Merge failed with error:")
    print(str(e))
    print("\nDebugging info:")
    print(f"merged_df dates range: {merged_df['Date'].min()} to {merged_df['Date'].max()}")
    print(f"financial_data dates range: {financial_data['date'].min()} to {financial_data['date'].max()}")
    print("Sample merged_df dates:", merged_df['Date'].head().tolist())
    print("Sample financial_data dates:", financial_data['date'].head().tolist())
    raise

# === Step 10: Clean and export ===
print("Final cleanup...")
df_merged.drop(columns=['date'], inplace=True)  # Drop the redundant column
df_merged.to_csv("stock_with_fundamentals.csv", index=False)

print(f"✅ Success! Merged DataFrame with {len(df_merged)} rows saved as 'stock_with_fundamentals.csv'")