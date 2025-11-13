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
    print(
        f"❌ Dropping {len(unsorted_tickers)} tickers with unsorted dates: {unsorted_tickers[:5]}...")
    merged_df = merged_df[~merged_df['Ticker'].isin(unsorted_tickers)]
    print(f"After removal, {len(merged_df)} rows remain")

# === Step 6: Verify and clean financial_data ===
print("Checking financial_data for unsorted dates...")
financial_unsorted = []
for ticker, group in financial_data.groupby("Ticker"):
    if not group['date'].is_monotonic_increasing:
        financial_unsorted.append(ticker)

if financial_unsorted:
    print(
        f"❌ Dropping {len(financial_unsorted)} tickers from financial_data with unsorted dates: {financial_unsorted[:5]}...")
    financial_data = financial_data[~financial_data['Ticker'].isin(
        financial_unsorted)]
    print(f"After removal, financial_data has {len(financial_data)} rows")

# === Step 7: Final sort with strict validation ===
print("Performing final sorting with strict validation...")


def strict_sort(df, sort_cols):
    """Sort with additional validation checks"""
    df = df.sort_values(by=sort_cols, ascending=[True, True])
    # Verify the sort worked
    if not df[sort_cols].is_monotonic_increasing.all():
        raise ValueError("DataFrame not properly sorted after sort operation")
    return df.reset_index(drop=True)


# Sort both DataFrames
merged_df = strict_sort(merged_df, ['Ticker', 'Date'])
financial_data = strict_sort(financial_data, ['Ticker', 'date'])

# === Step 8: Additional merge_asof preparation ===
print("Preparing for merge_asof...")

# Ensure we only keep relevant columns to reduce memory usage
required_cols = ['Date', 'Ticker'] + \
    [col for col in merged_df.columns if col not in ['Date', 'Ticker', 'date']]
merged_df = merged_df[required_cols]

# Verify the left DataFrame is properly sorted for merge_asof
if not merged_df['Date'].is_monotonic_increasing:
    print("⚠ Warning: Global date sorting not monotonic - performing final sort")
    merged_df = merged_df.sort_values('Date').reset_index(drop=True)
    if not merged_df['Date'].is_monotonic_increasing:
        raise ValueError("Could not establish global date sorting")

# === Step 9: Point-in-time join (merge_asof) ===
print("Performing merge_asof...")
try:
    # Create a copy of the DataFrames to ensure original data isn't modified
    merged_df_copy = merged_df.copy()
    financial_data_copy = financial_data.copy()

    # Perform the merge
    df_merged = pd.merge_asof(
        merged_df_copy,
        financial_data_copy,
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
    print(f"First 5 merged_df dates: {merged_df['Date'].head().tolist()}")
    print(
        f"First 5 financial_data dates: {financial_data['date'].head().tolist()}")
    print(f"Last 5 merged_df dates: {merged_df['Date'].tail().tolist()}")
    print(
        f"Last 5 financial_data dates: {financial_data['date'].tail().tolist()}")

    # Additional debug: Check if dates are sorted within each ticker
    sample_ticker = merged_df['Ticker'].iloc[0]
    print(f"\nSample dates for ticker {sample_ticker}:")
    print(merged_df[merged_df['Ticker'] == sample_ticker]
          ['Date'].head(10).tolist())

    raise

# === Step 10: Clean and export ===
print("Final cleanup...")
df_merged.drop(columns=['date'], inplace=True)  # Drop the redundant column

# Ensure we don't have duplicate columns
df_merged = df_merged.loc[:, ~df_merged.columns.duplicated()]

df_merged.to_csv("stock_with_fundamentals.csv", index=False, sep=';')

print(
    f"✅ Success! Merged DataFrame with {len(df_merged)} rows saved as 'stock_with_fundamentals.csv'")
