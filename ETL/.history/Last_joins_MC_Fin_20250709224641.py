import pandas as pd

# === Step 1: Load macroeconomic + Yahoo Finance market data ===
Macro_YF = pd.read_csv("D:/ETL/Macro_YF_1.csv", sep=";",
                       on_bad_lines='warn', low_memory=False)
Macro_YF['date'] = pd.to_datetime(Macro_YF['date'], dayfirst=True)

# === Step 2: Load daily market cap data ===
Market_cap = pd.read_csv("D:/ETL/all_market_caps.csv",
                         sep=";", low_memory=False)
Market_cap['date'] = pd.to_datetime(Market_cap['date'], dayfirst=True)

# === Step 3: Merge daily macro and market cap data ===
merged_df = pd.merge(Macro_YF, Market_cap, how='left', on=['date', 'ticker'])
merged_df['Date'] = pd.to_datetime(merged_df['date'], dayfirst=True)
merged_df.rename(columns={'ticker': 'Ticker'}, inplace=True)
merged_df['Ticker'] = merged_df['Ticker'].astype(str).str.upper().str.strip()

# === Step 4: Load quarterly financial statement data ===
financial_data = pd.read_csv(
    "D:/ETL/final_financial_data.csv", sep=";", low_memory=False)
financial_data['date'] = pd.to_datetime(financial_data['date'], dayfirst=True)
financial_data.rename(columns={'ticker': 'Ticker'}, inplace=True)
financial_data['Ticker'] = financial_data['Ticker'].astype(
    str).str.upper().str.strip()

# === Step 5: Merge-asof by Ticker
results = []
common_tickers = set(merged_df['Ticker']).intersection(
    set(financial_data['Ticker']))
print(f"🔁 Running merge_asof for {len(common_tickers)} common tickers...")

for ticker in sorted(common_tickers):
    left = merged_df[merged_df['Ticker'] == ticker].copy()
    right = financial_data[financial_data['Ticker'] == ticker].copy()

    left.sort_values(by='Date', inplace=True)
    right.sort_values(by='date', inplace=True)

    if left.empty or right.empty:
        continue

    merged = pd.merge_asof(
        left,
        right,
        left_on='Date',
        right_on='date',
        direction='backward'
    )

    merged['Ticker'] = ticker  # ✅ Ensure Ticker column is preserved
    results.append(merged)

# === Step 6: Combine results and clean
df_merged = pd.concat(results, ignore_index=True)

if 'date' in df_merged.columns:
    df_merged.drop(columns=['date'], inplace=True)

df_merged.sort_values(by=['Ticker', 'Date'], inplace=True)
df_merged.reset_index(drop=True, inplace=True)

# Optional forward fill per Ticker (if needed)
df_merged.update(df_merged.groupby('Ticker').ffill())

# === Step 7: Export
df_merged.to_csv("stock_with_fundamentals.csv", index=False, sep=';')
print("✅ Merged DataFrame saved as 'stock_with_fundamentals.csv'")
