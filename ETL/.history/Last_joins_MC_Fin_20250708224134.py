import pandas as pd 
Macro_YF=pd.read_csv("D:/ETL/Macro_YF_1csv", index_col=0, parse_dates=True)
Market_cap= pd.read_csv("D:/ETL/all_market_caps.csv", sep=",", low_memory=False)
merged_df = pd.merge(
    Macro_YF,
    Market_cap,
    how='left',
    left_on=['date', 'ticker'],
    right_on=['date', 'ticker']
)





# Sort both by date and ticker — REQUIRED for merge_asof
merged_df.sort_values(['Ticker', 'Date'], inplace=True)
financial_data=pd.read_csv("D:/ETL/final_financial_data.csv", sep=",", low_memory=False)
financial_data.sort_values(['Ticker', 'date'], inplace=True)

# Perform asof merge: for each (Ticker, Date) in stock, match the most recent fundamentals
df_merged = pd.merge_asof(
    merged_df,
    df_fundamentals,
    left_on='Date',
    right_on='date',
    by='Ticker',
    direction='backward'
)

# Drop the 'date' column from fundamentals if not needed
df_merged.drop(columns=['date'], inplace=True)

# Optional: save to CSV
df_merged.to_csv("stock_with_fundamentals.csv", index=False)

print("✅ Merged DataFrame saved as 'stock_with_fundamentals.csv'")

















"""