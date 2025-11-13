import pandas as pd 
Macro_YF=pd.read_csv("D:/ETL/Macro_YF_1csv", index_col=0, parse_dates=True)
Market_cap^=
merged_df = pd.merge(
    df_1,
    df,
    how='left',
    left_on=['date', 'ticker'],
    right_on=['Date', 'Ticker']
)