import pandas as pd 
Macro_YF=pd.read_csv("D:/ETL/Macro_YF_1csv", index_col=0, parse_dates=True)
Market_cap= pd.read_csv("D:/ETL/all_market_caps.csv", sep=",", low_memory=False)
merged_df = pd.merge(
    Macro_YF,
    Market_cap,
    how='left',
    left_on=['date', 'ticker'],
    right_on=['Date', 'Ticker']
)