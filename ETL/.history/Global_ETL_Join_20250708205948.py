
import pandas as pd

csv_file = "D:\\ETL\\historical_data.csv"
chunksize = 100_000

chunks = []  # list to hold each chunk
for chunk in pd.read_csv(csv_file, sep=",", chunksize=chunksize, low_memory=False):
    chunks.append(chunk)

df = pd.concat(chunks, ignore_index=True)
print(f"DataFrame created. Shape: {df.shape}")
df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')

#---------------------Joining Macrotrends Data---------------------#
SH_SS=read_csv("D:/ETL/all_stock_data_merged.csv", sep=",", low_memory=False)
macrotrends_df = pd.read_csv("D:/ETL/market_caps.csv", sep=",", low_memory=False)