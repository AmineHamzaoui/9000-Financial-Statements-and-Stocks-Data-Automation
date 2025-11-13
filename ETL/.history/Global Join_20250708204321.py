import pandas as pd

df = pd.read_csv("D:\ETL\historical_data.csv",sep=",")
df.to_parquet("your_file.parquet")