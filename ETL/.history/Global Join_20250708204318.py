import pandas as pd

df = pd.read_csv("D:\ETL\historical_data.csvv",sep=",")
df.to_parquet("your_file.parquet")