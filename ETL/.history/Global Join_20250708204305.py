import pandas as pd

df = pd.read_csv("your_file.csv",sep=",")
df.to_parquet("your_file.parquet")