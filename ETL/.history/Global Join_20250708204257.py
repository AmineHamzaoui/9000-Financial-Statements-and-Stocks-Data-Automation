import pandas as pd

df = pd.read_csv("your_file.csv")
df.to_parquet("your_file.parquet")