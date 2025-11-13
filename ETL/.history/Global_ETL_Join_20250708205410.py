""""""
import pandas as pd

csv_file = "D:\\ETL\\historical_data.csv"
chunksize = 100_000

chunks = []  # list to hold each chunk
for chunk in pd.read_csv(csv_file, sep=",", chunksize=chunksize, low_memory=False):
    chunks.append(chunk)

df = pd.concat(chunks, ignore_index=True)
print(f"DataFrame created. Shape: {df.shape}")
