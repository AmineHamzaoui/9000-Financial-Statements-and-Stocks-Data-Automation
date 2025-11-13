import pandas as pd

csv_file = "D:\\ETL\\historical_data.csv"

# Read in chunks of 100,000 rows
chunksize = 100_000
row_count = 0

for chunk in pd.read_csv(csv_file, sep=",", chunksize=chunksize, low_memory=False):
    row_count += len(chunk)

print(f"CSV file read successfully in chunks. Total rows: {row_count}")