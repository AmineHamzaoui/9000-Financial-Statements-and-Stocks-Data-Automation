import pandas as pd

# Read the CSV file
df = pd.read_csv("D:\\ETL\\historical_data.csv", sep=",", low_memory=False)

# Print confirmation
print("CSV file read successfully.")
