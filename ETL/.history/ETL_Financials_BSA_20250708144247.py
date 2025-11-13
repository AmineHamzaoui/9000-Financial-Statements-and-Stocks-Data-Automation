import os
import glob
import json
import pandas as pd
from bs4 import BeautifulSoup
import re


# Set paths for each folder
folders = [
    "D:/ETL/balance-sheet",
    "D:/ETL/cash-flow-statement",
    "D:/ETL/financial-ratios",
    "D:\ETL/income-statement"
]

records = []

for folder in folders:
    for filepath in glob.glob(os.path.join(folder, "*.json")):
        with open(filepath, "r", encoding="utf-8") as f:
            try:
                full_data = json.load(f)
            except json.JSONDecodeError:
                print(f"Skipping malformed file: {filepath}")
                continue

        for item in full_data:
            # Extract metric name from field_name HTML
            soup_name = BeautifulSoup(
                item.get('field_name', ''), 'html.parser')
            metric = soup_name.text.strip()

            # Extract ticker, frequency, and statement from popup_icon
            soup_popup = BeautifulSoup(
                item.get('popup_icon', ''), 'html.parser')
            div = soup_popup.find('div', class_='ajax-chart')
            if not div:
                continue

            data_attr = div.get('data-tipped-options', '')
            match = re.search(
                r"t: '([^']+)', s: '[^']+', freq: '([^']+)', statement: '([^']+)'", data_attr
            )
            if not match:
                continue

            ticker, frequency, statement = match.groups()

            # Process each date-value pair
            for date, value in item.items():
                if date in ['field_name', 'popup_icon']:
                    continue

                # Convert to float if possible, otherwise keep as None
                try:
                    value = float(value)
                except:
                    value = None

                records.append({
                    'ticker': ticker,
                    'date': date,
                    'frequency': frequency,
                    'statement': statement,
                    metric: value
                })

# Create DataFrame from all records
df = pd.DataFrame(records)

# Pivot to wide format: each metric becomes a column
df_wide = df.pivot_table(
    index=['ticker', 'date'],
    values=[col for col in df.columns if col not in [
        'ticker', 'date', 'frequency', 'statement']],
    aggfunc='first'
).reset_index()

# Sort the final table by ticker and date
df_wide['date'] = pd.to_datetime(df_wide['date'], errors='coerce')
df_wide = df_wide.sort_values(by=['ticker', 'date'])

# Final result
print(df_wide.head())

# Optional: save to CSV
df_wide.to_csv("final_financial_data.csv", index=False)
