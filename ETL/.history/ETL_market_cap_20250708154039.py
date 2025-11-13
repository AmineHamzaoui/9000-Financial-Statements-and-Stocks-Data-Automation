import os
import json
import pandas as pd


def process_market_cap_files(directory):
    all_data = []

    for filename in os.listdir(directory):
        if filename.endswith("_market_cap.json"):
            ticker = filename.split("_")[0]
            filepath = os.path.join(directory, filename)

            with open(filepath, 'r') as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError:
                    print(f"Skipping {filename}: JSON Decode Error")
                    continue

            df = pd.DataFrame(data)
            df['ticker'] = ticker
            df.rename(columns={'v1': 'Market Cap'}, inplace=True)
            all_data.append(df)

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        return final_df
    else:
        return pd.DataFrame(columns=['date', 'Market Cap', 'ticker'])


# Example usage
directory = "./your_folder_path_here"  # replace with the actual path
df_market_caps = process_market_cap_files(directory)

# Save to CSV
df_market_caps.to_csv("all_market_caps.csv", index=False)

# Display the first few rows
print(df_market_caps.head())
