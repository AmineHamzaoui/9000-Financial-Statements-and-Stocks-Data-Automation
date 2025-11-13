import pandas as pd
import numpy as np

df = pd.read_csv(
    'C:/Users/nss_1/Pictures/Automated_BSA_pipeline/final_features_output.csv', sep=';')
# Assuming your DataFrame is named df and `date_y` is in datetime format or string
columns_to_clear = [
    "Asset Turnover", "Basic EPS", "Basic Shares Outstanding", "Book Value Per Share",
    "Cash Flow From Financial Activities", "Cash Flow From Investing Activities",
    "Cash Flow From Operating Activities", "Cash On Hand", "Change In Accounts Payable",
    "Change In Accounts Receivable", "Change In Assets/Liabilities", "Change In Inventories",
    "Common Stock Dividends Paid", "Common Stock Net", "Comprehensive Income",
    "Cost Of Goods Sold", "Current Ratio", "Days Sales In Receivables",
    "Debt Issuance/Retirement Net - Total", "Debt/Equity Ratio", "EBIT", "EBIT Margin",
    "EBITDA", "EPS - Earnings Per Share", "Financial Activities - Other",
    "Free Cash Flow Per Share", "Goodwill And Intangible Assets", "Gross Margin",
    "Gross Profit", "Income After Taxes", "Income From Continuous Operations",
    "Income From Discontinued Operations", "Income Taxes", "Inventory",
    "Inventory Turnover Ratio", "Investing Activities - Other", "Long Term Debt",
    "Long-Term Investments", "Long-term Debt / Capital", "Net Acquisitions/Divestitures",
    "Net Cash Flow", "Net Change In Intangible Assets", "Net Change In Investments - Total",
    "Net Change In Long-Term Investments", "Net Change In Property, Plant, And Equipment",
    "Net Change In Short-term Investments", "Net Common Equity Issued/Repurchased",
    "Net Current Debt", "Net Income", "Net Income/Loss", "Net Long-Term Debt",
    "Net Profit Margin", "Net Total Equity Issued/Repurchased", "Operating Cash Flow Per Share",
    "Operating Expenses", "Operating Income", "Operating Margin", "Other Current Assets",
    "Other Income", "Other Long-Term Assets", "Other Non-Cash Items",
    "Other Non-Current Liabilities", "Other Operating Income Or Expenses",
    "Other Share Holders Equity", "Pre-Paid Expenses", "Pre-Tax Income",
    "Pre-Tax Profit Margin", "Property, Plant, And Equipment", "ROA - Return On Assets",
    "ROE - Return On Equity", "ROI - Return On Investment", "Receivables",
    "Receiveable Turnover", "Research And Development Expenses",
    "Retained Earnings (Accumulated Deficit)", "Return On Tangible Equity", "Revenue",
    "SG&A Expenses", "Share Holder Equity", "Shares Outstanding", "Stock-Based Compensation",
    "Total Assets", "Total Change In Assets/Liabilities",
    "Total Common And Preferred Stock Dividends Paid", "Total Current Assets",
    "Total Current Liabilities", "Total Depreciation And Amortization - Cash Flow",
    "Total Liabilities", "Total Liabilities And Share Holders Equity",
    "Total Long Term Liabilities", "Total Long-Term Assets", "Total Non-Cash Items",
    "Total Non-Operating Income/Expense"
]

# If date_y is string:
df.loc[df['date_y'] == '2025-03-31', columns_to_clear] = np.nan
df.to_csv('C:/Users/nss_1/Pictures/Automated_BSA_pipeline/Test_dataset.csv', sep=';', index=False)
# If date_y is datetime and needs conversion:
# df['date_y'] = pd.to_datetime(df['date_y'])
# df.loc[df['date_y'] == pd.Timestamp("2025-03-31"), columns_to_clear] = np.nan
"""