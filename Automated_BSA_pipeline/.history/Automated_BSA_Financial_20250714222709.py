import yfinance as yf
import pandas as pd
import os  # ✅ Needed for reading ticker list


def safe_get(df, label):
    try:
        return df.loc[label].iloc[0]
    except Exception:
        return None


def get_ticker_list():
    path = os.path.join(os.path.dirname(__file__), 'unique_tickers.txt')
    try:
        with open(path, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print("Ticker list file not found!")
        return []


def extract_all_yahoo_financials(ticker_symbol):
    ticker = yf.Ticker(ticker_symbol)

    # Raw statements
    income = ticker.quarterly_financials
    balance = ticker.quarterly_balance_sheet
    cashflow = ticker.quarterly_cashflow
    info = ticker.info

    # Extract the date of the most recent quarter
    try:
        report_date = income.columns[0].date()
    except Exception:
        report_date = None

    # Info shortcuts
    def info_get(key): return info.get(key, None)

    # Base variables
    revenue = safe_get(income, "Total Revenue")
    net_income = safe_get(income, "Net Income")
    ebit = safe_get(income, "EBIT")
    ebitda = safe_get(income, "EBITDA")
    pre_tax_income = safe_get(income, "Pretax Income")
    op_income = safe_get(income, "Operating Income")
    cogs = safe_get(income, "Cost Of Revenue")
    op_cashflow = safe_get(cashflow, "Total Cash From Operating Activities")
    capex = -safe_get(cashflow, "Capital Expenditures") if safe_get(cashflow,
                                                                    "Capital Expenditures") else None
    fcf = op_cashflow - capex if op_cashflow and capex else None
    shares = info_get("sharesOutstanding")
    equity = safe_get(balance, "Total Stockholder Equity")
    assets = safe_get(balance, "Total Assets")
    debt = safe_get(balance, "Long Term Debt") or 0
    cash = safe_get(balance, "Cash") or 0
    inventory = safe_get(balance, "Inventory")
    receivables = safe_get(balance, "Net Receivables")
    invested_capital = equity + debt - cash if equity else None

    # Extractable or computable features
    features = {
        # --- Income Statement ---
        "Revenue": revenue,
        "Cost Of Goods Sold": cogs,
        "Gross Profit": safe_get(income, "Gross Profit"),
        "R&D Expenses": safe_get(income, "Research Development"),
        "SG&A Expenses": safe_get(income, "Selling General Administrative"),
        "Other Operating Income Or Expenses": safe_get(income, "Other Operating Expenses"),
        "Operating Expenses": safe_get(income, "Operating Expenses"),
        "Operating Income": op_income,
        "Total Non-Operating Income/Expense": safe_get(income, "Non Recurring"),
        "Pre-Tax Income": pre_tax_income,
        "Income Taxes": safe_get(income, "Income Tax Expense"),
        "Income After Taxes": safe_get(income, "Net Income From Continuing Ops"),
        "Other Income": safe_get(income, "Other Income Expense"),
        "Income From Continuous Operations": safe_get(income, "Net Income From Continuing Ops"),
        "Income From Discontinued Operations": safe_get(income, "Discontinued Operations"),
        "Net Income": net_income,
        "EBITDA": ebitda,
        "EBIT": ebit,
        "Basic Shares Outstanding": shares,
        "Shares Outstanding": shares,
        "Basic EPS": info_get("trailingEps"),
        "EPS - Earnings Per Share": info_get("trailingEps"),

        # --- Balance Sheet ---
        "Cash On Hand": cash,
        "Receivables": receivables,
        "Inventory": inventory,
        "Pre-Paid Expenses": safe_get(balance, "Other Current Assets"),
        "Other Current Assets": safe_get(balance, "Other Current Assets"),
        "Total Current Assets": safe_get(balance, "Total Current Assets"),
        "Property, Plant, And Equipment": safe_get(balance, "Property Plant Equipment"),
        "Long-Term Investments": safe_get(balance, "Long Term Investments"),
        "Goodwill And Intangible Assets": safe_get(balance, "Good Will"),
        "Other Long-Term Assets": safe_get(balance, "Other Assets"),
        "Total Long-Term Assets": None,
        "Total Assets": assets,
        "Total Current Liabilities": safe_get(balance, "Total Current Liabilities"),
        "Long Term Debt": debt,
        "Other Non-Current Liabilities": safe_get(balance, "Other Liab"),
        "Total Long Term Liabilities": safe_get(balance, "Long Term Liabilities"),
        "Total Liabilities": safe_get(balance, "Total Liab"),
        "Common Stock Net": safe_get(balance, "Common Stock"),
        "Retained Earnings (Accumulated Deficit)": safe_get(balance, "Retained Earnings"),
        "Comprehensive Income": safe_get(balance, "Comprehensive Income Net Of Tax"),
        "Other Share Holders Equity": safe_get(balance, "Other Stockholder Equity"),
        "Share Holder Equity": equity,
        "Total Liabilities And Share Holders Equity": safe_get(balance, "Total Assets"),

        # --- Cash Flow ---
        "Net Income/Loss": net_income,
        "Total Depreciation And Amortization": safe_get(cashflow, "Depreciation"),
        "Other Non-Cash Items": safe_get(cashflow, "Deferred Income Tax"),
        "Total Non-Cash Items": safe_get(cashflow, "Other Non Cash Items"),
        "Change In Accounts Receivable": safe_get(cashflow, "Change To Account Receivables"),
        "Change In Inventories": safe_get(cashflow, "Change To Inventory"),
        "Change In Accounts Payable": safe_get(cashflow, "Change To Account Payable"),
        "Change In Assets/Liabilities": safe_get(cashflow, "Change To Liabilities"),
        "Total Change In Assets/Liabilities": None,
        "Net Change In Property, Plant, And Equipment": safe_get(cashflow, "Capital Expenditures"),
        "Net Change In Intangible Assets": safe_get(cashflow, "Change To Intangibles"),
        "Net Acquisitions/Divestitures": safe_get(cashflow, "Investments In Other Ventures"),
        "Net Change In Short-term Investments": safe_get(cashflow, "Change To Investments"),
        "Net Change In Long-Term Investments": safe_get(cashflow, "Change To Investments"),
        "Net Change In Investments - Total": None,
        "Investing Activities - Other": safe_get(cashflow, "Other Investing Activities"),
        "Net Long-Term Debt": debt,
        "Net Current Debt": None,
        "Debt Issuance/Retirement Net - Total": safe_get(cashflow, "Repurchase Of Stock"),
        "Net Common Equity Issued/Repurchased": safe_get(cashflow, "Repurchase Of Stock"),
        "Net Total Equity Issued/Repurchased": safe_get(cashflow, "Total Cashflows From Financing Activities"),
        "Total Common And Preferred Stock Dividends Paid": safe_get(cashflow, "Dividends Paid"),
        "Financial Activities - Other": safe_get(cashflow, "Other Financing Activities"),
        "Cash Flow From Operating Activities": op_cashflow,
        "Cash Flow From Investing Activities": safe_get(cashflow, "Total Cashflows From Investing Activities"),
        "Cash Flow From Financial Activities": safe_get(cashflow, "Total Cashflows From Financing Activities"),
        "Net Cash Flow": safe_get(cashflow, "Change In Cash"),
        "Stock-Based Compensation": safe_get(cashflow, "Stock Based Compensation"),
        "Common Stock Dividends Paid": safe_get(cashflow, "Dividends Paid"),

        # --- Ratios & Computables ---
        "Current Ratio": info_get("currentRatio"),
        "Long-term Debt / Capital": debt / (debt + equity) if debt and equity else None,
        "Debt/Equity Ratio": info_get("debtToEquity"),
        "Gross Margin": info_get("grossMargins"),
        "Operating Margin": info_get("operatingMargins"),
        "EBIT Margin": ebit / revenue if ebit and revenue else None,
        "EBITDA Margin": ebitda / revenue if ebitda and revenue else None,
        "Pre-Tax Profit Margin": pre_tax_income / revenue if pre_tax_income and revenue else None,
        "Net Profit Margin": net_income / revenue if net_income and revenue else None,
        "Asset Turnover": revenue / assets if revenue and assets else None,
        "Inventory Turnover Ratio": cogs / inventory if cogs and inventory else None,
        "Receivable Turnover": revenue / receivables if revenue and receivables else None,
        "Days Sales In Receivables": 365 / (revenue / receivables) if revenue and receivables else None,
        "ROE - Return On Equity": info_get("returnOnEquity"),
        "Return On Tangible Equity": None,
        "ROA - Return On Assets": info_get("returnOnAssets"),
        "ROI - Return On Investment": info_get("returnOnInvestment"),
        "Book Value Per Share": info_get("bookValue"),
        "Operating Cash Flow Per Share": op_cashflow / shares if op_cashflow and shares else None,
        "Free Cash Flow Per Share": fcf / shares if fcf and shares else None,
    }

    df = pd.DataFrame(features.items(), columns=["Metric", "Value"])
    # ✅ Add date from quarterly financials
    df["Date"] = income.columns[0].date() if not income.empty else None
    return df


if __name__ == "__main__":
    main_df = pd.read_csv(
        'C:/Users/nss_1/Pictures/Automated_BSA_pipeline/Test_dataset.csv',
        sep=';',
        on_bad_lines='skip'
    )

    print(f"📄 Loaded dataset with shape: {main_df.shape}")
    print(
        f"📌 Sample tickers from dataset: {main_df['Ticker'].dropna().unique()[:10]}")

    ticker_list = get_ticker_list()
    print(
        f"🧾 Tickers to fetch: {ticker_list[:10]} (total: {len(ticker_list)})")

    unique_tickers = set(main_df["Ticker"].dropna().unique())
    ticker_list = [t for t in ticker_list if t in unique_tickers]

    print(
        f"✅ Tickers found in main_df: {ticker_list[:10]} (filtered total: {len(ticker_list)})")

    for ticker in ticker_list:
        print(f"\n🔄 Processing {ticker}")
        try:
            df = extract_all_yahoo_financials(ticker)
            if df.empty:
                print(f"⚠️ No financial data for {ticker}")
                continue
        except Exception as e:
            print(f"❌ Failed to extract financials for {ticker}: {e}")
            continue

        sub_df = main_df[main_df["Ticker"] == ticker].copy()
        if sub_df.empty:
            print(f"⚠️ No matching rows in main_df for {ticker}")
            continue

        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        sub_df["Date"] = pd.to_datetime(sub_df["Date"], errors="coerce")

        df = df.dropna(subset=["Date"]).sort_values("Date")
        sub_df = sub_df.dropna(subset=["Date"]).sort_values("Date")

        if df.empty or sub_df.empty:
            print(f"⚠️ Skipped {ticker} due to empty date-aligned data.")
            continue

        fin_wide = df.pivot(index="Date", columns="Metric",
                            values="Value").reset_index()

        merged = pd.merge_asof(
            sub_df,
            fin_wide.sort_values("Date"),
            on="Date",
            direction="backward"
        )

        new_cols = fin_wide.columns.drop("Date")
        for col in new_cols:
            if col in sub_df.columns:
                merged.drop(columns=[col], inplace=True)

        main_df.loc[merged.index, merged.columns.difference(sub_df.columns)] = merged[
            merged.columns.difference(sub_df.columns)
        ]

        print(f"✅ Merged financials for {ticker} into main_df")

    output_path = "updated_main_df.csv"
    main_df.to_csv(output_path, index=False, sep=';')
    print(f"\n📁 Saved updated dataset to '{output_path}'")
