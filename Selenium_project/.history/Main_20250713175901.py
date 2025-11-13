import subprocess

scripts = [
    "market_cap_extraction.py",
    "financials_extraction.py",
    "stock_data_extraction.py"  # Rename macrotrends.py for clarity
]

for script in scripts:
    print(f"\n🚀 Running {script}...")
    result = subprocess.run(["python", script])
    if result.returncode != 0:
        print(f"❌ {script} failed with exit code {result.returncode}")
    else:
        print(f"✅ {script} completed successfully.")