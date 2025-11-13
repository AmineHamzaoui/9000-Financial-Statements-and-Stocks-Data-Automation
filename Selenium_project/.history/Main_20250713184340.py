import subprocess
import multiprocessing
import logging
import os
from datetime import datetime

# === SETUP LOGGING ===
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"main_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    filename=log_file,
    filemode='w',
    format='%(asctime)s | %(levelname)s | %(message)s',
    level=logging.INFO
)

def run_script(script_path, description):
    print(f"\n🚀 Running: {description}")
    logging.info(f"STARTED: {description}")
    try:
        subprocess.run(["python", script_path], check=True)
        logging.info(f"✅ COMPLETED: {description}")
    except subprocess.CalledProcessError as e:
        print(f"❌ Script failed: {script_path}")
        logging.error(f"❌ FAILED: {description} | Exit Code: {e.returncode}")
    except Exception as e:
        print(f"❌ Error running {script_path}: {e}")
        logging.exception(f"❌ EXCEPTION in {description}")

# === SECTION 1: SCRAPING (Run in Parallel) ===
scraping_scripts = [
    ("market_cap_extraction.py", "📊 Market Cap Extraction"),
    ("financials_extraction.py", "📄 Financials Extraction"),
    ("stock_data_extraction.py", "📈 Stock Price & Splits Extraction")
]

print("⚙️ Starting parallel scraping scripts...")

scraping_processes = []
for script, desc in scraping_scripts:
    p = multiprocessing.Process(target=run_script, args=(script, desc))
    p.start()
    scraping_processes.append(p)

# Wait for all scraping scripts to finish
for p in scraping_processes:
    p.join()

print("✅ All scraping scripts finished.")

# === SECTION 2: ETL (Sequential) ===
etl_scripts = [
    ("ETL_market_cap.py", "🔄 ETL: Market Cap JSON → CSV"),
    ("ETL_Financials_BSA.py", "🔄 ETL: Financial JSON → CSV"),
    ("ETL_SD_SS.py", "🔄 ETL: Stock Data & Splits JSON → CSV")
]

print("⚙️ Starting ETL scripts in parallel...")

etl_processes = []
for script, desc in etl_scripts:
    p = multiprocessing.Process(target=run_script, args=(script, desc))
    p.start()
    etl_processes.append(p)

# Wait for all ETL scripts to finish
for p in etl_processes:
    p.join()

print("✅ All ETL scripts finished.")

# === SECTION 3: JOINING (Sequential in Requested Order) ===
joining_scripts = [
    ("Big _Join_Between_Macrotrends_YF.py", "🔗 Final Join: Macrotrends + Yahoo"),
    ("Last_joins_MC_Fin.py", "🔗 Join: Macro + Financials")
]

for script, desc in joining_scripts:
    run_script(script, desc)

# === SECTION 4: FEATURE ENGINEERING (Final Step) ===
run_script("Future_feature_ETL.py", "🧠 Final Feature Engineering")

# === DONE ===
print("\n🎉 Pipeline finished successfully. Check logs for full trace.")
