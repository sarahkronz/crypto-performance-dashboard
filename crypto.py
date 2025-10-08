import pandas as pd
import glob
import os

source_folder = r"D:\crypto\archive"  

output_folder = r"D:\crypto\output"  

csv_files = glob.glob(os.path.join(source_folder, "*.csv"))

if len(csv_files) == 0:
    raise FileNotFoundError("❌ لا يوجد ملفات CSV داخل المجلد المحدد. تأكدي من المسار.")

dfs = []

for file in csv_files:
    symbol = os.path.basename(file).replace(".csv", "")
    df = pd.read_csv(file)
    df["Symbol"] = symbol
    dfs.append(df)

combined_df = pd.concat(dfs, ignore_index=True)

combined_df.dropna(inplace=True)
combined_df["Date"] = pd.to_datetime(combined_df["Date"], errors="coerce")
combined_df["Year"] = combined_df["Date"].dt.year
combined_df["Month"] = combined_df["Date"].dt.month_name()
combined_df["Daily Change %"] = ((combined_df["Close"] - combined_df["Open"]) / combined_df["Open"]) * 100


monthly_avg = combined_df.groupby(["Symbol", "Year", "Month"]).agg({
    "Open": "mean",
    "Close": "mean",
    "Volume": "mean",
    "Daily Change %": "mean"
}).reset_index()

combined_path = os.path.join(output_folder, "combined_crypto_data.csv")
monthly_path = os.path.join(output_folder, "monthly_avg_crypto.csv")

os.makedirs(output_folder, exist_ok=True)

combined_df.to_csv(combined_path, index=False)
monthly_avg.to_csv(monthly_path, index=False)

