import pandas as pd

EXCEL_PATH = r"C:\Users\NESSIM\Desktop\scrapping web\test3_updated.xlsx"

print("📥 Loading Sheet5...")
df_sheet5 = pd.read_excel(EXCEL_PATH, sheet_name='Sheet5')
print(f"✅ Loaded {len(df_sheet5)} rows from Sheet5.")

# 1..1918 expected IDs
expected_ids = set(range(1, 1919))
existing_ids = set(df_sheet5['produit_id'].unique())
missing_ids = sorted(expected_ids - existing_ids)

print(f"✅ Found {len(missing_ids)} missing produit_ids.")

# Save to CSV for check_test5.py
if missing_ids:
    df_missing = pd.DataFrame({'id': missing_ids})
    missing_csv_path = r"C:\Users\NESSIM\Desktop\scrapping web\fixed_ids.csv"
    df_missing.to_csv(missing_csv_path, index=False)
    print(f"✅ Missing IDs saved to: {missing_csv_path}")
else:
    print("🎉 No missing IDs found. Nothing to do!")
