import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

# ✅ FIX YOUR ACTUAL FILE PATH HERE
excel_path = r"C:\Users\NESSIM\Desktop\scrapping web\test3_updated.xlsx"

# ✅ Check if Excel exists
if not os.path.exists(excel_path):
    raise FileNotFoundError(f"Excel file not found at {excel_path}")

# ✅ Your target product URL
url = "https://www.mytek.tn/imprimante-multifonction-jet-d-encre-canon-pixma-mg2541s-couleur.html"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36"
}

# 1️⃣ Fetch the page
resp = requests.get(url, headers=headers)
if resp.status_code != 200:
    print(f"Error: {resp.status_code}")
    exit()

# 2️⃣ Parse HTML
soup = BeautifulSoup(resp.text, "html.parser")

# 3️⃣ Extract description
desc_block = soup.select_one("div.product.attribute.overview div.value")
if desc_block:
    description = desc_block.get_text(strip=True)
else:
    description = "Not found"

# 4️⃣ Build DataFrame
df = pd.DataFrame({
    "Attribute": ["Description"],
    "Value": [description]
})

print(df)

# 5️⃣ Save to new sheet in existing Excel
with pd.ExcelWriter(excel_path, mode="a", engine="openpyxl", if_sheet_exists="replace") as writer:
    df.to_excel(writer, sheet_name="Sheet6", index=False)

print(f"✅ Saved to {excel_path} in Sheet6")
