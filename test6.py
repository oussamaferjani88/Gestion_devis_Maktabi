import pandas as pd
import requests
from bs4 import BeautifulSoup
import os

# ✅ Update this to YOUR path
EXCEL_PATH = r"C:\Users\NESSIM\Desktop\scrapping web\test3_updated.xlsx"

# ------------------------------
# 1️⃣ Load the Master Product List
# ------------------------------
print("📥 Loading Sheet3 (all products)...")
df_all = pd.read_excel(EXCEL_PATH, sheet_name='Sheet3')
print(f"✅ Total products in Sheet3: {len(df_all)}")

# ------------------------------
# 2️⃣ Load Existing Scraped Data
# ------------------------------
try:
    df_scraped = pd.read_excel(EXCEL_PATH, sheet_name='Sheet5')
    print(f"✅ Already scraped products in Sheet5: {len(df_scraped)}")
except ValueError:
    print("⚠️ Sheet5 not found. Creating new.")
    df_scraped = pd.DataFrame(columns=['produit_id', 'URL', 'Attribute', 'Value'])

# ------------------------------
# 3️⃣ Define the Test Range
# ------------------------------
TEST_RANGE_IDS = set(range(1, 11))
scraped_ids = set(df_scraped['produit_id'].unique())
missing_ids = sorted(TEST_RANGE_IDS - scraped_ids)

print(f"✅ Missing IDs in test range: {missing_ids}")
if not missing_ids:
    print("🎉 No missing IDs in range 1–10. Nothing to scrape!")
    exit()

# ------------------------------
# 4️⃣ Filter URLs for Missing IDs
# ------------------------------
missing_products = df_all[df_all['id'].isin(missing_ids)]
print(f"✅ Found {len(missing_products)} URLs to scrape.")

# ------------------------------
# 5️⃣ Scraping Function
# ------------------------------
def scrape_product(url):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        )
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            print(f"❌ Failed {url} - Status {resp.status_code}")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")

        results = []

        # ✔️ Description
        desc_block = soup.select_one("div.product.attribute.overview div.value")
        description = desc_block.get_text(strip=True) if desc_block else "Not found"
        results.append(("Description", description))

        # ✔️ Title
        title_block = soup.select_one("h1.page-title span")
        title = title_block.get_text(strip=True) if title_block else "Not found"
        results.append(("Title", title))

        # ✔️ Price
        price_block = soup.select_one("span.price")
        price = price_block.get_text(strip=True) if price_block else "Not found"
        results.append(("Price", price))

        return results

    except Exception as e:
        print(f"❌ Error scraping {url}: {e}")
        return []

# ------------------------------
# 6️⃣ Scrape Missing Products
# ------------------------------
new_rows = []

for idx, row in missing_products.iterrows():
    pid = row['id']
    url = row['url']
    print(f"🔎 Scraping produit_id={pid} url={url}")
    attributes = scrape_product(url)

    for attr, value in attributes:
        new_rows.append({
            'produit_id': pid,
            'URL': url,
            'Attribute': attr,
            'Value': value
        })

print(f"✅ Scraped {len(new_rows)} new attribute rows.")

# ------------------------------
# 7️⃣ Append to Sheet5
# ------------------------------
if new_rows:
    df_new = pd.DataFrame(new_rows)
    print(df_new)

    with pd.ExcelWriter(EXCEL_PATH, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
        df_combined = pd.concat([df_scraped, df_new])
        df_combined.to_excel(writer, sheet_name='Sheet5', index=False)
    print(f"✅ Sheet5 updated with new data!")

else:
    print("⚠️ No new data to append.")
