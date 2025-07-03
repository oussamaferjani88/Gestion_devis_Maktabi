import pandas as pd
import requests
from bs4 import BeautifulSoup
import time

# -----------------------------
# CONFIGURATION
# -----------------------------
EXCEL_PATH = r"C:\Users\NESSIM\Desktop\scrapping web\test3_updated.xlsx"
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36'
DELAY_SECONDS = 1

# -----------------------------
# LOAD SHEETS
# -----------------------------
print("📥 Loading Excel sheets...")
products_df = pd.read_excel(EXCEL_PATH, sheet_name='Sheet3')
try:
    values_df = pd.read_excel(EXCEL_PATH, sheet_name='Sheet5')
    existing_value_ids = set(values_df['produit_id'].unique())
except Exception:
    print("⚠️ Sheet5 not found or empty - treating as no scraped values yet.")
    existing_value_ids = set()

attributes_df = pd.read_excel(EXCEL_PATH, sheet_name='Sheet4')

print(f"✅ Sheet3 (products): {len(products_df)} rows")
print(f"✅ Sheet5 (values): {len(existing_value_ids)} unique produit_ids scraped")
print(f"✅ Sheet4 (attributes): {len(attributes_df)} rows")

# -----------------------------
# DETERMINE MISSING produit_ids
# -----------------------------
expected_ids = set(range(1, 1919))
missing_ids = sorted(expected_ids - existing_value_ids)

if not missing_ids:
    print("🎉 No missing produit_ids found! All products seem covered in Sheet5. Exiting.")
    exit()

print(f"✅ Found {len(missing_ids)} missing produit_ids to re-scrape.")

# -----------------------------
# FILTER PRODUCTS TO SCRAPE
# -----------------------------
to_rescan_df = products_df[products_df['id'].isin(missing_ids)].copy()
print(f"✅ {len(to_rescan_df)} products found in Sheet3 with corrected URLs to rescan.")

if to_rescan_df.empty:
    print("⚠️ No matching rows in Sheet3 for missing IDs. Exiting.")
    exit()

# -----------------------------
# Build existing attribute set
# -----------------------------
known_attributes = set(
    (row['nom'].strip(), row['sous_categorie_id'])
    for _, row in attributes_df.iterrows()
)

attr_id_counter = attributes_df['id'].max() + 1 if not attributes_df.empty else 1
new_attributes = []

# -----------------------------
# Headers
# -----------------------------
HEADERS = {'User-Agent': USER_AGENT}

# -----------------------------
# Scraping function
# -----------------------------
def scrape_attributes(url):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code != 200:
            print(f"⚠️ HTTP {resp.status_code} for {url}")
            return []
        soup = BeautifulSoup(resp.text, 'html.parser')
        table = soup.find('table', id='product-attribute-specs-table')
        if not table:
            return []
        attrs = []
        for row in table.select('tbody tr'):
            th = row.select_one('th')
            td = row.select_one('td')
            if th and td:
                key = th.text.strip()
                if key:
                    attrs.append(key)
        return attrs
    except Exception as e:
        print(f"❌ Error scraping {url}: {e}")
        return []

# -----------------------------
# Scrape only missing products
# -----------------------------
for _, product in to_rescan_df.iterrows():
    produit_id = product['id']
    sous_categorie_id = product['sous_categorie_id']
    url = product['url']

    if pd.isnull(url) or not isinstance(url, str) or not url.startswith('http'):
        print(f"⚠️ Skipping invalid URL for produit_id={produit_id}")
        continue

    print(f"➡️ Scraping produit_id={produit_id} URL={url}")
    attrs = scrape_attributes(url)

    for attr_name in attrs:
        attr_key = (attr_name, sous_categorie_id)
        if attr_key not in known_attributes:
            new_attributes.append({
                'id': attr_id_counter,
                'nom': attr_name,
                'sous_categorie_id': sous_categorie_id
            })
            known_attributes.add(attr_key)
            print(f"🆕 NEW attribute: '{attr_name}' (sous_categorie_id={sous_categorie_id}) as id={attr_id_counter}")
            attr_id_counter += 1

    time.sleep(DELAY_SECONDS)

# -----------------------------
# Combine with existing attributes
# -----------------------------
if new_attributes:
    print(f"✅ Found {len(new_attributes)} NEW attributes!")
    df_new = pd.DataFrame(new_attributes)
    final_attributes_df = pd.concat([attributes_df, df_new], ignore_index=True)
else:
    print("✅ No new attributes found. Nothing to add.")
    final_attributes_df = attributes_df

# -----------------------------
# Save back to Excel
# -----------------------------
with pd.ExcelWriter(EXCEL_PATH, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
    final_attributes_df.to_excel(writer, index=False, sheet_name='Sheet4')

print(f"✅ Sheet4 updated in {EXCEL_PATH} with {len(final_attributes_df)} total attributes.")
