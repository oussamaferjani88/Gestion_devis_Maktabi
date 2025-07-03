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
print(f"✅ Sheet5 (values): {len(existing_value_ids)} unique produit_ids already scraped")
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
# Build attribute lookup
# -----------------------------
attribute_lookup = {}
for _, row in attributes_df.iterrows():
    key = (row['nom'].strip(), row['sous_categorie_id'])
    attribute_lookup[key] = row['id']

print(f"✅ Built attribute lookup with {len(attribute_lookup)} entries.")

# -----------------------------
# Headers
# -----------------------------
HEADERS = {'User-Agent': USER_AGENT}

# -----------------------------
# Scraping function
# -----------------------------
def scrape_attributes_and_values(url):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code != 200:
            print(f"⚠️ HTTP {resp.status_code} for {url}")
            return []
        soup = BeautifulSoup(resp.text, 'html.parser')
        table = soup.find('table', id='product-attribute-specs-table')
        if not table:
            return []

        result = []
        for row in table.select('tbody tr'):
            th = row.select_one('th')
            td = row.select_one('td')
            if th and td:
                key = th.text.strip()
                value = td.text.strip()
                if key and value:
                    result.append((key, value))
        return result
    except Exception as e:
        print(f"❌ Error scraping {url}: {e}")
        return []

# -----------------------------
# Collect new EAV rows
# -----------------------------
new_values = []

for _, product in to_rescan_df.iterrows():
    produit_id = product['id']
    sous_categorie_id = product['sous_categorie_id']
    url = product['url']

    if pd.isnull(url) or not isinstance(url, str) or not url.startswith('http'):
        print(f"⚠️ Skipping invalid URL for produit_id={produit_id}")
        continue

    print(f"➡️ Scraping produit_id={produit_id} URL={url}")
    attr_values = scrape_attributes_and_values(url)

    for attr_name, valeur in attr_values:
        lookup_key = (attr_name, sous_categorie_id)
        attribut_id = attribute_lookup.get(lookup_key)

        if attribut_id:
            new_values.append({
                'produit_id': produit_id,
                'attribut_id': attribut_id,
                'valeur': valeur
            })
            print(f"✅ Added: produit_id={produit_id}, attribut_id={attribut_id}, valeur='{valeur}'")
        else:
            print(f"⚠️ Attribute '{attr_name}' not found in Sheet4 for sous_categorie_id={sous_categorie_id}")

    time.sleep(DELAY_SECONDS)

# -----------------------------
# Combine with existing Sheet5
# -----------------------------
if new_values:
    print(f"✅ Found {len(new_values)} new value rows!")
    df_new_values = pd.DataFrame(new_values)
    if 'values_df' in locals() and not values_df.empty:
        final_values_df = pd.concat([values_df, df_new_values], ignore_index=True)
    else:
        final_values_df = df_new_values
else:
    print("✅ No new values found. Nothing to add.")
    if 'values_df' in locals():
        final_values_df = values_df
    else:
        final_values_df = pd.DataFrame(columns=['produit_id', 'attribut_id', 'valeur'])

# -----------------------------
# Save back to Excel
# -----------------------------
with pd.ExcelWriter(EXCEL_PATH, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
    final_values_df.to_excel(writer, index=False, sheet_name='Sheet5')

print(f"✅ Sheet5 updated in {EXCEL_PATH} with {len(final_values_df)} total rows.")
