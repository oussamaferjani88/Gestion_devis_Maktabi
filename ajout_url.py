import pandas as pd
import requests
from bs4 import BeautifulSoup
import time

# -----------------------------
# Path to your Excel file
# -----------------------------
EXCEL_PATH = r"C:\Users\NESSIM\Desktop\scrapping web\test3_updated.xlsx"

# -----------------------------
# User-Agent
# -----------------------------
HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0 Safari/537.36'
    )
}

# -----------------------------
# Load existing Sheet3
# -----------------------------
df_existing = pd.read_excel(EXCEL_PATH, sheet_name='Sheet3')
print(f"✅ Loaded Sheet3 with {len(df_existing)} products.")

# Ensure 'url' column exists
if 'url' not in df_existing.columns:
    df_existing['url'] = None

# -----------------------------
# Define category listing pages
# (same IDs as in your test3.py)
# -----------------------------
sous_categories = [
    {"id": 1, "url": "https://www.mytek.tn/impression/imprimantes.html"},
    {"id": 2, "url": "https://www.mytek.tn/impression/photocopieurs.html"},
    {"id": 3, "url": "https://www.mytek.tn/impression/scanners.html"},
    {"id": 4, "url": "https://www.mytek.tn/informatique/ordinateur-de-bureau.html"},
    {"id": 5, "url": "https://www.mytek.tn/informatique/ordinateurs-portables.html"},
    {"id": 6, "url": "https://www.mytek.tn/informatique/serveurs.html"},
    {"id": 7, "url": "https://www.mytek.tn/gaming/gaming-pc.html"},
    {"id": 8, "url": "https://www.mytek.tn/image-son/projection/video-projecteurs.html"},
    {"id": 9, "url": "https://www.mytek.tn/telephonie-tunisie/telephone-fixe.html"},
]

# -----------------------------
# Collect real URLs from site
# -----------------------------
found_products = []

for cat in sous_categories:
    sous_cat_id = cat['id']
    base_url = cat['url']
    page = 1

    while True:
        # Build pagination URL
        if page == 1:
            url = base_url
        else:
            url = f"{base_url}?p={page}"

        print(f"➡️ Fetching category page {page} - {url}")

        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
        except Exception as e:
            print(f"❌ Connection error: {e}")
            break

        if resp.status_code != 200:
            print(f"⚠️ HTTP {resp.status_code} - stopping this category.")
            break

        soup = BeautifulSoup(resp.text, 'html.parser')
        items = soup.select('li.item.product.product-item')

        if not items:
            print(f"✅ No more items for category ID {sous_cat_id}.")
            break

        for item in items:
            # ✅ Real product URL
            link_elem = item.select_one('a.product-item-link')
            product_url = link_elem['href'] if link_elem else None

            # ✅ Name
            name_elem = item.select_one('a.product-item-link')
            name = name_elem.text.strip() if name_elem else None

            # ✅ Reference
            ref_elem = item.select_one('div.skuDesktop')
            reference = ref_elem.text.strip().replace('[','').replace(']','') if ref_elem else None

            if reference or name:
                found_products.append({
                    'sous_categorie_id': sous_cat_id,
                    'reference': reference,
                    'nom': name,
                    'url': product_url
                })

        page += 1
        time.sleep(1)  # Be polite!

print(f"✅ Collected {len(found_products)} products with real URLs from site.")

# -----------------------------
# Build DataFrame of found products
# -----------------------------
df_found = pd.DataFrame(found_products)

# -----------------------------
# Merge back onto existing Sheet3
# Priority: match by reference
# -----------------------------
merged = pd.merge(
    df_existing,
    df_found[['reference', 'url']],
    on='reference',
    how='left',
    suffixes=('', '_new')
)

# Use 'url_new' where available
merged['url_final'] = merged['url_new']
merged.loc[merged['url_final'].isnull(), 'url_final'] = merged['url']

# Drop helper columns
merged.drop(columns=['url', 'url_new'], inplace=True)

# Rename back to 'url'
merged.rename(columns={'url_final': 'url'}, inplace=True)

# -----------------------------
# Fallback: also try name match for remaining blanks
# -----------------------------
needs_name_match = merged['url'].isnull()

if needs_name_match.any():
    print(f"⚠️ {needs_name_match.sum()} products missing URLs after reference match. Trying name match.")
    to_fill = merged.loc[needs_name_match]

    matched_by_name = pd.merge(
        to_fill.drop(columns=['url']),
        df_found[['nom', 'url']].rename(columns={'nom': 'nom_match'}),
        left_on='nom',
        right_on='nom_match',
        how='left'
    )

    merged.loc[needs_name_match, 'url'] = matched_by_name['url']

# -----------------------------
# Save updated Sheet3 back
# -----------------------------
with pd.ExcelWriter(EXCEL_PATH, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
    merged.to_excel(writer, index=False, sheet_name='Sheet3')

print(f"✅ Sheet3 updated with real product URLs in: {EXCEL_PATH}")
