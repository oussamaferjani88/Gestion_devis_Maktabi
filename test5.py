import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

# -----------------------------
# Fichier Excel existant
# -----------------------------
EXCEL_PATH = r"C:\Users\NESSIM\Desktop\scrapping web\test3_updated.xlsx"

# -----------------------------
# User-Agent
# -----------------------------
HEADERS = {
    'User-Agent': 'Mozilla/5.0'
}

# -----------------------------
# Charger les produits et attributs
# -----------------------------
products_df = pd.read_excel(EXCEL_PATH, sheet_name='Sheet3')
attributs_df = pd.read_excel(EXCEL_PATH, sheet_name='Sheet4')

print(f"✅ Produits : {len(products_df)} trouvés")
print(f"✅ Attributs : {len(attributs_df)} trouvés")

# -----------------------------
# Préparer un mapping attributs
# -----------------------------
attribute_lookup = {}
for _, row in attributs_df.iterrows():
    key = (row['nom'].strip(), row['sous_categorie_id'])
    attribute_lookup[key] = row['id']

# -----------------------------
# Collecte des valeurs EAV
# -----------------------------
valeurs_list = []
for _, product in products_df.iterrows():
    produit_id = product['id']
    sous_categorie_id = product['sous_categorie_id']
    url = product['url']

    if pd.isnull(url):
        continue

    try:
        print(f"➡️ Lecture produit : {url}")
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code != 200:
            print(f"⚠️ Erreur HTTP {response.status_code}")
            continue

        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', id='product-attribute-specs-table')
        if not table:
            continue

        for row in table.select('tbody tr'):
            th = row.select_one('th')
            td = row.select_one('td')
            if th and td:
                key = th.text.strip()
                value = td.text.strip()

                if key and value:
                    lookup_key = (key, sous_categorie_id)
                    attribut_id = attribute_lookup.get(lookup_key)
                    if attribut_id:
                        valeurs_list.append({
                            'produit_id': produit_id,
                            'attribut_id': attribut_id,
                            'valeur': value
                        })

        time.sleep(1)

    except Exception as e:
        print(f"❌ Erreur : {e}")

# -----------------------------
# Sauvegarde dans Sheet5
# -----------------------------
valeurs_df = pd.DataFrame(valeurs_list)
print(valeurs_df)

with pd.ExcelWriter(EXCEL_PATH, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
    valeurs_df.to_excel(writer, index=False, sheet_name='Sheet5')

print(f"✅ Table valeurs sauvegardée dans Sheet5 de {EXCEL_PATH}")
