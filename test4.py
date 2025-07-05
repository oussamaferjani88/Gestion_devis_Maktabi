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
# Charger la liste des produits
# -----------------------------

products_df = pd.read_excel(EXCEL_PATH, sheet_name='Sheet3')
print(f"✅ {len(products_df)} produits trouvés dans Sheet3.")

# -----------------------------
# Collecte des attributs uniques
# -----------------------------
unique_attributes = set()

for _, row in products_df.iterrows():
    sous_categorie_id = row['sous_categorie_id']
    url = row['url']

    if pd.isnull(url):
        continue

    try:
        print(f"➡️ Lecture : {url}")
        response = requests.get(url, headers=HEADERS, timeout=10)
        print(" Contenu de la réponse:", response.text[:500])

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
                if key:
                    unique_attributes.add( (key, sous_categorie_id) )

        time.sleep(1)

    except Exception as e:
        print(f"❌ Erreur : {e}")

# -----------------------------
# Générer la table attributs
# -----------------------------
attributes_list = []
id_counter = 1
for attr, sous_cat_id in sorted(unique_attributes):
    attributes_list.append({
        'id': id_counter,
        'nom': attr,
        'sous_categorie_id': sous_cat_id
    })
    id_counter += 1

attributs_df = pd.DataFrame(attributes_list)
print(attributs_df)

# -----------------------------
# Sauvegarde dans Sheet4
# -----------------------------
with pd.ExcelWriter(EXCEL_PATH, mode='a', engine='openpyxl', if_sheet_exists='replace') as writer:
    attributs_df.to_excel(writer, index=False, sheet_name='Sheet4')

print(f"✅ Table attributs sauvegardée dans Sheet4 de {EXCEL_PATH}")
