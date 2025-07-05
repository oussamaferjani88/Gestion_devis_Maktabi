import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

# -------------------------------
# Paramètres
# -------------------------------
BASE_URL = "https://www.mytek.tn/impression/imprimantes.html"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
}
NUM_PAGES = 7

all_products = []

# -------------------------------
# Boucle sur toutes les pages
# -------------------------------
for page in range(1, NUM_PAGES + 1):
    if page == 1:
        url = BASE_URL
    else:
        url = f"{BASE_URL}?p={page}"

    print(f"🔎 Scraping page {page}: {url}")

    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        print(f"⚠️ Erreur {response.status_code} pour la page {page}")
        continue

    soup = BeautifulSoup(response.text, 'html.parser')

    # -------------------------------
    # Scraping des produits sur la page
    # -------------------------------
    for item in soup.select('li.item.product.product-item'):
        # Nom du produit
        name_elem = item.select_one('h2.product.name.product-item-name')
        name = name_elem.text.strip() if name_elem else None

        # Référence
        ref_elem = item.select_one('div.skuDesktop')
        reference = ref_elem.text.strip().replace('[','').replace(']','') if ref_elem else None

        # Prix avant remise
        old_price_elem = item.select_one('.old-price .price')
        old_price = old_price_elem.text.strip() if old_price_elem else None

        # Prix après remise
        new_price_elem = item.select_one('.special-price .price')
        if new_price_elem:
            new_price = new_price_elem.text.strip()
        else:
            new_price_elem = item.select_one('.price')
            new_price = new_price_elem.text.strip() if new_price_elem else None

        all_products.append({
            'Reference': reference,
            'Nom': name,
            'Prix Avant Remise': old_price,
            'Prix Après Remise': new_price
        })

    # Petit délai pour être poli avec le serveur
    time.sleep(1)

# -------------------------------
# Exporter les données dans Excel
# -------------------------------
df = pd.DataFrame(all_products)
df.to_excel('mytek_imprimantes_complet.xlsx', index=False)

print("✅ Fichier Excel généré : mytek_imprimantes_complet.xlsx")
