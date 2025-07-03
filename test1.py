import requests
from bs4 import BeautifulSoup
import pandas as pd

url = "https://www.mytek.tn/impression/imprimantes.html"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
}

response = requests.get(url, headers=headers)
if response.status_code != 200:
    print(f"Erreur {response.status_code}")
    exit()

soup = BeautifulSoup(response.text, 'html.parser')
products = []

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
        # Pas de remise -> prix normal
        new_price_elem = item.select_one('.price')
        new_price = new_price_elem.text.strip() if new_price_elem else None

    products.append({
        'Reference': reference,
        'Nom': name,
        'Prix Avant Remise': old_price,
        'Prix Après Remise': new_price
    })

# Sauvegarde dans Excel
df = pd.DataFrame(products)
df.to_excel('mytek_imprimantes.xlsx', index=False)

print("✅ Fichier Excel généré : mytek_imprimantes.xlsx")
