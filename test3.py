import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

# -----------------------------
# Chemin du fichier de sortie
# -----------------------------
OUTPUT_PATH = r"C:\Users\NESSIM\Desktop\scrapping web\test3_updated.xlsx"

# -----------------------------
# Configuration User-Agent
# -----------------------------
HEADERS = {
    'User-Agent': 'Mozilla/5.0'
}

# -----------------------------
# Mapping sous-catégories avec ID et URL
# -----------------------------
sous_categories = [
    {"id": 1, "categorie": "Impression", "sous_categorie": "Imprimantes", "url": "https://www.mytek.tn/impression/imprimantes.html"},
    {"id": 2, "categorie": "Impression", "sous_categorie": "Photocopieurs", "url": "https://www.mytek.tn/impression/photocopieurs.html"},
    {"id": 3, "categorie": "Impression", "sous_categorie": "Scanners", "url": "https://www.mytek.tn/impression/scanners.html"},
    {"id": 4, "categorie": "Informatique", "sous_categorie": "Ordinateur de bureau", "url": "https://www.mytek.tn/informatique/ordinateur-de-bureau.html"},
    {"id": 5, "categorie": "Informatique", "sous_categorie": "Ordinateurs portables", "url": "https://www.mytek.tn/informatique/ordinateurs-portables.html"},
    {"id": 6, "categorie": "Informatique", "sous_categorie": "Serveurs", "url": "https://www.mytek.tn/informatique/serveurs.html"},
    {"id": 7, "categorie": "Gaming", "sous_categorie": "Gaming PC", "url": "https://www.mytek.tn/gaming/gaming-pc.html"},
    {"id": 8, "categorie": "Image & Son", "sous_categorie": "Vidéoprojecteurs", "url": "https://www.mytek.tn/image-son/projection/video-projecteurs.html"},
    {"id": 9, "categorie": "Téléphonie", "sous_categorie": "Téléphone Fixe", "url": "https://www.mytek.tn/telephonie-tunisie/telephone-fixe.html"},
]

# -----------------------------
# Résultats collectés
# -----------------------------
produits = []
produit_id = 1

# -----------------------------
# Scraping
# -----------------------------
for sous_cat in sous_categories:
    sous_cat_id = sous_cat["id"]
    base_url = sous_cat["url"]
    print(f"=== Scraping sous-catégorie ID {sous_cat_id}: {sous_cat['sous_categorie']} ===")
    
    page = 1
    while True:
        if page == 1:
            url = base_url
        else:
            url = f"{base_url}?p={page}"
        
        print(f"➡️ Page {page}: {url}")
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
        except Exception as e:
            print(f"⚠️ Erreur de connexion : {e} - on passe à la catégorie suivante.")
            break

        if response.status_code >= 500:
            print(f"⚠️ Erreur serveur HTTP {response.status_code} - probablement pas de page {page} pour cette catégorie.")
            break
        elif response.status_code != 200:
            print(f"⚠️ Erreur HTTP {response.status_code} - on arrête cette sous-catégorie.")
            break

        soup = BeautifulSoup(response.text, 'html.parser')
        items = soup.select('li.item.product.product-item')
        if not items:
            print(f"✅ Fin des pages pour {sous_cat['sous_categorie']}")
            break

        for item in items:
            try:
                # Nom du produit
                name_elem = item.select_one('h2.product.name.product-item-name')
                name = name_elem.text.strip() if name_elem else None

                # Référence SKU
                ref_elem = item.select_one('div.skuDesktop')
                reference = ref_elem.text.strip().replace('[','').replace(']','') if ref_elem else None

                # Prix avant remise
                old_price_elem = item.select_one('.old-price .price')
                prix_avant_remise = old_price_elem.text.strip() if old_price_elem else None

                # Prix après remise
                new_price_elem = item.select_one('.special-price .price')
                if new_price_elem:
                    prix_apres_remise = new_price_elem.text.strip()
                else:
                    new_price_elem = item.select_one('.price')
                    prix_apres_remise = new_price_elem.text.strip() if new_price_elem else None

                if name:
                    produits.append({
                        'id': produit_id,
                        'reference': reference,
                        'nom': name,
                        'sous_categorie_id': sous_cat_id,
                        'prix_avant_remise': prix_avant_remise,
                        'prix_apres_remise': prix_apres_remise
                    })
                    produit_id += 1

            except Exception as e:
                print(f"❌ Erreur sur un produit : {e}")

        page += 1
        time.sleep(1)

# -----------------------------
# Sauvegarde dans un NOUVEAU fichier Excel
# -----------------------------
if produits:
    df = pd.DataFrame(produits)
    with pd.ExcelWriter(OUTPUT_PATH, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet3')
    print(f"✅ Résultats sauvegardés dans : {OUTPUT_PATH}")
else:
    print("⚠️ Aucun produit n'a été trouvé pour les catégories données.")
