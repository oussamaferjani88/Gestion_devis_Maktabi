import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

# -----------------------------
# Chemin UNIQUE du fichier Excel
# -----------------------------
EXCEL_PATH = r"C:\Users\NESSIM\Desktop\scrapping web\test9.xlsx"

# -----------------------------
# Configuration
# -----------------------------
BASE_DOMAIN = "https://www.mytek.tn/reseaux-securite/onduleurs"
HEADERS = {'User-Agent': 'Mozilla/5.0'}

# -----------------------------
# 1️⃣ Lire le mapping officiel depuis la feuille SSC de test8.xlsx
# -----------------------------
df_mapping = pd.read_excel(EXCEL_PATH, sheet_name='SSC')
df_mapping = df_mapping[
    (df_mapping['categorie_id'] == 6) &
    (df_mapping['sous_categorie_id'] == 10)
]

# Créer le dictionnaire nom -> id officiel
nom_to_id_officiel = dict(zip(df_mapping['nom'], df_mapping['id']))
print(f"✅ Mapping chargé depuis feuille SSC ({len(nom_to_id_officiel)} sous-sous-catégories)")

# -----------------------------
# 2️⃣ Liste des sous-sous-catégories à scraper
# -----------------------------
sous_sous_categories = [
    {"nom": "Onduleur Off-Line", "slug": "onduleur-off-line.html"},
    {"nom": "Onduleur On-Line", "slug": "onduleur-on-line.html"},
    {"nom": "Onduleur In-Line", "slug": "onduleur-in-line.html"},
    {"nom": "Multiprises", "slug": "multiprises.html"},
    {"nom": "Batterie", "slug": "batterie-onduleur.html"},
    {"nom": "Accessoires Onduleur", "slug": "accessoires-onduleur.html"},
]

# -----------------------------
# 3️⃣ Charger les données existantes
# -----------------------------
try:
    df_exist = pd.read_excel(EXCEL_PATH, sheet_name='Sheet3')
    print(f"✅ Données existantes chargées : {len(df_exist)} lignes")
except Exception as e:
    print(f"⚠️ Erreur lecture Excel : {e}")
    df_exist = pd.DataFrame(columns=[
        'id',
        'reference_mytek',
        'nom',
        'sous_categorie_id',
        'sous_sous_categorie_id',
        'mytek_avant_remise',
        'mytek_apres_remise',
        'url_mytek',
        'reference_tunisianet',
        'tunisianet_avant_remise',
        'tunisianet_apres_remise',
        'url_tunisianet'
    ])

# -----------------------------
# 4️⃣ Scraping des nouveaux onduleurs
# -----------------------------
produits = []
produit_id_start = df_exist['id'].max() + 1 if not df_exist.empty else 1
produit_id = produit_id_start
sous_categorie_id = 10

for sous_sous in sous_sous_categories:
    sous_sous_nom = sous_sous["nom"]
    try:
        sous_sous_id_officiel = nom_to_id_officiel[sous_sous_nom]
    except KeyError:
        print(f"⚠️ ATTENTION : Nom non trouvé dans mapping (SSC) : {sous_sous_nom}")
        continue

    base_url = f"{BASE_DOMAIN}/{sous_sous['slug']}"
    print(f"\n=== Scraping sous-sous-catégorie ID officiel {sous_sous_id_officiel}: {sous_sous_nom} ===")

    page = 1
    while True:
        if page == 1:
            url = base_url
        else:
            url = f"{base_url}?p={page}"
        
        print(f"➡️ Page {page}: {url}")
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            response.raise_for_status()
        except Exception as e:
            print(f"⚠️ Erreur : {e}")
            break

        soup = BeautifulSoup(response.text, 'html.parser')
        items = soup.select('li.item.product.product-item')
        if not items:
            print(f"✅ Fin des pages pour {sous_sous_nom}")
            break

        for item in items:
            try:
                name_elem = item.select_one('h2.product.name.product-item-name')
                name = name_elem.text.strip() if name_elem else None

                ref_elem = item.select_one('div.skuDesktop')
                reference = ref_elem.text.strip().replace('[','').replace(']','') if ref_elem else None

                old_price_elem = item.select_one('.old-price .price')
                prix_avant_remise = old_price_elem.text.strip() if old_price_elem else None

                new_price_elem = item.select_one('.special-price .price')
                if new_price_elem:
                    prix_apres_remise = new_price_elem.text.strip()
                else:
                    new_price_elem = item.select_one('.price')
                    prix_apres_remise = new_price_elem.text.strip() if new_price_elem else None

                link_elem = item.select_one('a.product.photo.product-item-photo')
                url_mytek = link_elem.get('href') if link_elem else None

                if name:
                    produits.append({
                        'id': produit_id,
                        'reference_mytek': reference,
                        'nom': name,
                        'sous_categorie_id': sous_categorie_id,
                        'sous_sous_categorie_id': sous_sous_id_officiel,
                        'mytek_avant_remise': prix_avant_remise,
                        'mytek_apres_remise': prix_apres_remise,
                        'url_mytek': url_mytek,
                        'reference_tunisianet': None,
                        'tunisianet_avant_remise': None,
                        'tunisianet_apres_remise': None,
                        'url_tunisianet': None
                    })
                    produit_id += 1

            except Exception as e:
                print(f"❌ Erreur sur un produit : {e}")

        page += 1
        time.sleep(1)

# -----------------------------
# 5️⃣ Append et sauvegarde
# -----------------------------
if produits:
    df_new = pd.DataFrame(produits)
    df_result = pd.concat([df_exist, df_new], ignore_index=True)
    with pd.ExcelWriter(EXCEL_PATH, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        df_result.to_excel(writer, index=False, sheet_name='Sheet3')
    print(f"\n✅ {len(df_new)} nouveaux produits ajoutés. Total maintenant : {len(df_result)} lignes.")
else:
    print("\n⚠️ Aucun nouveau produit onduleur trouvé ou ajouté.")
