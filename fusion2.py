"""
tunisianet_to_sheet5.py
—————————
• Parcourt Sheet3 : sélectionne UNIQUEMENT les lignes créées pour Tunisianet
  (reference_mytek est NaN)
• Pour chacune, télécharge la page produit Tunisianet, récupère le tableau
  <dl class="data-sheet">, et ajoute dans Sheet5 les (attribut,valeur)
  déjà présents dans Sheet4 pour cette sous-catégorie.
"""

import pandas as pd, requests, time, re, unidecode
from bs4 import BeautifulSoup
from openpyxl import load_workbook

EXCEL_PATH = r"C:\Users\NESSIM\Desktop\scrapping web\test5.xlsx"
HEADERS    = {"User-Agent": "Mozilla/5.0"}
SLEEP_SEC  = 1

# ---------- 1. Charger sheets ----------
sheet3 = pd.read_excel(EXCEL_PATH, sheet_name="Sheet3")
sheet4 = pd.read_excel(EXCEL_PATH, sheet_name="Sheet4")   # id | nom | sous_categorie_id
try:
    sheet5 = pd.read_excel(EXCEL_PATH, sheet_name="Sheet5")
except ValueError:   # Sheet5 n'existe pas encore
    sheet5 = pd.DataFrame(columns=["produit_id","attribut_id","valeur"])

# ---------- 2. Lookup attributs déjà connus ----------
attr_lookup = { (r["nom"].strip(), int(r["sous_categorie_id"])) : int(r["id"])
                for _, r in sheet4.iterrows() }

# ---------- 3. Parcourir produits Tunisianet ----------
new_valeurs = []
tunis_rows  = sheet3[sheet3["reference_mytek"].isna()]

for _, prod in tunis_rows.iterrows():
    prod_id   = int(prod["id"])
    sous_id   = int(prod["sous_categorie_id"])
    url       = prod["url_tunisianet"]

    if pd.isna(url):
        continue
    print(f"🔎 Produit {prod_id} → {url}")

    try:
        html = requests.get(url, headers=HEADERS, timeout=10).text
    except Exception as e:
        print(f"   ❌ Connexion : {e}"); continue

    soup = BeautifulSoup(html, "html.parser")
    for dt in soup.select("section.product-features dl.data-sheet dt.name"):
        key   = dt.get_text(strip=True)
        dd    = dt.find_next("dd", class_="value")
        value = dd.get_text(strip=True) if dd else None
        if not value: continue

        attr_id = attr_lookup.get( (key, sous_id) )
        if attr_id:
            new_valeurs.append({
                "produit_id"  : prod_id,
                "attribut_id" : attr_id,
                "valeur"      : value
            })
    time.sleep(SLEEP_SEC)

# ---------- 4. Ajouter dans Sheet5 ----------
if new_valeurs:
    sheet5 = pd.concat([sheet5, pd.DataFrame(new_valeurs)], ignore_index=True)

    with pd.ExcelWriter(EXCEL_PATH,
                        mode="a", engine="openpyxl",
                        if_sheet_exists="overlay") as w:
        sheet5.to_excel(w, index=False, sheet_name="Sheet5")

    print(f"✅ {len(new_valeurs)} valeurs EAV ajoutées à Sheet5.")
else:
    print("ℹ️  Aucun nouvel attribut commun trouvé.")
