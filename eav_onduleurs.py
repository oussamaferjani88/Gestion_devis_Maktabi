"""
eav_onduleurs.py
————————
• Parcourt Sheet3 (test9.xlsx) où reference_mytek est NaN
  ET sous_categorie_id = 10 (Onduleurs)
• Pour chaque produit Tunisianet :
    – scrape la fiche technique (<dl class="data-sheet">)
    – ajoute attributs et valeurs dans Sheet4 / Sheet5
"""

import pandas as pd, requests, time, re, unidecode
from bs4 import BeautifulSoup

EXCEL_PATH = r"C:\Users\NESSIM\Desktop\scrapping web\test9.xlsx"
HEADERS    = {"User-Agent": "Mozilla/5.0"}
SLEEP_SEC  = 1

def slug(t):
    t = unidecode.unidecode(str(t or '').lower())
    t = re.sub(r'[^a-z0-9 ]+', ' ', t)
    return re.sub(r'\s+', ' ', t).strip()

# ---------- 1. Charger Excel ----------
sheet3 = pd.read_excel(EXCEL_PATH, sheet_name="Sheet3")
sheet4 = pd.read_excel(EXCEL_PATH, sheet_name="Sheet4")
try:
    sheet5 = pd.read_excel(EXCEL_PATH, sheet_name="Sheet5")
except ValueError:
    sheet5 = pd.DataFrame(columns=["produit_id","attribut_id","valeur"])

# ---------- 2. Lookup attributs existants ----------
attr_lookup = {}          # (sid, slug) -> (attr_id, nom)
for _, r in sheet4.iterrows():
    attr_lookup[(int(r.sous_categorie_id), slug(r.nom))] = (int(r.id), r.nom)
next_attr_id = sheet4["id"].max() + 1 if not sheet4.empty else 1

new_attrs, new_vals = [], []

# ---------- 3. Parcourir produits Onduleurs Tunisianet ----------
to_process = sheet3[
    sheet3["reference_mytek"].isna() &
    (sheet3["sous_categorie_id"] == 10) &
    sheet3["url_tunisianet"].notna()
]

for _, prod in to_process.iterrows():
    pid = int(prod.id)
    sid = int(prod.sous_categorie_id)
    url = prod.url_tunisianet
    print(f"🔎 Produit {pid} → {url}")

    try:
        soup = BeautifulSoup(
            requests.get(url, headers=HEADERS, timeout=10).text,
            "html.parser"
        )
    except Exception as e:
        print(f"   ❌ {e}"); continue

    for dt in soup.select("section.product-features dl.data-sheet dt.name"):
        key_raw = dt.get_text(strip=True)
        dd      = dt.find_next("dd", class_="value")
        val     = dd.get_text(strip=True) if dd else None
        if not val: continue

        key_s = slug(key_raw)
        k     = (sid, key_s)

        if k in attr_lookup:
            aid = attr_lookup[k][0]
        else:
            aid = next_attr_id
            attr_lookup[k] = (aid, key_raw)
            new_attrs.append({"id": aid, "nom": key_raw, "sous_categorie_id": sid})
            next_attr_id += 1

        new_vals.append({"produit_id": pid, "attribut_id": aid, "valeur": val})
    time.sleep(SLEEP_SEC)

# ---------- 4. Sauvegarde ----------
if new_attrs:
    sheet4 = pd.concat([sheet4, pd.DataFrame(new_attrs)], ignore_index=True)
if new_vals:
    sheet5 = pd.concat([sheet5, pd.DataFrame(new_vals)], ignore_index=True)

with pd.ExcelWriter(EXCEL_PATH, mode="a", engine="openpyxl",
                    if_sheet_exists="replace") as w:
    sheet4.to_excel(w, index=False, sheet_name="Sheet4")
    sheet5.to_excel(w, index=False, sheet_name="Sheet5")

print(f"✅ {len(new_attrs)} attributs ajoutés, {len(new_vals)} valeurs insérées.")
