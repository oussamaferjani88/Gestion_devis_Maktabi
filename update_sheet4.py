import pandas as pd, requests, time, re, unidecode
from bs4 import BeautifulSoup

EXCEL_PATH = r"C:\Users\NESSIM\Desktop\scrapping web\test5.xlsx"
HEADERS    = {"User-Agent": "Mozilla/5.0"}
SLEEP_SEC  = 1

# ---------- 1. Fonctions ----------
def slug(txt: str) -> str:
    txt = unidecode.unidecode(txt or "").lower()
    txt = re.sub(r'[^a-z0-9 ]+', ' ', txt)
    return re.sub(r'\s+', ' ', txt).strip()

# ---------- 2. Charger Excel ----------
sheet3 = pd.read_excel(EXCEL_PATH, sheet_name="Sheet3")
sheet4 = pd.read_excel(EXCEL_PATH, sheet_name="Sheet4")
try:
    sheet5 = pd.read_excel(EXCEL_PATH, sheet_name="Sheet5")
except ValueError:
    sheet5 = pd.DataFrame(columns=["produit_id","attribut_id","valeur"])

# ---------- 3. Lookup attributs existants ----------
attr_lookup = {}      # (sous_cat_id, slug) -> (attr_id, attr_nom)
for _, r in sheet4.iterrows():
    key = (int(r["sous_categorie_id"]), slug(r["nom"]))
    attr_lookup[key] = (int(r["id"]), r["nom"])

next_attr_id = sheet4["id"].max() + 1

# Préparer listes pour ajouts
new_attr_rows  = []
new_value_rows = []

# ---------- 4. Parcourir les produits Tunisianet ----------
tunis_products = sheet3[ sheet3["url_tunisianet"].notna() ]

for _, p in tunis_products.iterrows():
    url   = p["url_tunisianet"]
    pid   = int(p["id"])
    sid   = int(p["sous_categorie_id"])
    if pd.isna(url): continue

    print(f"🔎 Produit {pid} → {url}")
    try:
        soup = BeautifulSoup(
            requests.get(url, headers=HEADERS, timeout=10).text,
            "html.parser"
        )
    except Exception as e:
        print(f"   ❌ {e}"); continue

    # ---- 4a. Fiche technique ----
    for dt in soup.select("section.product-features dl.data-sheet dt.name"):
        key_raw  = dt.get_text(strip=True)
        dd       = dt.find_next("dd", class_="value")
        value    = dd.get_text(strip=True) if dd else None
        if not value: continue

        key_s = slug(key_raw)
        k = (sid, key_s)

        # Attribut déjà connu ?
        if k in attr_lookup:
            aid = attr_lookup[k][0]
        else:
            # Nouveau → ajouter à Sheet4
            aid = next_attr_id
            attr_lookup[k] = (aid, key_raw)
            new_attr_rows.append({
                "id": aid, "nom": key_raw, "sous_categorie_id": sid
            })
            next_attr_id += 1

        # Ajouter valeur
        new_value_rows.append({
            "produit_id": pid, "attribut_id": aid, "valeur": value
        })

    # ---- 4b. Disponibilité magasin ----
    stock_div = soup.select_one("#stock_availability")
    if stock_div:
        stock_txt = stock_div.get_text(strip=True)
        key_s = slug("Disponibilité magasin")
        k = (sid, key_s)
        if k in attr_lookup:
            aid = attr_lookup[k][0]
        else:
            aid = next_attr_id
            attr_lookup[k] = (aid, "Disponibilité magasin")
            new_attr_rows.append({
                "id": aid, "nom": "Disponibilité magasin", "sous_categorie_id": sid
            })
            next_attr_id += 1
        new_value_rows.append({
            "produit_id": pid, "attribut_id": aid, "valeur": stock_txt
        })

    time.sleep(SLEEP_SEC)

# ---------- 5. Sauvegarde ----------
if new_attr_rows:
    sheet4 = pd.concat([sheet4, pd.DataFrame(new_attr_rows)], ignore_index=True)
if new_value_rows:
    sheet5 = pd.concat([sheet5, pd.DataFrame(new_value_rows)], ignore_index=True)

with pd.ExcelWriter(EXCEL_PATH, mode="a", engine="openpyxl",
                    if_sheet_exists="replace") as w:
    sheet4.to_excel(w, index=False, sheet_name="Sheet4")
    sheet5.to_excel(w, index=False, sheet_name="Sheet5")

print(f"✅ {len(new_attr_rows)} nouveaux attributs ajoutés.")
print(f"✅ {len(new_value_rows)} valeurs EAV insérées.")
