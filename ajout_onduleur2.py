"""
fusion_onduleurs.py
————————
• Scrape la catégorie Onduleurs de Tunisianet
• Fait la fusion avec Sheet3 de test9.xlsx
• Ajoute reference/prix/url Tunisianet OU crée un nouveau produit si
  la référence n’existe pas côté Mytek.
"""

import requests, re, time, unidecode, pandas as pd
from bs4 import BeautifulSoup
from rapidfuzz import fuzz

# ---------- CONFIG ----------
EXCEL_PATH = r"C:\Users\NESSIM\Desktop\scrapping web\test9.xlsx"
SHEET_NAME = "Sheet3"
HEADERS    = {"User-Agent": "Mozilla/5.0"}
BASE_URL   = "https://www.tunisianet.com.tn/380-onduleur"
MAX_PAGES  = 30
SLEEP_SEC  = 1
SOUS_CATEGORIE_ID = 10          # Onduleurs
# -----------------------------

def clean_ref(r):
    return re.sub(r'[^a-z0-9]', '', unidecode.unidecode(str(r or '').lower()))

def price_float(t):
    try: return float(re.sub(r'[^\d,.]', '', t).replace(',', '.'))
    except: return None

# ---------- 1. Charger Excel ----------
# a) feuille SSC pour récupérer mapping nom → sous_sous_categorie_id
ssc = pd.read_excel(EXCEL_PATH, sheet_name="SSC")
ssc_onduleur = ssc[ssc["sous_categorie_id"] == SOUS_CATEGORIE_ID]
nom_to_sscid = {r.nom.strip(): int(r.id) for _, r in ssc_onduleur.iterrows()}

# b) Sheet3 existant (ou vide)
try:
    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)
except ValueError:
    df = pd.DataFrame(columns=[
        'id','reference_mytek','nom','sous_categorie_id','sous_sous_categorie_id',
        'mytek_avant_remise','mytek_apres_remise','url_mytek',
        'reference_tunisianet','tunisianet_avant_remise','tunisianet_apres_remise',
        'url_tunisianet'
    ])

df["ref_clean"] = df["reference_mytek"].apply(clean_ref)
next_id = int(df["id"].max() or 0) + 1

# ---------- 2. Scraper Onduleurs Tunisianet ----------
records = []
for page in range(1, MAX_PAGES+1):
    url = f"{BASE_URL}?page={page}"
    print(f"🔎 Onduleurs – page {page}")
    html = requests.get(url, headers=HEADERS, timeout=10).text
    soup = BeautifulSoup(html, "html.parser")
    items = soup.select("article.product-miniature")
    if not items:
        print("✅ Fin pagination"); break

    for it in items:
        name = it.select_one("h2.h3.product-title")
        ref  = it.select_one("span.product-reference")
        pr   = it.select_one(".product-price-and-shipping .price")
        link = it.select_one("a.product-thumbnail") or it.select_one("a.product-title")
        records.append({
            "nom"               : name.text.strip()  if name else None,
            "reference_tun"     : ref.text.strip()   if ref  else None,
            "prix_tun_txt"      : pr.text.strip()    if pr   else None,
            "url_tunisianet"    : link["href"]       if link else None,
            "ref_clean"         : clean_ref(ref.text) if ref else None
        })
    time.sleep(SLEEP_SEC)

tn_df = pd.DataFrame(records)
tn_df["prix_float"] = tn_df["prix_tun_txt"].apply(price_float)

# ---------- 3. Fusion ----------
PRICE_TOLERANCE = 0.03
def price_close(p1, p2): 
    return p1 and p2 and abs(p1-p2)/max(p1,p2) <= PRICE_TOLERANCE

added, updated = 0, 0
for _, p in tn_df.iterrows():
    # 3a. tenter de trouver un match côté Mytek
    sub_mytek = df[df["sous_categorie_id"] == SOUS_CATEGORIE_ID]
    found_idx = None
    for idx, m in sub_mytek.iterrows():
        if m["ref_clean"] and p["ref_clean"] and (
            m["ref_clean"] in p["ref_clean"] or p["ref_clean"] in m["ref_clean"] 
            or fuzz.partial_ratio(m["ref_clean"], p["ref_clean"]) >= 85
        ):
            if price_close(price_float(m["mytek_apres_remise"]), p["prix_float"]):
                found_idx = idx; break

    if found_idx is not None:
        df.loc[found_idx, ["reference_tunisianet","tunisianet_avant_remise",
                           "tunisianet_apres_remise","url_tunisianet"]] = \
            [p.reference_tun, p.prix_tun_txt, p.prix_tun_txt, p.url_tunisianet]
        updated += 1
    else:
        # 3b. nouveau produit uniquement Tunisianet
        # Sous-sous-catégorie : on cherche un mot-clé du nom dans le mapping
        ssc_id = None
        for key in nom_to_sscid:
            if key.lower() in (p.nom or '').lower():
                ssc_id = nom_to_sscid[key]; break
        if not ssc_id:          # défaut : Accessoires
            ssc_id = nom_to_sscid.get("Accessoires Onduleur")

        df = pd.concat([df, pd.DataFrame([{
            "id": next_id,
            "reference_mytek": None,
            "nom": p.nom,
            "sous_categorie_id": SOUS_CATEGORIE_ID,
            "sous_sous_categorie_id": ssc_id,
            "mytek_avant_remise": None,
            "mytek_apres_remise": None,
            "url_mytek": None,
            "reference_tunisianet": p.reference_tun,
            "tunisianet_avant_remise": p.prix_tun_txt,
            "tunisianet_apres_remise": p.prix_tun_txt,
            "url_tunisianet": p.url_tunisianet,
            "ref_clean": p.ref_clean
        }])], ignore_index=True)
        next_id += 1
        added += 1

# ---------- 4. Sauvegarde Sheet3 ----------
df.drop(columns="ref_clean", inplace=True, errors="ignore")
with pd.ExcelWriter(EXCEL_PATH, mode="a", engine="openpyxl",
                    if_sheet_exists="replace") as w:
    df.to_excel(w, index=False, sheet_name=SHEET_NAME)

print(f"✅ Fusion Onduleurs : {updated} lignes mises à jour, {added} créées.")
