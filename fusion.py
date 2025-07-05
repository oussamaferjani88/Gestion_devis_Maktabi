"""
fusion_mytek_tunisianet.py   (version loggée)
---------------------------------------------
• Scrape Tunisianet (toutes les catégories listées) en affichant
  catégorie + N° page + URL à chaque requête
• Match sur les références, enrichit / ajoute dans Sheet3 de mytek.xlsx
"""

import requests, re, time
from bs4 import BeautifulSoup
import pandas as pd
from rapidfuzz import fuzz
import unidecode

# ========= CONFIG =========
EXCEL_PATH      = r"C:\Users\NESSIM\Desktop\scrapping web\mytek.xlsx"
SHEET_NAME      = "Sheet3"
HEADERS         = {"User-Agent": "Mozilla/5.0"}
PRICE_TOLERANCE = 0.03
MAX_PAGES       = 50          # sécurité
SLEEP_SEC       = 1

TN_CATEGORIES = [
    ("Ordinateur de bureau" , "https://www.tunisianet.com.tn/701-ordinateur-de-bureau"),
    ("Ordinateurs portables", "https://www.tunisianet.com.tn/702-ordinateur-portable"),
    ("Imprimantes"          , "https://www.tunisianet.com.tn/316-imprimante-en-tunisie"),
    ("Scanners"             , "https://www.tunisianet.com.tn/326-scanner-informatique"),
    ("Vidéoprojecteurs"     , "https://www.tunisianet.com.tn/368-videoprojecteurs"),
    ("Gaming PC"            , "https://www.tunisianet.com.tn/681-pc-portable-gamer"),
    ("Photocopieurs"        , "https://www.tunisianet.com.tn/444-photocopieur-tunisie"),
    ("Serveurs"             , "https://www.tunisianet.com.tn/375-serveur-informatique-tunisie"),
    ("Téléphone Fixe"       , "https://www.tunisianet.com.tn/462-telephone-fixe"),
]
# ===========================

def clean_ref(r): 
    return re.sub(r'[^a-z0-9]', '', unidecode.unidecode(str(r).lower()))

def price_float(txt):
    if not txt: return None
    try: return float(re.sub(r'[^\d,.]', '', txt).replace(',', '.'))
    except: return None

def ref_match(a, b):
    if not a or not b: return False
    return a in b or b in a or fuzz.partial_ratio(a, b) >= 85

def price_close(p1, p2, tol=PRICE_TOLERANCE):
    return p1 and p2 and abs(p1-p2)/max(p1,p2) <= tol

# -------- Scraper avec LOG --------
def scrape_category(base_url, cat_name):
    recs = []
    for page in range(1, MAX_PAGES+1):
        url = f"{base_url}?page={page}"
        print(f"🔎 {cat_name} – page {page} → {url}")
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
        except Exception as e:
            print(f"   ❌ Erreur connexion ({e}) → stop catégorie\n")
            break
        if resp.status_code != 200:
            print(f"   ⚠️ HTTP {resp.status_code} → stop catégorie\n")
            break

        soup = BeautifulSoup(resp.text, 'html.parser')
        items = soup.select("article.product-miniature")
        if not items:
            print(f"   ✅ Fin des pages pour {cat_name}\n")
            break

        for it in items:
            name = it.select_one("h2.h3.product-title")
            ref  = it.select_one("span.product-reference")
            pr   = it.select_one(".product-price-and-shipping .price")
            link = it.select_one("a.product-thumbnail") or it.select_one("a.product-title")
            recs.append({
                "nom"           : name.text.strip()  if name else None,
                "reference_tun" : ref.text.strip()   if ref  else None,
                "prix_tun_txt"  : pr.text.strip()    if pr   else None,
                "url_tunisianet": link["href"]       if link else None
            })
        time.sleep(SLEEP_SEC)
    return pd.DataFrame(recs)

# -------- PREP Mytek --------
sheet1 = pd.read_excel(EXCEL_PATH, sheet_name="Sheet1")
sheet2 = pd.read_excel(EXCEL_PATH, sheet_name="Sheet2")
subcat_to_id = {r.nom.strip(): int(r.id) for _, r in sheet2.iterrows()}

df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)
rename_map = {
    "reference_mytek":["reference mytek","reference"],
    "mytek_avant_remise":["mytek avant remise","prix avant remise","prix_avant_remise"],
    "mytek_apres_remise":["mytek apres remise","prix apres remise","prix_apres_remise"],
    "url_mytek":["url mytek","url"],
}
for std, aliases in rename_map.items():
    for alt in aliases:
        if alt in df.columns and std not in df.columns:
            df = df.rename(columns={alt:std})
if "reference_tunisianet" not in df.columns:
    for col in ["reference_tunisianet","tunisianet_avant_remise",
                "tunisianet_apres_remise","url_tunisianet"]:
        df[col] = None

df["ref_clean"] = df["reference_mytek"].apply(clean_ref)
next_id = int(df["id"].max() or 0) + 1

# -------- LOOP sur catégories --------
for cat_name, base_url in TN_CATEGORIES:
    sid = subcat_to_id.get(cat_name)
    if not sid:
        print(f"⚠️  Sous-catégorie absente de Sheet2 : {cat_name}\n"); continue

    tn_df = scrape_category(base_url, cat_name)
    if tn_df.empty: continue
    tn_df["ref_clean"]  = tn_df["reference_tun"].apply(clean_ref)
    tn_df["prix_float"] = tn_df["prix_tun_txt"].apply(price_float)

    sub_mytek = df[df["sous_categorie_id"] == sid]

    for _, p in tn_df.iterrows():
        found = None
        for idx, m in sub_mytek.iterrows():
            if ref_match(p.ref_clean, m.ref_clean):
                if price_close(p.prix_float, price_float(m.mytek_apres_remise)):
                    found = idx; break
        if found is not None:
            df.loc[found, ["reference_tunisianet","tunisianet_avant_remise",
                           "tunisianet_apres_remise","url_tunisianet"]] = \
                [p.reference_tun, p.prix_tun_txt, p.prix_tun_txt, p.url_tunisianet]
        else:
            df = pd.concat([df, pd.DataFrame([{
                "id":next_id,"reference_mytek":None,"reference_tunisianet":p.reference_tun,
                "nom":p.nom,"sous_categorie_id":sid,
                "mytek_avant_remise":None,"mytek_apres_remise":None,
                "tunisianet_avant_remise":p.prix_tun_txt,"tunisianet_apres_remise":p.prix_tun_txt,
                "url_mytek":None,"url_tunisianet":p.url_tunisianet,
                "ref_clean":p.ref_clean
            }])], ignore_index=True)
            next_id += 1

# -------- SAVE --------
df.drop(columns="ref_clean", inplace=True, errors="ignore")
with pd.ExcelWriter(EXCEL_PATH, mode="a", engine="openpyxl", if_sheet_exists="replace") as w:
    df.to_excel(w, index=False, sheet_name=SHEET_NAME)

print("🎉 Fusion terminée : Sheet3 mis à jour.")
